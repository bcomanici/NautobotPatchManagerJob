"""
Nautobot Job: Import racks, devices, cables, and connection data from Patch Manager.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from nautobot.apps.jobs import BooleanVar, ChoiceVar, IntegerVar, Job, StringVar
from nautobot.dcim.models import Cable, Device, DeviceType, Interface, Location, LocationType, Manufacturer, Rack
from nautobot.extras.models import Role, SecretsGroup, SecretsGroupAssociation, Status


DEFAULT_FORMATS = {
    "rack_format": "Cabinet Export",
    "device_format": "Equipment Export",
    "cable_format": "Cable Export",
}

DEFAULT_FIELDS = {
    "rack_name": "Cabinet Name",
    "rack_identifier": "Cabinet Identifier",
    "rack_location": "Cabinet Location",
    "rack_template": "Cabinet Template",
    "rack_description": "Cabinet Description",
    "device_name": "Equipment Label",
    "device_identifier": "Equipment Identifier",
    "device_type": "Equipment Template",
    "device_position": "Equipment Position",
    "device_description": "Equipment Description",
    "cable_label": "Cable Label",
    "cable_description": "Cable Description",
    "connection_left": "Connections Left",
    "connection_right": "Connections Right",
    "port_labeling_front": "Port Labeling Front",
    "port_labeling_rear": "Port Labeling Rear",
    "port_attributes_front": "Port Attributes Front",
    "port_attributes_rear": "Port Attributes Rear",
}

SECRETS_GROUP_NAME = "PatchManagerAPI"
USERNAME_SECRET_NAME = "PatchManagerUser"
PASSWORD_SECRET_NAME = "PatchManagerPassword"

PORT_TYPE_DEFAULT = "1000base-t"
DEFAULT_MANUFACTURER_NAME = "Patch Manager"
DEFAULT_LOCATION_TYPE_NAME = "Site"
DEFAULT_STATUS_NAME = "Active"
DEFAULT_DEVICE_ROLE_NAME = "Patch Manager Imported"
DEFAULT_PASSIVE_ROLE_NAME = "Patch Manager Passive Infrastructure"
DEFAULT_PASSIVE_MANUFACTURER_NAME = "Generic"
DEFAULT_RACK_HEIGHT = 45

RACK_LOOKUP_KEYWORDS = (
    "rack",
    "cabinet",
    "colo",
    "fdp",
    "cage",
    "panel",
    "panels",
)

IGNORED_RACK_LOOKUP_TOKENS = {
    "24th floor",
    "401 north broad street",
    "32 aoa",
    "32 aoa colo",
    "32 aoa col",
    "syracuse",
    "syracuse pop",
    "patch manager",
}

PORT_DETAIL_FIELDS = (
    "Port Labeling Front",
    "Port Labeling Rear",
    "Port Attributes Front",
    "Port Attributes Rear",
)

PM_PORT_DETAILS_START = "## Patch Manager Port Details"
PM_PORT_DETAILS_END = "## End Patch Manager Port Details"

CONNECTION_SPLIT_RE = re.compile(r"\s*\|\s*|\s*;\s*")
PM_TEMPLATE_BRACKET_RE = re.compile(r"\[[^\]]+\]")


@dataclass(frozen=True)
class PMEndpoint:
    raw: str
    device_name: str
    port_name: str


class PatchManagerClient:
    def __init__(self, base_url: str, username: str, password: str, verify_ssl: bool = True, timeout: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = verify_ssl
        self.timeout = timeout

    def get_collection(self, resource: str, fmt: str, limit: int = 500) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        start = 0

        while True:
            params = {"start": start, "limit": limit}
            if fmt:
                params["format"] = fmt

            url = f"{self.base_url}/rest/{resource}?{urlencode(params)}"
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code >= 400:
                raise requests.HTTPError(
                    f"{response.status_code} Client Error for url: {url}; response body: {response.text[:1000]}",
                    response=response,
                )

            batch = self._normalize_collection(response.json(), resource)
            rows.extend(batch)

            if len(batch) < limit:
                break

            start += limit

        return rows

    @staticmethod
    def _normalize_collection(payload: Any, resource: str) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]

        if isinstance(payload, dict):
            for key in (resource, resource.replace("-", " "), resource.title(), "items", "results", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]

            if all(isinstance(k, str) for k in payload.keys()):
                return [payload]

        return []


class PatchManagerImport(Job):
    """Import Patch Manager cabinets, equipment, and cables into Nautobot."""

    class Meta:
        name = "Import Patch Manager Inventory"
        description = "Imports Patch Manager cabinets, equipment, cables, and cable endpoint connections into Nautobot."

    patch_manager_url = StringVar(
        description="Patch Manager base URL",
        default="https://nysernet.patchmanager.com",
    )
    verify_ssl = BooleanVar(default=True)
    dryrun = BooleanVar(default=True)
    page_size = IntegerVar(default=500, min_value=1, max_value=5000)

    rack_format = StringVar(default=DEFAULT_FORMATS["rack_format"])
    device_format = StringVar(default=DEFAULT_FORMATS["device_format"])
    cable_format = StringVar(default=DEFAULT_FORMATS["cable_format"])

    import_mode = ChoiceVar(
        choices=(
            ("all", "All: racks, equipment, cables"),
            ("inventory", "Racks and equipment"),
            ("racks", "Racks only"),
            ("equipment", "Equipment only"),
            ("cables", "Cables only"),
        ),
        default="all",
    )

    def run(self, *args: Any, **kwargs: Any) -> None:
        username = self.get_secret_from_group(SECRETS_GROUP_NAME, USERNAME_SECRET_NAME)
        password = self.get_secret_from_group(SECRETS_GROUP_NAME, PASSWORD_SECRET_NAME)

        client = PatchManagerClient(
            base_url=kwargs["patch_manager_url"],
            username=username,
            password=password,
            verify_ssl=kwargs["verify_ssl"],
        )

        self.dryrun = kwargs["dryrun"]
        self.page_size = kwargs["page_size"]
        self.fields = DEFAULT_FIELDS.copy()
        self.no_valid_u_rows: List[Dict[str, str]] = []
        self.no_valid_u_outcomes: Dict[int, Dict[str, str]] = {}
        self.rack_lookup_cache: Dict[str, List[Rack]] = {}
        self.rack_lookup_cache_loaded = False

        with transaction.atomic():
            if kwargs["import_mode"] in ("all", "inventory", "racks"):
                self.import_racks(client.get_collection("cabinets", kwargs["rack_format"], self.page_size))

            if kwargs["import_mode"] in ("all", "inventory", "equipment"):
                equipment_rows = client.get_collection("equipment", kwargs["device_format"], self.page_size)
                skipped_port_detail_rows = self.import_devices(equipment_rows)
                self.apply_skipped_device_port_details(skipped_port_detail_rows)

            if kwargs["import_mode"] in ("all", "cables"):
                self.import_cables(client.get_collection("cables", kwargs["cable_format"], self.page_size))

            self.log_no_valid_u_summary()

            if self.dryrun:
                self.logger.warning("Dry run enabled; rolling back all database changes.")
                transaction.set_rollback(True)

    def record_no_valid_u_row(self, name: str, row: Dict[str, Any]) -> None:
        self.no_valid_u_rows.append(
            {
                "row_id": str(id(row)),
                "device": name,
                "equipment_identifier": self.clean(row.get(self.fields["device_identifier"])),
                "equipment_position": self.clean(row.get(self.fields["device_position"])),
                "equipment_template": self.clean(row.get(self.fields["device_type"])),
            }
        )

    def mark_no_valid_u_outcome(
        self,
        row: Dict[str, Any],
        outcome: str,
        target_device: Optional[Device] = None,
    ) -> None:
        self.no_valid_u_outcomes[id(row)] = {
            "outcome": outcome,
            "target_device": target_device.name if target_device else "",
        }

    def log_no_valid_u_summary(self) -> None:
        if not self.no_valid_u_rows:
            self.logger.info("No devices were skipped for missing or invalid U position.")
            return

        grouped: Dict[str, List[Dict[str, str]]] = {
            "attached_with_port_details": [],
            "matched_parent_but_empty_details": [],
            "no_matching_parent": [],
            "not_processed": [],
        }

        for item in self.no_valid_u_rows:
            row_id = int(item["row_id"])
            outcome_data = self.no_valid_u_outcomes.get(row_id, {})
            outcome = outcome_data.get("outcome", "not_processed")
            target_device = outcome_data.get("target_device", "")

            item_with_target = dict(item)
            item_with_target["target_device"] = target_device

            if outcome in grouped:
                grouped[outcome].append(item_with_target)
            else:
                grouped["not_processed"].append(item_with_target)

        self.logger.warning(
            "No-valid-U summary: total=%s attached_with_port_details=%s "
            "matched_parent_but_empty_details=%s no_matching_parent=%s not_processed=%s",
            len(self.no_valid_u_rows),
            len(grouped["attached_with_port_details"]),
            len(grouped["matched_parent_but_empty_details"]),
            len(grouped["no_matching_parent"]),
            len(grouped["not_processed"]),
        )

        summary_labels = {
            "attached_with_port_details": "No valid U, attached to parent and appended port details",
            "matched_parent_but_empty_details": "No valid U, matched parent but no port detail fields populated",
            "no_matching_parent": "No valid U, no matching parent device found",
            "not_processed": "No valid U, not processed by port-detail pass",
        }

        for outcome, rows in grouped.items():
            if not rows:
                continue

            self.logger.warning("%s: %s", summary_labels[outcome], len(rows))

            for item in rows:
                self.logger.warning(
                    "%s: device=%s target=%s position=%r template=%r identifier=%r",
                    summary_labels[outcome],
                    item["device"],
                    item.get("target_device", ""),
                    item["equipment_position"],
                    item["equipment_template"],
                    item["equipment_identifier"],
                )

    def build_rack_name(self, name: str, identifier: str) -> str:
        """
        Build a stable, location-qualified Nautobot rack name from the full
        Patch Manager Cabinet Identifier path.

        Examples:
        - NYSERNet Backbone,Ashburn VA - PoP,...,Cabinet 0316
          -> Ashburn VA - PoP Cabinet 0316
        - NYSERNet Backbone,Binghamton - PoP,...,Cabinet 001
          -> Binghamton - PoP Cabinet 001
        - NYSERNet Backbone,New York City - PoP,...,177828-COLO-CCF
          -> New York City - PoP 177828-COLO-CCF
        - Syracuse POP,Syracuse,...,Rack 101.02
          -> Syracuse POP Rack 101.2
        """
        clean_name = self.clean(name).replace("<COMMA>", ",")
        clean_identifier = self.clean(identifier)

        if not clean_identifier:
            return clean_name

        parts = self.split_identifier_preserving_escaped_commas(clean_identifier)
        if not parts:
            return clean_name

        should_qualify = self.is_generic_patch_manager_rack_name(clean_name) or self.is_location_qualified_rack_id(clean_name)

        if not should_qualify:
            return clean_name

        site_name = self.get_rack_name_prefix_from_identifier(parts)
        hierarchy_parts = parts[3:] if len(parts) > 3 else parts[1:]

        precise_part = self.find_precise_rack_identifier_part(hierarchy_parts)
        if precise_part:
            normalized_part = self.normalize_imported_rack_name_part(precise_part)

            if self.is_location_qualified_rack_id(normalized_part):
                return f"{site_name} {normalized_part}".strip()

            if re.match(r"^\d+\.\d+\b", normalized_part):
                return normalized_part

            if normalized_part.lower().startswith("rack "):
                return f"{site_name} {normalized_part}"

            if site_name.lower() not in normalized_part.lower():
                return f"{site_name} {normalized_part}"

            return normalized_part

        hierarchy_name = self.build_location_qualified_rack_name_from_identifier(
            site_name=site_name,
            hierarchy_parts=hierarchy_parts,
            rack_name=clean_name,
        )

        return hierarchy_name or clean_name

    @staticmethod
    def get_rack_name_prefix_from_identifier(parts: List[str]) -> str:
        """
        Choose the best prefix for generic/cabinet Patch Manager rack names.

        Keep POP-style names like "Syracuse POP", but when parts[0] is a broad
        bucket like "NYSERNet Backbone", use parts[1] as the physical site.
        """
        if not parts:
            return ""

        broad_buckets = {
            "nysernet backbone",
            "customer locations",
            "customer location",
            "colocation",
            "colo",
        }

        first = re.sub(r"\s+", " ", parts[0].strip()).lower()

        if first in broad_buckets and len(parts) > 1 and parts[1].strip():
            return parts[1].strip()

        return parts[0].strip()

    @staticmethod
    def split_identifier_preserving_escaped_commas(identifier: str) -> List[str]:
        placeholder = "__PM_ESCAPED_COMMA__"
        protected = (identifier or "").replace("<COMMA>", placeholder)
        parts = [part.strip().replace(placeholder, ",") for part in protected.split(",") if part.strip()]
        return parts

    @staticmethod
    def is_generic_patch_manager_rack_name(name: str) -> bool:
        normalized = re.sub(r"\s+", " ", name or "").strip().lower()
        return not normalized or normalized == "rack" or normalized.startswith("rack ")

    @staticmethod
    def is_location_qualified_rack_id(value: str) -> bool:
        normalized = re.sub(r"\s+", " ", value or "").strip().lower()

        if re.match(r"^cabinet\s+[a-z0-9-]+$", normalized):
            return True

        if re.match(r"^\d{5,}-colo-[a-z0-9-]+$", normalized):
            return True

        return False

    def find_precise_rack_identifier_part(self, parts: List[str]) -> str:
        ignored = set(IGNORED_RACK_LOOKUP_TOKENS) | {
            "nysernet cage",
            "nysenet cage",
        }

        # Explicit rack-number references such as Rack 101.02 Panel 2.
        for part in reversed(parts):
            normalized = self.normalize_pm_match_text(part)
            if normalized in ignored:
                continue

            if re.search(r"\brack\s+\d+\.\d+\b", normalized):
                return part

        # Cabinet 0316 / Cabinet 001 are authoritative rack identifiers.
        for part in reversed(parts):
            normalized = self.normalize_pm_match_text(part)
            if normalized in ignored:
                continue

            if re.search(r"\bcabinet\s+[a-z0-9-]+\b", normalized):
                return part

        # 177828-COLO-CCF-style cabinet IDs are authoritative rack identifiers.
        for part in reversed(parts):
            normalized = self.normalize_pm_match_text(part)
            if normalized in ignored:
                continue

            if re.search(r"\b\d{5,}-colo-[a-z0-9-]+\b", normalized):
                return part

        # Leading decimal rack identifiers such as 2405.14 NYPH.
        for part in reversed(parts):
            normalized = self.normalize_pm_match_text(part)
            if normalized in ignored:
                continue

            if re.search(r"^\d+\.\d+\b", normalized):
                return part

        return ""

    def find_meaningful_rack_identifier_part(self, parts: List[str]) -> str:
        precise = self.find_precise_rack_identifier_part(parts)
        if precise:
            return precise

        for part in reversed(parts):
            normalized = self.normalize_pm_match_text(part)
            if normalized and normalized not in IGNORED_RACK_LOOKUP_TOKENS:
                return part

        return ""

    def build_location_qualified_rack_name_from_identifier(
        self,
        site_name: str,
        hierarchy_parts: List[str],
        rack_name: str,
    ) -> str:
        cleaned_parts: List[str] = []

        for part in hierarchy_parts:
            normalized_part = self.normalize_imported_rack_name_part(part)
            normalized_key = self.normalize_pm_match_text(normalized_part)

            if not normalized_part or normalized_key in IGNORED_RACK_LOOKUP_TOKENS:
                continue

            if normalized_key == self.normalize_pm_match_text(site_name):
                continue

            if re.search(r"\d+\s+[a-z]+\s+(street|st|avenue|ave|road|rd|broad|court|ct)", normalized_key):
                continue

            if normalized_part not in cleaned_parts:
                cleaned_parts.append(normalized_part)

        pieces = [site_name] + cleaned_parts

        if rack_name and rack_name not in pieces:
            pieces.append(rack_name)

        result = " ".join(piece for piece in pieces if piece)
        result = re.sub(r"\s+", " ", result).strip()
        return result

    @staticmethod
    def normalize_imported_rack_name_part(value: str) -> str:
        normalized = (value or "").replace("<COMMA>", ",").strip()

        normalized = re.sub(
            r"\bpanel\s+\d+\b",
            "",
            normalized,
            flags=re.IGNORECASE,
        ).strip()

        normalized = re.sub(r"\s+", " ", normalized)

        normalized = re.sub(
            r"(\d+)\.0+(\d+)",
            lambda match: f"{match.group(1)}.{int(match.group(2))}",
            normalized,
        )

        return normalized

    def import_racks(self, rows: Iterable[Dict[str, Any]]) -> None:
        status = self.get_status()

        for row in rows:
            name = self.clean(row.get(self.fields["rack_name"]))
            identifier = self.clean(row.get(self.fields["rack_identifier"]))
            rack_template = self.clean(row.get(self.fields["rack_template"]))

            if not name:
                self.logger.warning("Skipping rack without name: %s", row)
                continue

            name = self.build_rack_name(name, identifier)

            u_height = self.parse_rack_height(rack_template)
            location_name = self.clean(row.get(self.fields["rack_location"])) or "Patch Manager"
            location = self.get_or_create_location(location_name)

            rack, created = Rack.objects.update_or_create(
                name=name,
                location=location,
                defaults={
                    "status": status,
                    "u_height": u_height,
                    "comments": self.clean(row.get(self.fields["rack_description"])),
                },
            )

            self.add_rack_to_lookup_cache(rack)

            self.logger.info(
                "%s rack %s (%sU) from Cabinet Identifier=%r",
                "Created" if created else "Updated",
                rack.name,
                rack.u_height,
                identifier,
            )

    def import_devices(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        status = self.get_status()
        skipped_port_detail_rows: List[Dict[str, Any]] = []

        for row in rows:
            name = self.clean(row.get(self.fields["device_name"]))
            if not name:
                self.logger.warning("Skipping device without Equipment Label: %s", row)
                continue

            identifier_data = self.parse_equipment_identifier(self.clean(row.get(self.fields["device_identifier"])))

            device_type = self.get_or_create_device_type(self.clean(row.get(self.fields["device_type"])))
            role = self.get_or_create_device_role(identifier_data["role"])
            location = self.get_or_create_location(
                identifier_data["location"] or "Patch Manager",
                identifier_data["address"],
            )
            rack = self.find_rack_from_names(identifier_data["identifier_parts"], location)

            if rack:
                location = rack.location
                if location and location.location_type:
                    self.ensure_location_type_content_types(location.location_type)

            position, face = self.parse_equipment_position(self.clean(row.get(self.fields["device_position"])))

            if position is None:
                self.logger.info(
                    "Cataloging skipped device %s; no valid U position found in Equipment Position",
                    name,
                )
                self.record_no_valid_u_row(name, row)
                skipped_port_detail_rows.append(row)
                continue

            if not rack:
                self.logger.warning(
                    "Device %s has a valid U position but no matching rack; importing without rack position. "
                    "Equipment Position=%r Equipment Identifier=%r",
                    name,
                    self.clean(row.get(self.fields["device_position"])),
                    self.clean(row.get(self.fields["device_identifier"])),
                )
                position = None
                face = ""

            existing_device = Device.objects.filter(name=name).first()

            self.handle_front_rear_shared_position(rack, position, face, device_type)

            if rack:
                conflict_qs = Device.objects.filter(rack=rack, position=position, face=face)

                if existing_device:
                    conflict_qs = conflict_qs.exclude(pk=existing_device.pk)

                conflicting_device = conflict_qs.first()

                if conflicting_device:
                    self.logger.warning(
                        "Rack position conflict for %s: rack=%s position=%s face=%s already occupied by %s. "
                        "Importing device without rack position.",
                        name,
                        rack.name,
                        position,
                        face,
                        conflicting_device.name,
                    )
                    position = None

            device_defaults = {
                "device_type": device_type,
                "role": role,
                "status": status,
                "location": location,
                "rack": rack,
                "position": position,
                "face": face,
                "comments": self.clean(row.get(self.fields["device_description"])) or identifier_data["address"],
            }

            try:
                device, created = Device.objects.update_or_create(
                    name=name,
                    defaults=device_defaults,
                )
            except ValidationError as exc:
                self.log_device_import_validation_issue(
                    name=name,
                    row=row,
                    rack=rack,
                    position=position,
                    face=face,
                    device_type=device_type,
                    exc=exc,
                )

                if "position" not in getattr(exc, "message_dict", {}):
                    raise

                self.logger.warning(
                    "Rack position validation failed for %s at rack=%s position=%s face=%s: %s. "
                    "Retrying import without rack position.",
                    name,
                    rack.name if rack else None,
                    position,
                    face,
                    exc,
                )

                device_defaults["position"] = None
                device_defaults["face"] = ""

                try:
                    device, created = Device.objects.update_or_create(
                        name=name,
                        defaults=device_defaults,
                    )
                except ValidationError as retry_exc:
                    self.log_device_import_validation_issue(
                        name=name,
                        row=row,
                        rack=rack,
                        position=device_defaults.get("position"),
                        face=device_defaults.get("face"),
                        device_type=device_type,
                        exc=retry_exc,
                        retry=True,
                    )
                    raise

            self.logger.info("%s device %s", "Created" if created else "Updated", device.name)

        return skipped_port_detail_rows

    def log_device_import_validation_issue(
        self,
        name: str,
        row: Dict[str, Any],
        rack: Optional[Rack],
        position: Optional[int],
        face: str,
        device_type: DeviceType,
        exc: ValidationError,
        retry: bool = False,
    ) -> None:
        identifier = self.clean(row.get(self.fields["device_identifier"]))
        equipment_position = self.clean(row.get(self.fields["device_position"]))
        phase = "retry" if retry else "initial save"

        self.logger.error(
            "Device import validation issue during %s: device=%s rack=%s position=%s face=%r "
            "device_type=%s equipment_position=%r equipment_identifier=%r error=%s",
            phase,
            name,
            rack.name if rack else None,
            position,
            face,
            device_type.model if device_type else None,
            equipment_position,
            identifier,
            exc,
        )

    def apply_skipped_device_port_details(self, rows: Iterable[Dict[str, Any]]) -> None:
        details_by_device_id: Dict[int, List[Dict[str, str]]] = {}
        details_by_interface_id: Dict[int, List[Dict[str, str]]] = {}

        for row in rows:
            identifier = self.clean(row.get(self.fields["device_identifier"]))
            target_interface = self.get_or_create_interface_for_child_row(row)

            if target_interface:
                target_device = target_interface.device
            else:
                target_device = self.find_imported_device_in_identifier(identifier)

            if not target_device:
                target_device = self.get_or_create_passive_infrastructure_device_for_row(row)

            if not target_device:
                self.mark_no_valid_u_outcome(row, "no_matching_parent")
                self.logger.info(
                    "Skipping port detail row; could not find imported parent device from Equipment Identifier: %s",
                    row.get(self.fields["device_identifier"]),
                )
                continue

            details = self.extract_port_detail_fields(row)
            if not details:
                self.mark_no_valid_u_outcome(row, "matched_parent_but_empty_details", target_device)
                self.logger.info(
                    "Skipping port detail row for %s; no port detail fields populated",
                    target_device.name,
                )
                continue

            self.mark_no_valid_u_outcome(row, "attached_with_port_details", target_device)

            if target_interface:
                details_by_interface_id.setdefault(target_interface.pk, []).append(details)
            else:
                details_by_device_id.setdefault(target_device.pk, []).append(details)

        for interface_id, detail_rows in details_by_interface_id.items():
            interface = Interface.objects.select_related("device").get(pk=interface_id)
            updated_description = self.replace_pm_port_details_block(
                getattr(interface, "description", "") or "",
                detail_rows,
            )

            # Interface details are stored on the interface itself. Direct
            # update avoids revalidating unrelated device rack placement.
            Interface.objects.filter(pk=interface.pk).update(description=updated_description)

            self.logger.info(
                "Updated Patch Manager port details on interface %s:%s",
                interface.device.name,
                interface.name,
            )

        for device_id, detail_rows in details_by_device_id.items():
            device = Device.objects.get(pk=device_id)
            updated_comments = self.replace_pm_port_details_block(device.comments or "", detail_rows)

            # This pass only updates comments. Use a direct column update so an
            # existing device with a rack-placement issue does not fail the
            # whole sync when Nautobot revalidates rack occupancy.
            Device.objects.filter(pk=device.pk).update(comments=updated_comments)

            self.logger.info("Updated Patch Manager port details on device %s", device.name)

    def get_or_create_interface_for_child_row(
        self,
        row: Dict[str, Any],
    ) -> Optional[Interface]:
        identifier = self.clean(row.get(self.fields["device_identifier"]))
        if not identifier:
            return None

        identifier_parts = self.split_equipment_identifier_for_matching(identifier)
        matched_racks = self.find_racks_in_identifier(identifier_parts)
        parsed = self.parse_child_interface_identifier(identifier_parts)

        if not parsed:
            return None

        parent_device = self.find_mounted_device_by_identifier_token(
            token=parsed["parent_device"],
            matched_racks=matched_racks,
        )

        if not parent_device:
            return None

        interface_name = parsed["interface_name"]
        status = self.get_status()

        interface, created = Interface.objects.get_or_create(
            device=parent_device,
            name=interface_name,
            defaults={
                "type": PORT_TYPE_DEFAULT,
                "status": status,
                "description": "",
            },
        )

        update_fields = []

        if not interface.status_id:
            interface.status = status
            update_fields.append("status")

        if not getattr(interface, "type", None):
            interface.type = PORT_TYPE_DEFAULT
            update_fields.append("type")

        if update_fields:
            interface.validated_save()

        self.logger.info(
            "%s interface %s on device %s from Patch Manager child/interface row",
            "Created" if created else "Updated",
            interface.name,
            parent_device.name,
        )

        return interface

    def parse_child_interface_identifier(self, identifier_parts: List[str]) -> Optional[Dict[str, str]]:
        """
        Parse Patch Manager child/interface identifiers.

        Common patterns:
            ..., <parent-device>, <slot-or-port-index>, <interface-name>
            ..., <parent-device>, <interface-name>

        Examples:
            ..., pts-5110, 1, xe-0/0/1
            ..., router01, xe-0/0/1
            ..., switch01, QSFP 0
        """
        if len(identifier_parts) < 2:
            return None

        if len(identifier_parts) >= 3:
            interface_token = identifier_parts[-1]
            slot_token = identifier_parts[-2]
            parent_token = identifier_parts[-3]

            if self.looks_like_interface_token(interface_token) and self.looks_like_slot_or_port_index(slot_token):
                return {
                    "parent_device": parent_token,
                    "interface_name": self.normalize_interface_name(interface_token),
                }

        interface_token = identifier_parts[-1]
        parent_token = identifier_parts[-2]

        if self.looks_like_interface_token(interface_token):
            return {
                "parent_device": parent_token,
                "interface_name": self.normalize_interface_name(interface_token),
            }

        return None

    @staticmethod
    def normalize_interface_name(value: str) -> str:
        return re.sub(r"\s+", " ", (value or "").strip())

    @staticmethod
    def looks_like_interface_token(value: str) -> bool:
        token = re.sub(r"\s+", " ", (value or "").strip().lower())

        if not token:
            return False

        interface_patterns = (
            r"^(xe|ge|et|fe|te|gi|fo|hundredgige|tengige|ethernet|eth|mgmt)[-a-z0-9/_.:]+$",
            r"^[a-z]+-\d+/\d+/\d+$",
            r"^\d+/\d+/\d+$",
            r"^\d+/\d+$",
            r"^(qsfp|sfp|cfp|xfp)\s*-?\s*\d+$",
            r"^(qsfp|sfp|cfp|xfp)\d+$",
        )

        if any(re.match(pattern, token) for pattern in interface_patterns):
            return True

        if "/" in token and re.search(r"\d+/\d+", token):
            return True

        if re.match(r"^(qsfp|sfp|cfp|xfp)", token):
            return True

        return False

    @staticmethod
    def looks_like_slot_or_port_index(value: str) -> bool:
        token = re.sub(r"\s+", " ", (value or "").strip().lower())

        if not token:
            return False

        if re.match(r"^\d+$", token):
            return True

        if re.match(r"^(slot|module|lc|mic|mpa|pic)\s*-?\s*\d+$", token):
            return True

        return False

    def get_or_create_passive_infrastructure_device_for_row(
        self,
        row: Dict[str, Any],
    ) -> Optional[Device]:
        identifier = self.clean(row.get(self.fields["device_identifier"]))
        if not identifier:
            return None

        identifier_data = self.parse_equipment_identifier(identifier)
        identifier_parts = identifier_data.get("identifier_parts", [])

        bucket_type = self.detect_passive_infrastructure_bucket(identifier_parts)
        if not bucket_type:
            return None

        location = self.get_or_create_location(
            identifier_data.get("location") or "Patch Manager",
            identifier_data.get("address") or "",
        )

        rack = self.find_rack_from_names(identifier_parts, location)
        if not rack:
            return None

        device_name = f"{rack.name} {bucket_type}"
        device_type = self.get_or_create_passive_device_type(bucket_type)
        role = self.get_or_create_device_role(DEFAULT_PASSIVE_ROLE_NAME)
        status = self.get_status()

        base_defaults = {
            "device_type": device_type,
            "role": role,
            "status": status,
            "location": rack.location,
            "rack": rack,
            "comments": f"Passive infrastructure bucket created by Patch Manager import: {bucket_type}",
        }

        existing_device = Device.objects.filter(name=device_name).first()
        if existing_device and existing_device.rack_id:
            defaults = dict(base_defaults)
            defaults["position"] = existing_device.position
            defaults["face"] = existing_device.face or "front"

            try:
                with transaction.atomic():
                    device, created = Device.objects.update_or_create(
                        name=device_name,
                        defaults=defaults,
                    )

                self.logger.info(
                    "%s passive infrastructure device %s in rack %s",
                    "Created" if created else "Updated",
                    device.name,
                    rack.name,
                )
                return device
            except (ValidationError, IntegrityError) as exc:
                self.logger.warning(
                    "Existing passive infrastructure placement is invalid for %s in rack=%s "
                    "position=%s face=%s: %s. Searching for a new valid U.",
                    device_name,
                    rack.name,
                    existing_device.position,
                    existing_device.face,
                    exc,
                )

        return self.create_or_update_passive_device_in_first_valid_u(
            device_name=device_name,
            rack=rack,
            base_defaults=base_defaults,
        )

    def create_or_update_passive_device_in_first_valid_u(
        self,
        device_name: str,
        rack: Rack,
        base_defaults: Dict[str, Any],
    ) -> Optional[Device]:
        """
        Try U positions from top to bottom. If no rack U is valid, create/update
        the passive bucket device in the rack's location without rack placement.
        """
        failed_positions: List[str] = []
        existing_device = Device.objects.filter(name=device_name).first()

        for position in range(rack.u_height, 0, -1):
            defaults = dict(base_defaults)
            defaults["position"] = position
            defaults["face"] = "front"

            conflict_qs = Device.objects.filter(
                rack=rack,
                position=position,
                face="front",
            )

            if existing_device:
                conflict_qs = conflict_qs.exclude(pk=existing_device.pk)

            if conflict_qs.exists():
                failed_positions.append(f"{position}:occupied")
                continue

            try:
                with transaction.atomic():
                    device, created = Device.objects.update_or_create(
                        name=device_name,
                        defaults=defaults,
                    )

                self.logger.info(
                    "%s passive infrastructure device %s in rack %s at U%s",
                    "Created" if created else "Updated",
                    device.name,
                    rack.name,
                    position,
                )
                return device

            except ValidationError as exc:
                message_dict = getattr(exc, "message_dict", {})
                if "position" in message_dict or "face" in message_dict:
                    failed_positions.append(f"{position}:validation")
                    continue

                self.logger.warning(
                    "Passive infrastructure device %s failed non-placement validation at rack=%s U%s: %s",
                    device_name,
                    rack.name,
                    position,
                    exc,
                )
                raise

            except IntegrityError:
                failed_positions.append(f"{position}:integrity")
                continue

        self.logger.warning(
            "Passive infrastructure device %s could not be placed in rack %s. Tried U positions: %s. "
            "Creating/updating it without rack position.",
            device_name,
            rack.name,
            ", ".join(failed_positions),
        )

        fallback_defaults = dict(base_defaults)
        fallback_defaults["rack"] = None
        fallback_defaults["position"] = None
        fallback_defaults["face"] = ""

        try:
            with transaction.atomic():
                device, created = Device.objects.update_or_create(
                    name=device_name,
                    defaults=fallback_defaults,
                )

            self.logger.info(
                "%s passive infrastructure device %s without rack position",
                "Created" if created else "Updated",
                device.name,
            )
            return device

        except (ValidationError, IntegrityError) as exc:
            self.logger.warning(
                "Passive infrastructure fallback without rack position also failed for %s: %s",
                device_name,
                exc,
            )
            return None

    def detect_passive_infrastructure_bucket(self, identifier_parts: List[str]) -> str:
        for part in identifier_parts:
            bucket = self.normalize_passive_infrastructure_bucket(part)
            if bucket:
                return bucket

        return ""

    @staticmethod
    def normalize_passive_infrastructure_bucket(value: str) -> str:
        text = re.sub(r"\s+", " ", (value or "").replace("<COMMA>", ",").strip())
        normalized = text.lower()

        if not normalized:
            return ""

        if "crown castle fdp" in normalized:
            return "Crown Castle FDP"

        if "fdp" in normalized:
            return text

        if "non nysernet panel" in normalized:
            return "Non NYSERNet Panels"

        if "fiber management" in normalized:
            return "Fiber Management"

        if "bulkhead connector" in normalized:
            return "Bulkhead Connector"

        if "commscope fpx" in normalized or normalized.startswith("commscope fpx"):
            return "CommScope FPX"

        if "telect" in normalized:
            return "Telect"

        if "patch panel" in normalized:
            return "Patch Panel"

        return ""

    def get_or_create_passive_device_type(self, bucket_type: str) -> DeviceType:
        manufacturer, _ = Manufacturer.objects.get_or_create(name=DEFAULT_PASSIVE_MANUFACTURER_NAME)

        device_type, _ = DeviceType.objects.get_or_create(
            manufacturer=manufacturer,
            model=bucket_type,
        )

        return device_type

    def find_imported_device_in_identifier(self, value: str) -> Optional[Device]:
        """
        Resolve a skipped Patch Manager equipment row back to an already-imported,
        rack-mounted Nautobot device.

        Matching order:
        1. Existing known-good behavior: Equipment Identifier must contain a rack
           reference matching an imported Nautobot rack after normalization, then
           a mounted device in that rack must have a normalized name contained
           somewhere in the identifier.
        2. Conservative fallback: if the rack-scoped match fails, look for an
           exact comma-separated Equipment Identifier token that matches an
           existing mounted Nautobot device name. This helps rows where the
           device name is explicit but the rack text is not normalized the same
           way as the imported rack name.
        """
        if not value:
            return None

        identifier = self.clean(value)
        identifier_parts = self.split_equipment_identifier_for_matching(identifier)
        matched_racks = self.find_racks_in_identifier(identifier_parts)

        child_parent_match = self.find_device_by_child_interface_pattern(
            identifier_parts=identifier_parts,
            matched_racks=matched_racks,
        )
        if child_parent_match:
            self.logger.info(
                "Matched skipped port detail row using child/interface parent inference: %s",
                child_parent_match.name,
            )
            return child_parent_match

        rack_scoped_match = self.find_device_by_rack_scoped_contains(
            matched_racks=matched_racks,
            identifier=identifier,
        )
        if rack_scoped_match:
            return rack_scoped_match

        fallback_match = self.find_device_by_exact_identifier_token(
            identifier_parts=identifier_parts,
            matched_racks=matched_racks,
        )
        if fallback_match:
            self.logger.info(
                "Matched skipped port detail row using exact mounted device token fallback: %s",
                fallback_match.name,
            )
            return fallback_match

        return None

    def split_equipment_identifier_for_matching(self, value: str) -> List[str]:
        normalized = (value or "").replace("<COMMA>", ",")
        return [part.strip() for part in normalized.split(",") if part.strip()]

    def find_device_by_child_interface_pattern(
        self,
        identifier_parts: List[str],
        matched_racks: List[Rack],
    ) -> Optional[Device]:
        """
        Resolve Patch Manager child/interface rows to their parent chassis.

        Common Patch Manager pattern:
            ..., <parent-device>, <module-or-port-index>, <interface-name>

        Example:
            ..., pts-5110, 1, xe-0/0/1
        should resolve to device "pts-5110".

        This is intentionally conservative:
        - final token must look like an interface/port name
        - token before it must be numeric or slot-like
        - token before that must match an existing mounted device name
        """
        if len(identifier_parts) < 3:
            return None

        interface_token = identifier_parts[-1]
        slot_token = identifier_parts[-2]
        parent_token = identifier_parts[-3]

        if not self.looks_like_interface_token(interface_token):
            return None

        if not self.looks_like_slot_or_port_index(slot_token):
            return None

        return self.find_mounted_device_by_identifier_token(
            token=parent_token,
            matched_racks=matched_racks,
        )

    def find_mounted_device_by_identifier_token(
        self,
        token: str,
        matched_racks: List[Rack],
    ) -> Optional[Device]:
        normalized_token = self.normalize_pm_match_text(token)

        if not normalized_token:
            return None

        candidate_devices: List[Device] = []

        for device in Device.objects.filter(position__isnull=False).exclude(name=""):
            normalized_device_name = self.normalize_pm_match_text(device.name)
            if normalized_device_name == normalized_token:
                candidate_devices.append(device)

        if not candidate_devices:
            return None

        matched_rack_ids = {rack.pk for rack in matched_racks}
        if matched_rack_ids:
            rack_scoped_candidates = [
                device for device in candidate_devices if device.rack_id in matched_rack_ids
            ]
            if rack_scoped_candidates:
                candidate_devices = rack_scoped_candidates

        candidate_devices.sort(key=lambda device: len(device.name), reverse=True)
        return candidate_devices[0]

    def find_device_by_rack_scoped_contains(
        self,
        matched_racks: List[Rack],
        identifier: str,
    ) -> Optional[Device]:
        if not matched_racks:
            return None

        normalized_identifier = self.normalize_pm_match_text(identifier)
        matched_devices: List[Device] = []

        for rack in matched_racks:
            devices = Device.objects.filter(
                rack=rack,
                position__isnull=False,
            ).exclude(name="")

            for device in devices:
                normalized_device_name = self.normalize_pm_match_text(device.name)
                if normalized_device_name and normalized_device_name in normalized_identifier:
                    matched_devices.append(device)

        if not matched_devices:
            return None

        matched_devices.sort(key=lambda device: len(device.name), reverse=True)
        return matched_devices[0]

    def find_device_by_exact_identifier_token(
        self,
        identifier_parts: List[str],
        matched_racks: List[Rack],
    ) -> Optional[Device]:
        normalized_parts = {
            self.normalize_pm_match_text(part)
            for part in identifier_parts
            if self.normalize_pm_match_text(part)
        }

        if not normalized_parts:
            return None

        candidate_devices: List[Device] = []

        for device in Device.objects.filter(position__isnull=False).exclude(name=""):
            normalized_device_name = self.normalize_pm_match_text(device.name)
            if normalized_device_name in normalized_parts:
                candidate_devices.append(device)

        if not candidate_devices:
            hostname_match = self.find_device_by_hostname_normalized_token(
                identifier_parts=identifier_parts,
                matched_racks=matched_racks,
            )
            if hostname_match:
                self.logger.info(
                    "Matched skipped port detail row using hostname-normalized fallback: %s",
                    hostname_match.name,
                )
                return hostname_match

            return None

        matched_rack_ids = {rack.pk for rack in matched_racks}
        if matched_rack_ids:
            rack_scoped_candidates = [
                device for device in candidate_devices if device.rack_id in matched_rack_ids
            ]
            if rack_scoped_candidates:
                candidate_devices = rack_scoped_candidates

        candidate_devices.sort(key=lambda device: len(device.name), reverse=True)
        return candidate_devices[0]

    def find_device_by_hostname_normalized_token(
        self,
        identifier_parts: List[str],
        matched_racks: List[Rack],
    ) -> Optional[Device]:
        """
        Conservative hostname-like fallback for remaining PM/Nautobot hostname
        mismatches, such as pec-3000r7 where surrounding identifier text differs.
        """
        hostname_tokens = [
            self.normalize_hostname_match_token(part)
            for part in identifier_parts
            if self.normalize_hostname_match_token(part)
        ]

        if not hostname_tokens:
            return None

        candidate_devices: List[Device] = []

        for device in Device.objects.filter(position__isnull=False).exclude(name=""):
            device_hostname = self.normalize_hostname_match_token(device.name)
            if not device_hostname:
                continue

            for token in hostname_tokens:
                if token == device_hostname or token in device_hostname or device_hostname in token:
                    candidate_devices.append(device)
                    break

        if not candidate_devices:
            return None

        matched_rack_ids = {rack.pk for rack in matched_racks}
        if matched_rack_ids:
            rack_scoped_candidates = [
                device for device in candidate_devices if device.rack_id in matched_rack_ids
            ]
            if rack_scoped_candidates:
                candidate_devices = rack_scoped_candidates

        candidate_devices.sort(key=lambda device: len(device.name), reverse=True)
        return candidate_devices[0]

    @staticmethod
    def normalize_hostname_match_token(value: str) -> str:
        normalized = (value or "").replace("<COMMA>", ",").strip().lower()

        match = re.search(r"\b[a-z]{2,10}[a-z0-9]*-[a-z0-9][a-z0-9-]*\b", normalized)
        if not match:
            return ""

        token = re.sub(r"[^a-z0-9-]+", "", match.group(0))
        if len(token) < 6:
            return ""

        return token

    def find_racks_in_identifier(self, identifier_parts: List[str]) -> List[Rack]:
        if not identifier_parts:
            return []

        normalized_parts = [self.normalize_pm_match_text(part) for part in identifier_parts]
        normalized_parts = [part for part in normalized_parts if part]

        matched_racks: List[Rack] = []

        for rack in Rack.objects.all():
            normalized_rack_name = self.normalize_pm_match_text(rack.name)
            if not normalized_rack_name:
                continue

            rack_name_variants = {
                normalized_rack_name,
                self.normalize_rack_name_order(normalized_rack_name),
            }

            for normalized_part in normalized_parts:
                part_variants = {
                    normalized_part,
                    self.normalize_rack_name_order(normalized_part),
                }

                if rack_name_variants & part_variants:
                    matched_racks.append(rack)
                    break

        return matched_racks

    @staticmethod
    def normalize_pm_match_text(value: str) -> str:
        normalized = (value or "").replace("<COMMA>", ",")
        normalized = re.sub(r"\s+", " ", normalized)
        normalized = normalized.strip().lower()
        return normalized

    @staticmethod
    def normalize_rack_name_order(value: str) -> str:
        """
        Normalize "Rack 301.09" and "301.09 Rack" to the same comparison form.
        Leaves non-matching rack names unchanged.
        """
        normalized = re.sub(r"\s+", " ", value or "").strip().lower()

        rack_prefix_match = re.match(r"^rack\s+(.+)$", normalized)
        if rack_prefix_match:
            return f"{rack_prefix_match.group(1).strip()} rack"

        rack_suffix_match = re.match(r"^(.+)\s+rack$", normalized)
        if rack_suffix_match:
            return f"{rack_suffix_match.group(1).strip()} rack"

        return normalized

    def extract_port_detail_fields(self, row: Dict[str, Any]) -> Dict[str, str]:
        details: Dict[str, str] = {}

        field_map = {
            self.fields["port_labeling_front"]: "Port Labeling Front",
            self.fields["port_labeling_rear"]: "Port Labeling Rear",
            self.fields["port_attributes_front"]: "Port Attributes Front",
            self.fields["port_attributes_rear"]: "Port Attributes Rear",
        }

        for source_column, header in field_map.items():
            value = self.clean(row.get(source_column))
            if value:
                details[header] = value

        return details

    def replace_pm_port_details_block(self, existing_comments: str, detail_rows: List[Dict[str, str]]) -> str:
        base_comments = self.strip_pm_port_details_block(existing_comments).rstrip()
        new_block = self.render_pm_port_details_block(detail_rows)

        if base_comments:
            return f"{base_comments}\n\n{new_block}"

        return new_block

    def strip_pm_port_details_block(self, comments: str) -> str:
        pattern = re.compile(
            rf"\n*{re.escape(PM_PORT_DETAILS_START)}.*?{re.escape(PM_PORT_DETAILS_END)}\n*",
            re.DOTALL,
        )
        return pattern.sub("\n", comments or "").strip()

    def render_pm_port_details_block(self, detail_rows: List[Dict[str, str]]) -> str:
        lines = [PM_PORT_DETAILS_START]

        for index, details in enumerate(detail_rows, start=1):
            if len(detail_rows) > 1:
                lines.append(f"### Entry {index}")

            for header in PORT_DETAIL_FIELDS:
                value = details.get(header)
                if value:
                    lines.extend([f"### {header}", value])

        lines.append(PM_PORT_DETAILS_END)
        return "\n".join(lines)

    def handle_front_rear_shared_position(
        self,
        rack: Optional[Rack],
        position: Optional[int],
        face: str,
        device_type: DeviceType,
    ) -> None:
        """
        If Patch Manager has two devices at the same rack/U position, one front
        and one rear, make both device types not full-depth so Nautobot can
        model the shared rack unit correctly.
        """
        if not rack or position is None:
            return

        opposite_face = "rear" if face == "front" else "front"
        opposite_device = Device.objects.filter(
            rack=rack,
            position=position,
            face=opposite_face,
        ).first()

        if not opposite_device:
            return

        self.set_device_type_not_full_depth(device_type)

        if opposite_device.device_type:
            self.set_device_type_not_full_depth(opposite_device.device_type)

        self.logger.info(
            "Detected front/rear shared rack position at rack=%s U%s; marked device types as not full-depth.",
            rack.name,
            position,
        )

    def set_device_type_not_full_depth(self, device_type: DeviceType) -> None:
        if not getattr(device_type, "is_full_depth", False):
            return

        device_type.is_full_depth = False
        device_type.validated_save()
        self.logger.info(
            "Set device type %s to not full-depth",
            device_type.model,
        )

    def import_cables(self, rows: Iterable[Dict[str, Any]]) -> None:
        status = self.get_status()

        for row in rows:
            label = self.clean(row.get(self.fields["cable_label"])) or None
            left = self.parse_endpoint(self.clean(row.get(self.fields["connection_left"])))
            right = self.parse_endpoint(self.clean(row.get(self.fields["connection_right"])))

            if not left or not right:
                self.logger.warning("Skipping cable %s; missing endpoint", label or row)
                continue

            left_if = self.get_interface_for_endpoint(left)
            right_if = self.get_interface_for_endpoint(right)

            if not left_if or not right_if:
                self.logger.warning("Skipping cable %s; could not resolve endpoints", label)
                continue

            if left_if.cable_id or right_if.cable_id:
                self.logger.info(
                    "Skipping cable %s; one endpoint is already cabled",
                    label or f"{left.raw} -> {right.raw}",
                )
                continue

            cable = Cable.objects.create(
                termination_a=left_if,
                termination_b=right_if,
                status=status,
                label=label,
                comments=self.clean(row.get(self.fields["cable_description"])),
            )

            self.logger.info(
                "Created cable %s: %s:%s -> %s:%s",
                cable.label or cable.pk,
                left.device_name,
                left.port_name,
                right.device_name,
                right.port_name,
            )

    def parse_equipment_identifier(self, value: str) -> Dict[str, Any]:
        parts = [p.strip() for p in value.split(",") if p.strip()]

        return {
            "role": parts[0] if len(parts) > 0 else DEFAULT_DEVICE_ROLE_NAME,
            "location": parts[1] if len(parts) > 1 else "Patch Manager",
            "address": parts[2] if len(parts) > 2 else "",
            "racks": parts[3:] if len(parts) > 3 else [],
            "identifier_parts": parts,
        }

    @staticmethod
    def parse_equipment_position(value: str) -> Tuple[Optional[int], str]:
        if not value:
            return None, "front"

        u_match = re.search(r"\bU\s*(\d+)\b", value, re.IGNORECASE)
        position = int(u_match.group(1)) if u_match else None

        face_match = re.search(r"\b(front|rear)\b", value, re.IGNORECASE)
        face = face_match.group(1).lower() if face_match else "front"

        return position, face

    @staticmethod
    def parse_rack_height(value: str) -> int:
        if not value:
            return DEFAULT_RACK_HEIGHT

        match = re.search(r"(\d+)\s*U", value, re.IGNORECASE)
        if match:
            return int(match.group(1))

        return DEFAULT_RACK_HEIGHT

    def get_secret_from_group(self, group_name: str, secret_name: str) -> str:
        group = SecretsGroup.objects.get(name=group_name)
        association = SecretsGroupAssociation.objects.select_related("secret").get(
            secrets_group=group,
            secret__name=secret_name,
        )
        value = association.secret.get_value()
        return "" if value is None else str(value).strip()

    def get_interface_for_endpoint(self, endpoint: PMEndpoint) -> Optional[Interface]:
        device = self.find_device(endpoint.device_name)
        if not device:
            return None

        status = self.get_status()

        interface, _ = Interface.objects.get_or_create(
            device=device,
            name=endpoint.port_name,
            defaults={
                "type": PORT_TYPE_DEFAULT,
                "status": status,
            },
        )

        if not interface.status_id:
            interface.status = status
            interface.validated_save()

        return interface

    def parse_endpoint(self, value: str) -> Optional[PMEndpoint]:
        if not value or value == "|":
            return None

        candidates = [x for x in CONNECTION_SPLIT_RE.split(value) if x and x != "|"]
        if not candidates:
            return None

        raw = candidates[0].strip()
        parts = [PM_TEMPLATE_BRACKET_RE.sub("", p).strip() for p in raw.split(",") if p.strip()]

        if len(parts) < 2:
            return None

        return PMEndpoint(raw=raw, device_name=parts[-2], port_name=parts[-1])

    def find_device(self, value: str) -> Optional[Device]:
        if not value:
            return None

        name = value.split(",")[-1].strip()
        return Device.objects.filter(name=name).first()

    def load_rack_lookup_cache(self) -> None:
        if self.rack_lookup_cache_loaded:
            return

        self.rack_lookup_cache = {}

        for rack in Rack.objects.all().select_related("location"):
            for key in self.extract_rack_lookup_keys(rack.name):
                self.rack_lookup_cache.setdefault(key, []).append(rack)

        self.rack_lookup_cache_loaded = True

    def add_rack_to_lookup_cache(self, rack: Rack) -> None:
        if not self.rack_lookup_cache_loaded:
            return

        for key in self.extract_rack_lookup_keys(rack.name):
            cached_racks = self.rack_lookup_cache.setdefault(key, [])
            if rack not in cached_racks:
                cached_racks.append(rack)

    def extract_rack_lookup_keys(self, value: str) -> set:
        """
        Build deterministic rack lookup aliases from either a Patch Manager
        identifier token or an imported Nautobot rack name.

        This makes these imported rack names directly discoverable by their
        short aliases during device placement:

        - Ashburn VA - PoP Cabinet 0316 -> cabinet 0316
        - Binghamton - PoP Cabinet 001 -> cabinet 001
        - New York City - PoP 177828-COLO-CCF -> 177828-colo-ccf
        - 2405.14 NYPH -> 2405.14 nyph
        """
        normalized = self.normalize_pm_match_text((value or "").replace("<COMMA>", ","))

        if not normalized:
            return set()

        keys = {
            normalized,
            self.normalize_rack_name_order(normalized),
        }

        # Rack-number aliases.
        for number_token in self.extract_rack_number_tokens(normalized):
            keys.add(number_token)
            keys.add(f"rack {number_token}")

        # Cabinet aliases, anywhere in the name/token.
        for cabinet_match in re.finditer(r"\bcabinet\s+([a-z0-9-]+)\b", normalized):
            keys.add(f"cabinet {cabinet_match.group(1)}")

        # COLO cabinet aliases, anywhere in the name/token.
        for colo_match in re.finditer(r"\b(\d{5,}-colo-[a-z0-9-]+)\b", normalized):
            keys.add(colo_match.group(1))

        # Leading decimal rack aliases, preserving the whole token.
        decimal_match = re.search(r"^(\d+\.\d+\b.*)$", normalized)
        if decimal_match:
            keys.add(decimal_match.group(1).strip())

        # Also add embedded decimal rack aliases when they look like cabinet IDs
        # or rack/location IDs in a longer imported rack name.
        for embedded_decimal in re.finditer(r"\b(\d+\.\d+\b[^,]*)", normalized):
            keys.add(embedded_decimal.group(1).strip())

        return {key for key in keys if key}

    def should_attempt_rack_lookup(self, value: str) -> bool:
        normalized = self.normalize_pm_match_text(value)

        if not normalized or normalized in IGNORED_RACK_LOOKUP_TOKENS:
            return False

        if self.extract_rack_lookup_keys(normalized):
            return True

        return any(keyword in normalized for keyword in RACK_LOOKUP_KEYWORDS)

    def find_rack(self, value: str) -> Optional[Rack]:
        if not value or not self.should_attempt_rack_lookup(value):
            return None

        self.load_rack_lookup_cache()
        candidates = self.get_rack_lookup_candidates(value)

        for candidate in candidates:
            normalized_candidate = self.normalize_pm_match_text(candidate)
            candidate_keys = self.extract_rack_lookup_keys(normalized_candidate)

            for key in candidate_keys:
                cached_matches = self.rack_lookup_cache.get(key, [])
                if cached_matches:
                    return cached_matches[0]

        normalized_candidates = {
            self.normalize_pm_match_text(candidate)
            for candidate in candidates
            if self.normalize_pm_match_text(candidate)
        }

        # Conservative cached fallback: compare only against cached rack entries,
        # but do not query Rack.objects.all() per token.
        possible_racks: List[Rack] = []
        seen_rack_ids = set()

        for cached_racks in self.rack_lookup_cache.values():
            for rack in cached_racks:
                if rack.pk in seen_rack_ids:
                    continue
                seen_rack_ids.add(rack.pk)
                possible_racks.append(rack)

        for rack in possible_racks:
            normalized_rack_name = self.normalize_pm_match_text(rack.name)
            rack_numbers = self.extract_rack_number_tokens(normalized_rack_name)

            if not rack_numbers:
                continue

            for normalized_candidate in normalized_candidates:
                candidate_numbers = self.extract_rack_number_tokens(normalized_candidate)
                if not candidate_numbers or not (rack_numbers & candidate_numbers):
                    continue

                if (
                    normalized_candidate in normalized_rack_name
                    or normalized_rack_name in normalized_candidate
                    or self.rack_context_overlaps(normalized_candidate, normalized_rack_name)
                ):
                    return rack

        return None

    @staticmethod
    def extract_rack_number_tokens(value: str) -> set:
        tokens = set()

        for match in re.finditer(r"\b(\d+)\.0*(\d+)\b", value or ""):
            tokens.add(f"{match.group(1)}.{int(match.group(2))}")

        return tokens

    @staticmethod
    def rack_context_overlaps(left: str, right: str) -> bool:
        left_tokens = {token for token in re.split(r"[^a-z0-9]+", left) if len(token) >= 3}
        right_tokens = {token for token in re.split(r"[^a-z0-9]+", right) if len(token) >= 3}
        ignored = {"rack", "panel", "floor", "colo", "cage", "pop", "the"}
        return bool((left_tokens - ignored) & (right_tokens - ignored))

    def get_rack_lookup_candidates(self, value: str) -> List[str]:
        """
        Return possible Nautobot rack names from a Patch Manager rack identifier.
        '<COMMA>' is an escaped comma inside a single Patch Manager field.
        """
        raw = self.clean(value)
        unescaped = raw.replace("<COMMA>", ",")

        candidates = [
            raw,
            unescaped,
        ]

        last_segment = unescaped.split(",")[-1].strip()
        if last_segment:
            candidates.append(last_segment)

        rack_match = re.search(r"\brack\s+(\d+)\.0*(\d+)\b", unescaped, re.IGNORECASE)
        if rack_match:
            candidates.append(f"Rack {rack_match.group(1)}.{int(rack_match.group(2))}")

        cabinet_match = re.search(r"\bcabinet\s+([a-z0-9-]+)\b", unescaped, re.IGNORECASE)
        if cabinet_match:
            cabinet_name = f"Cabinet {cabinet_match.group(1)}"
            candidates.append(cabinet_name)

        colo_match = re.search(r"\b(\d{5,}-COLO-[A-Za-z0-9-]+)\b", unescaped, re.IGNORECASE)
        if colo_match:
            candidates.append(colo_match.group(1))

        decimal_match = re.search(r"^(\d+\.\d+\b.*)$", unescaped)
        if decimal_match:
            candidates.append(decimal_match.group(1).strip())

        expanded_candidates: List[str] = []
        for candidate in candidates:
            expanded_candidates.append(candidate)
            expanded_candidates.append(
                re.sub(
                    r"(\d+)\.0+(\d+)",
                    lambda match: f"{match.group(1)}.{int(match.group(2))}",
                    candidate,
                )
            )

        unique_candidates: List[str] = []
        for candidate in expanded_candidates:
            candidate = re.sub(r"\s+", " ", candidate.strip())
            if candidate and candidate not in unique_candidates:
                unique_candidates.append(candidate)

        return unique_candidates

    def find_rack_from_names(
        self,
        rack_names: List[str],
        location: Optional[Location] = None,
    ) -> Optional[Rack]:
        for rack_name in rack_names:
            rack = self.find_rack(rack_name)
            if rack:
                return rack

        return self.get_or_create_virtual_rack_from_identifier_parts(rack_names, location)

    def get_or_create_virtual_rack_from_identifier_parts(
        self,
        identifier_parts: List[str],
        location: Optional[Location] = None,
    ) -> Optional[Rack]:
        """
        Create a virtual rack only for unresolved non-rack infrastructure buckets.

        This deliberately does NOT create virtual racks from:
        - generic location hierarchy tokens
        - actual rack-looking strings
        - customer/descriptor tokens such as Netflix
        - device names
        """
        if not identifier_parts:
            return None

        parts = [self.clean(part).replace("<COMMA>", ",") for part in identifier_parts if self.clean(part)]
        if not parts:
            return None

        # If a real rack-looking value exists anywhere in the identifier, do not
        # create a virtual rack. The real rack normalizer should handle it.
        for part in parts:
            if self.looks_like_real_rack_reference(part):
                return None

        virtual_part = self.find_virtual_rack_part(parts)
        if not virtual_part:
            return None

        site_name = parts[0]
        virtual_rack_name = self.normalize_virtual_rack_name(site_name, virtual_part)

        if not virtual_rack_name:
            return None

        rack_location = location or self.get_or_create_location(site_name)
        status = self.get_status()

        rack, created = Rack.objects.update_or_create(
            name=virtual_rack_name,
            location=rack_location,
            defaults={
                "status": status,
                "u_height": DEFAULT_RACK_HEIGHT,
                "comments": "Virtual rack created by Patch Manager import for non-rack infrastructure grouping.",
            },
        )

        self.add_rack_to_lookup_cache(rack)

        self.logger.info(
            "%s virtual rack %s for unresolved Patch Manager infrastructure bucket",
            "Created" if created else "Updated",
            rack.name,
        )

        return rack

    @staticmethod
    def normalize_virtual_rack_name(site_name: str, virtual_part: str) -> str:
        site = re.sub(r"\s+", " ", (site_name or "").strip())
        bucket = re.sub(r"\s+", " ", (virtual_part or "").strip())

        if not site or not bucket:
            return ""

        if bucket.lower().startswith(site.lower()):
            return bucket

        return f"{site} {bucket}"

    @staticmethod
    def looks_like_real_rack_reference(value: str) -> bool:
        normalized = re.sub(r"\s+", " ", (value or "").replace("<COMMA>", ",").strip().lower())

        if not normalized:
            return False

        if re.search(r"\brack\s+\d+\.\d+\b", normalized):
            return True

        if re.search(r"^\d+\.\d+\b", normalized):
            return True

        return False

    @staticmethod
    def find_virtual_rack_part(parts: List[str]) -> str:
        """
        Pick only true infrastructure bucket tokens for virtual rack creation.
        """
        virtual_keywords = (
            "fdp",
            "panel",
            "panels",
            "cage",
        )

        ignored_exact = set(IGNORED_RACK_LOOKUP_TOKENS) | {
            "nysernet cage",
            "nysenet cage",
            "netflix",
            "san. netflix",
        }

        for part in parts[3:]:
            normalized = re.sub(r"\s+", " ", part.strip().lower())

            if not normalized or normalized in ignored_exact:
                continue

            if "rack" in normalized:
                continue

            if re.search(r"\b[a-z]{2,10}[a-z0-9]*-[a-z0-9][a-z0-9-]*\b", normalized):
                continue

            if any(keyword in normalized for keyword in virtual_keywords):
                return part.strip()

        return ""

    def get_or_create_location(self, qname: str, physical_address: str = "") -> Location:
        parts = [p.strip() for p in qname.split(",") if p.strip()]

        if len(parts) >= 3:
            name = f"{parts[1]} {parts[2]}"
        elif len(parts) >= 2:
            name = parts[1]
        elif parts:
            name = parts[-1]
        else:
            name = "Patch Manager"

        location_type, _ = LocationType.objects.get_or_create(
            name=DEFAULT_LOCATION_TYPE_NAME,
            defaults={"nestable": True},
        )
        self.ensure_location_type_content_types(location_type)

        location, _ = Location.objects.get_or_create(
            name=name,
            defaults={
                "location_type": location_type,
                "status": self.get_status(),
                "physical_address": physical_address,
            },
        )

        if location.location_type:
            self.ensure_location_type_content_types(location.location_type)

        if physical_address and not location.physical_address:
            location.physical_address = physical_address
            location.validated_save()

        return location

    def ensure_location_type_content_types(self, location_type: LocationType) -> None:
        """
        Nautobot validates whether a LocationType may contain specific object
        types. Imported racks and devices need the selected LocationType to
        allow Rack and Device assignments.
        """
        required_content_types = [
            ContentType.objects.get_for_model(Rack),
            ContentType.objects.get_for_model(Device),
        ]

        for content_type in required_content_types:
            location_type.content_types.add(content_type)

    def ensure_role_content_types(self, role: Role) -> None:
        """
        Nautobot validates Role choices by content type. Imported device roles
        must be enabled for Device objects before assigning them to Device.role.
        """
        role.content_types.add(ContentType.objects.get_for_model(Device))

    def get_or_create_device_type(self, name: str) -> DeviceType:
        clean_name = self.clean(name) or "Unknown Patch Manager Equipment"

        manufacturer_name, model_name = self.parse_manufacturer_and_model(clean_name)

        manufacturer, _ = Manufacturer.objects.get_or_create(
            name=manufacturer_name
        )

        device_type, _ = DeviceType.objects.get_or_create(
            manufacturer=manufacturer,
            model=model_name,
        )

        return device_type

    def parse_manufacturer_and_model(self, value: str) -> Tuple[str, str]:
        """
        Parse Patch Manager equipment templates into Nautobot manufacturer/model.

        Examples:
        - "Cisco NX540" -> ("Cisco", "NX540")
        - "Juniper MX960" -> ("Juniper", "MX960")
        - "Ciena 5171" -> ("Ciena", "5171")
        """
        clean_value = self.clean(value)

        if not clean_value:
            return DEFAULT_MANUFACTURER_NAME, "Unknown"

        known_manufacturers = {
            "Cisco",
            "Juniper",
            "Ciena",
            "Arista",
            "Nokia",
            "Adtran",
            "Fujitsu",
            "Infinera",
            "Ekinops",
            "HP",
            "HPE",
            "Dell",
            "Supermicro",
        }

        for manufacturer in sorted(known_manufacturers, key=len, reverse=True):
            if clean_value.lower().startswith(manufacturer.lower() + " "):
                model = clean_value[len(manufacturer):].strip()
                return manufacturer, model or clean_value

        parts = clean_value.split(None, 1)
        if len(parts) == 2 and len(parts[0]) > 2:
            return parts[0], parts[1]

        return DEFAULT_MANUFACTURER_NAME, clean_value

    def get_or_create_device_role(self, name: str) -> Role:
        role_name = name or DEFAULT_DEVICE_ROLE_NAME
        role, _ = Role.objects.get_or_create(name=role_name)
        self.ensure_role_content_types(role)
        return role

    def get_status(self) -> Status:
        status = Status.objects.filter(name=DEFAULT_STATUS_NAME).first()
        if not status:
            raise RuntimeError(f"Nautobot status {DEFAULT_STATUS_NAME!r} was not found.")
        return status

    @staticmethod
    def clean(value: Any) -> str:
        return "" if value is None else str(value).strip()


jobs = [PatchManagerImport]
