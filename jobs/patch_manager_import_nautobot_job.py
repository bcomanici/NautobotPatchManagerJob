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
from django.db import transaction
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
DEFAULT_RACK_HEIGHT = 42

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

        with transaction.atomic():
            if kwargs["import_mode"] in ("all", "inventory", "racks"):
                self.import_racks(client.get_collection("cabinets", kwargs["rack_format"], self.page_size))

            if kwargs["import_mode"] in ("all", "inventory", "equipment"):
                equipment_rows = client.get_collection("equipment", kwargs["device_format"], self.page_size)
                skipped_port_detail_rows = self.import_devices(equipment_rows)
                self.apply_skipped_device_port_details(skipped_port_detail_rows)

            if kwargs["import_mode"] in ("all", "cables"):
                self.import_cables(client.get_collection("cables", kwargs["cable_format"], self.page_size))

            if self.dryrun:
                self.logger.warning("Dry run enabled; rolling back all database changes.")
                transaction.set_rollback(True)

    def import_racks(self, rows: Iterable[Dict[str, Any]]) -> None:
        status = self.get_status()

        for row in rows:
            name = self.clean(row.get(self.fields["rack_name"]))
            identifier = self.clean(row.get(self.fields["rack_identifier"]))
            rack_template = self.clean(row.get(self.fields["rack_template"]))

            if not name:
                self.logger.warning("Skipping rack without name: %s", row)
                continue

            if name.lower().startswith("rack") and identifier:
                identifier_parts = [p.strip() for p in identifier.split(",") if p.strip()]
                if len(identifier_parts) > 1:
                    name = f"{identifier_parts[1]} {name}"

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

            self.logger.info(
                "%s rack %s (%sU)",
                "Created" if created else "Updated",
                rack.name,
                rack.u_height,
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
            rack = self.find_rack_from_names(identifier_data["racks"])

            if rack:
                location = rack.location
                if location and location.location_type:
                    self.ensure_location_type_content_types(location.location_type)
            else:
                location = self.get_or_create_location(
                    identifier_data["location"] or "Patch Manager",
                    identifier_data["address"],
                )

            position, face = self.parse_equipment_position(self.clean(row.get(self.fields["device_position"])))

            if position is None:
                self.logger.info(
                    "Cataloging skipped device %s; no valid U position found in Equipment Position",
                    name,
                )
                skipped_port_detail_rows.append(row)
                continue

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
                    face = "front"

            device_defaults = {
                "device_type": device_type,
                "role": role,
                "status": status,
                "location": location,
                "rack": rack,
                "position": position,
                "face": face or "front",
                "comments": self.clean(row.get(self.fields["device_description"])) or identifier_data["address"],
            }

            try:
                device, created = Device.objects.update_or_create(
                    name=name,
                    defaults=device_defaults,
                )
            except ValidationError as exc:
                message_dict = getattr(exc, "message_dict", {})

                if "position" in message_dict:
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
                    device_defaults["face"] = "front"

                    try:
                        device, created = Device.objects.update_or_create(
                            name=name,
                            defaults=device_defaults,
                        )
                    except ValidationError as retry_exc:
                        retry_message_dict = getattr(retry_exc, "message_dict", {})
                        if "face" not in retry_message_dict:
                            raise

                        self.logger.warning(
                            "Rack face validation failed for %s after clearing position: %s. "
                            "Retrying import without rack or rack position.",
                            name,
                            retry_exc,
                        )

                        device_defaults["rack"] = None
                        device_defaults["position"] = None
                        device_defaults["face"] = "front"
                        device, created = Device.objects.update_or_create(
                            name=name,
                            defaults=device_defaults,
                        )

                elif "face" in message_dict:
                    self.logger.warning(
                        "Rack face validation failed for %s at rack=%s position=%s face=%s: %s. "
                        "Retrying import without rack or rack position.",
                        name,
                        rack.name if rack else None,
                        position,
                        face,
                        exc,
                    )

                    device_defaults["rack"] = None
                    device_defaults["position"] = None
                    device_defaults["face"] = "front"
                    device, created = Device.objects.update_or_create(
                        name=name,
                        defaults=device_defaults,
                    )

                else:
                    raise

            self.logger.info("%s device %s", "Created" if created else "Updated", device.name)

        return skipped_port_detail_rows

    def apply_skipped_device_port_details(self, rows: Iterable[Dict[str, Any]]) -> None:
        details_by_device_id: Dict[int, List[Dict[str, str]]] = {}

        for row in rows:
            target_device = self.find_imported_device_in_identifier(
                self.clean(row.get(self.fields["device_identifier"]))
            )

            if not target_device:
                self.logger.info(
                    "Skipping port detail row; could not find imported parent device from Equipment Identifier: %s",
                    row.get(self.fields["device_identifier"]),
                )
                continue

            details = self.extract_port_detail_fields(row)
            if not details:
                self.logger.info(
                    "Skipping port detail row for %s; no port detail fields populated",
                    target_device.name,
                )
                continue

            details_by_device_id.setdefault(target_device.pk, []).append(details)

        for device_id, detail_rows in details_by_device_id.items():
            device = Device.objects.get(pk=device_id)
            device.comments = self.replace_pm_port_details_block(device.comments or "", detail_rows)
            device.validated_save()
            self.logger.info("Updated Patch Manager port details on device %s", device.name)

    def find_imported_device_in_identifier(self, value: str) -> Optional[Device]:
        """
        Resolve a skipped Patch Manager equipment row back to an already-imported,
        rack-mounted Nautobot device.

        Matching order:
        1. Preferred: Equipment Identifier contains a rack reference matching an
           imported Nautobot rack after normalization, and a mounted device in
           that rack has a normalized name contained in the identifier.
        2. Safe fallback: if rack-scoped matching fails, look for an exact
           comma-separated Equipment Identifier token that matches an existing
           mounted Nautobot device name. If multiple mounted devices share that
           name, prefer one whose rack is also hinted in the identifier.
        """
        if not value:
            return None

        identifier = self.clean(value)
        identifier_parts = [p.strip() for p in identifier.replace("<COMMA>", ",").split(",") if p.strip()]
        matched_racks = self.find_racks_in_identifier(identifier_parts)
        normalized_identifier = self.normalize_pm_match_text(identifier)

        rack_scoped_match = self.find_device_by_rack_scoped_contains(
            matched_racks=matched_racks,
            normalized_identifier=normalized_identifier,
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

    def find_device_by_rack_scoped_contains(
        self,
        matched_racks: List[Rack],
        normalized_identifier: str,
    ) -> Optional[Device]:
        if not matched_racks:
            return None

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

    def find_rack(self, value: str) -> Optional[Rack]:
        if not value:
            return None

        name = value.split(",")[-1].strip()
        return Rack.objects.filter(name=name).first()

    def find_rack_from_names(self, rack_names: List[str]) -> Optional[Rack]:
        for rack_name in rack_names:
            rack = self.find_rack(rack_name)
            if rack:
                return rack

        return None

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
        clean_name = name or "Unknown Patch Manager Equipment"
        manufacturer, _ = Manufacturer.objects.get_or_create(name=DEFAULT_MANUFACTURER_NAME)

        device_type, _ = DeviceType.objects.get_or_create(
            manufacturer=manufacturer,
            model=clean_name,
        )

        return device_type

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
