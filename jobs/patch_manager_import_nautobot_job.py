"""
Nautobot Job: Import racks, devices, cables, and connection data from Patch Manager.

Current behavior
----------------
- Pull-only from Patch Manager.
- Uses Nautobot Secrets Group "PatchManagerAPI".
- Expects Secret names "PatchManagerUser" and "PatchManagerPassword" in that group.
- Pulls:
  - /rest/cabinets?format=Cabinet+Export
  - /rest/equipment?format=Equipment+Export
  - /rest/cables?format=Cable+Export
- Does not call /rest/ports.
- Creates interfaces on demand from Cable Export connection strings.
- Parses Equipment Identifier as:
    role, location, address, rack1, rack2, ...
- Uses the parsed role as the Nautobot Device Role.
- Uses the parsed location as the fallback Nautobot Location.
- Uses the parsed address as the device comments fallback.
- Matches the first parsed rack name that exists from the rack import.
- Uses Equipment Label as the device name, falling back to the last Equipment Identifier value.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests
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
    "rack_location": "Cabinet Location",
    "rack_template": "Cabinet Template",
    "rack_description": "Cabinet Description",
    "device_name": "Equipment Label",
    "device_identifier": "Equipment Identifier",
    "device_type": "Equipment Template",
    "device_rack": "Equipment Position Cabinet",
    "device_location": "Equipment Location",
    "device_position": "Equipment Position",
    "device_description": "Equipment Description",
    "cable_label": "Cable Label",
    "cable_type": "Cable Template",
    "cable_description": "Cable Description",
    "connection_left": "Connections Left",
    "connection_right": "Connections Right",
}

SECRETS_GROUP_NAME = "PatchManagerAPI"
USERNAME_SECRET_NAME = "PatchManagerUser"
PASSWORD_SECRET_NAME = "PatchManagerPassword"

PORT_TYPE_DEFAULT = "1000base-t"
DEFAULT_MANUFACTURER_NAME = "Patch Manager"
DEFAULT_LOCATION_TYPE_NAME = "Patch Manager Site"
DEFAULT_STATUS_NAME = "Active"
DEFAULT_DEVICE_ROLE_NAME = "Patch Manager Imported"
DEFAULT_LOCATION_NAME = "Patch Manager"

CONNECTION_SPLIT_RE = re.compile(r"\s*\|\s*|\s*;\s*")
PM_TEMPLATE_BRACKET_RE = re.compile(r"\[[^\]]+\]")


@dataclass(frozen=True)
class PMEndpoint:
    raw: str
    device_name: str
    port_name: str


@dataclass(frozen=True)
class PMEquipmentIdentifier:
    role: str
    location: str
    address: str
    racks: List[str]


class PatchManagerClient:
    """Small Patch Manager REST client."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        verify_ssl: bool = True,
        timeout: int = 60,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = verify_ssl
        self.timeout = timeout

    def get_collection(self, resource: str, fmt: str, limit: int = 500) -> List[Dict[str, Any]]:
        """Read all rows from a Patch Manager resource using start/limit pagination."""
        rows: List[Dict[str, Any]] = []
        start = 0

        while True:
            params = {"start": start, "limit": limit}
            if fmt:
                # Patch Manager accepted Cabinet+Export via curl.
                # urlencode() encodes spaces as "+", matching that behavior.
                params["format"] = fmt

            url = f"{self.base_url}/rest/{resource}?{urlencode(params)}"
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code >= 400:
                hint = ""
                if response.status_code == 400 and "format=" in url:
                    hint = (
                        " Hint: Patch Manager returned 400 for a formatted GET. "
                        "Verify that the named data format exists and is the correct resource type "
                        "for this endpoint, for example a Cabinets data format for /rest/cabinets."
                    )
                raise requests.HTTPError(
                    f"{response.status_code} Client Error for url: {url}; "
                    f"response body: {response.text[:1000]}{hint}",
                    response=response,
                )

            payload = response.json()
            batch = self._normalize_collection(payload, resource)
            rows.extend(batch)

            if len(batch) < limit:
                break
            start += limit

        return rows

    @staticmethod
    def _normalize_collection(payload: Any, resource: str) -> List[Dict[str, Any]]:
        """Patch Manager may return a bare list or a dict containing a resource list."""
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]

        if isinstance(payload, dict):
            for key in (resource, resource.replace("-", " "), resource.title(), "items", "results", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]

            # A single object response.
            if all(isinstance(k, str) for k in payload.keys()):
                return [payload]

        return []


# Expected Nautobot Secrets configuration:
# - Secrets Group: PatchManagerAPI
# - Secret name in that group: PatchManagerUser
# - Secret name in that group: PatchManagerPassword
#
# This job resolves the Secret records by name within the group, so the group's
# access_type and secret_type labels do not need to be hard-coded in the job.
class PatchManagerImport(Job):
    """Import Patch Manager cabinets, equipment, and cables into Nautobot."""

    class Meta:
        name = "Import Patch Manager Inventory"
        description = "Imports Patch Manager cabinets, equipment, cables, and cable endpoint connections into Nautobot."

    patch_manager_url = StringVar(
        description="Patch Manager base URL, for example https://nysernet.patchmanager.com",
        default="https://nysernet.patchmanager.com",
    )
    verify_ssl = BooleanVar(default=True, description="Verify Patch Manager TLS certificate")
    dryrun = BooleanVar(default=True, description="Validate and log changes without writing to Nautobot")
    page_size = IntegerVar(default=500, min_value=1, max_value=5000)

    rack_format = StringVar(
        default=DEFAULT_FORMATS["rack_format"],
        description="Patch Manager Cabinets data format.",
    )
    device_format = StringVar(
        default=DEFAULT_FORMATS["device_format"],
        description="Patch Manager Equipment data format.",
    )
    cable_format = StringVar(
        default=DEFAULT_FORMATS["cable_format"],
        description="Patch Manager Cables data format.",
    )

    import_mode = ChoiceVar(
        choices=(
            ("all", "Racks, devices, cables"),
            ("inventory", "Racks and devices only"),
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
            if kwargs["import_mode"] in ("all", "inventory"):
                racks = client.get_collection("cabinets", kwargs["rack_format"], self.page_size)
                devices = client.get_collection("equipment", kwargs["device_format"], self.page_size)
                self.import_racks(racks)
                self.import_devices(devices)

            if kwargs["import_mode"] in ("all", "cables"):
                cables = client.get_collection("cables", kwargs["cable_format"], self.page_size)
                self.import_cables(cables)

            if self.dryrun:
                self.logger.warning("Dry run enabled; rolling back all database changes.")
                transaction.set_rollback(True)

    def import_racks(self, rows: Iterable[Dict[str, Any]]) -> None:
        status = self.get_status()

        for row in rows:
            name = self.clean(row.get(self.fields["rack_name"]))
            if not name:
                self.logger.warning("Skipping rack without name: %s", row)
                continue

            location_name = self.clean(row.get(self.fields["rack_location"])) or DEFAULT_LOCATION_NAME
            location = self.get_or_create_location(location_name)

            rack, created = Rack.objects.update_or_create(
                name=name,
                location=location,
                defaults={
                    "status": status,
                    "comments": self.clean(row.get(self.fields["rack_description"])),
                },
            )
            self.logger.info("%s rack %s", "Created" if created else "Updated", rack)

    def import_devices(self, rows: Iterable[Dict[str, Any]]) -> None:
        status = self.get_status()

        for row in rows:
            identifier_raw = self.clean(row.get(self.fields["device_identifier"]))
            identifier = self.parse_equipment_identifier(identifier_raw)

            name = self.clean(row.get(self.fields["device_name"]))
            if not name and identifier_raw:
                # Keep the prior behavior: fallback to the last comma-separated identifier value.
                # With the new identifier shape, this is usually the deepest rack value.
                name = identifier_raw.split(",")[-1].strip() or identifier_raw
                self.logger.info("Using Equipment Identifier as device name: %s", name)

            if not name:
                self.logger.warning("Skipping device without name or identifier: %s", row)
                continue

            device_type = self.get_or_create_device_type(self.clean(row.get(self.fields["device_type"])))
            role = self.get_or_create_device_role(identifier.role)

            rack = self.find_rack_for_device(row, identifier)
            if rack:
                location = rack.location
            else:
                explicit_location = self.clean(row.get(self.fields["device_location"]))
                location = self.get_or_create_location(explicit_location or identifier.location or DEFAULT_LOCATION_NAME)

            position, face = self.parse_rack_position(self.clean(row.get(self.fields["device_position"])))
            comments = self.clean(row.get(self.fields["device_description"])) or identifier.address

            device, created = Device.objects.update_or_create(
                name=name,
                defaults={
                    "device_type": device_type,
                    "role": role,
                    "status": status,
                    "location": location,
                    "rack": rack,
                    "position": position,
                    "face": face,
                    "comments": comments,
                },
            )
            self.logger.info("%s device %s", "Created" if created else "Updated", device)

    def import_cables(self, rows: Iterable[Dict[str, Any]]) -> None:
        status = self.get_status()

        for row in rows:
            label = self.clean(row.get(self.fields["cable_label"])) or None
            left = self.parse_endpoint(self.clean(row.get(self.fields["connection_left"])))
            right = self.parse_endpoint(self.clean(row.get(self.fields["connection_right"])))

            if not left or not right:
                self.logger.warning("Skipping cable %s; missing connection endpoint", label or row)
                continue

            left_if = self.get_interface_for_endpoint(left)
            right_if = self.get_interface_for_endpoint(right)

            if not left_if or not right_if:
                self.logger.warning("Skipping cable %s; could not resolve %s -> %s", label, left, right)
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

        interface, _ = Interface.objects.get_or_create(
            device=device,
            name=endpoint.port_name,
            defaults={"type": PORT_TYPE_DEFAULT},
        )
        return interface

    def parse_endpoint(self, value: str) -> Optional[PMEndpoint]:
        """
        Parse Patch Manager connection strings.

        Common qualified-label forms:
        - Site,Room,Rack,Device,xe-0/0/0[Port Template]
        - Site,Rack,Switch[Switch 24-Port],1[RJ45 Switch Port]
        - multiple alternatives separated by | or ;, where | may mean empty/no connection.
        """
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

    def parse_equipment_identifier(self, value: str) -> PMEquipmentIdentifier:
        """
        Parse Equipment Identifier as:
            role, location, address, rack1, rack2, ...
        """
        parts = [p.strip() for p in value.split(",") if p.strip()]

        return PMEquipmentIdentifier(
            role=parts[0] if len(parts) > 0 else DEFAULT_DEVICE_ROLE_NAME,
            location=parts[1] if len(parts) > 1 else DEFAULT_LOCATION_NAME,
            address=parts[2] if len(parts) > 2 else "",
            racks=parts[3:] if len(parts) > 3 else [],
        )

    def find_rack_for_device(self, row: Dict[str, Any], identifier: PMEquipmentIdentifier) -> Optional[Rack]:
        """Find a rack from the explicit equipment rack field, then from parsed Equipment Identifier racks."""
        explicit_rack = self.find_rack(self.clean(row.get(self.fields["device_rack"])))
        if explicit_rack:
            return explicit_rack

        for rack_name in identifier.racks:
            rack = self.find_rack(rack_name)
            if rack:
                self.logger.info("Matched rack %s from Equipment Identifier", rack.name)
                return rack

        return None

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

    def get_or_create_location(self, qname: str) -> Location:
        parts = [p.strip() for p in qname.split(",") if p.strip()]
        name = parts[-1] if parts else DEFAULT_LOCATION_NAME

        location_type, _ = LocationType.objects.get_or_create(
            name=DEFAULT_LOCATION_TYPE_NAME,
            defaults={"nestable": True},
        )
        status = self.get_status()

        location, _ = Location.objects.get_or_create(
            name=name,
            defaults={
                "location_type": location_type,
                "status": status,
            },
        )
        return location

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
        return role

    def get_status(self) -> Status:
        status = Status.objects.filter(name=DEFAULT_STATUS_NAME).first()
        if not status:
            raise RuntimeError(f"Nautobot status {DEFAULT_STATUS_NAME!r} was not found.")
        return status

    @staticmethod
    def parse_rack_position(value: str) -> Tuple[Optional[int], str]:
        if not value:
            return None, "front"

        match = re.search(r"U?\s*(\d+)", value, re.IGNORECASE)
        position = int(match.group(1)) if match else None

        face = "front"
        if "rear" in value.lower():
            face = "rear"
        elif "front" in value.lower():
            face = "front"

        return position, face

    @staticmethod
    def clean(value: Any) -> str:
        return "" if value is None else str(value).strip()


jobs = [PatchManagerImport]

