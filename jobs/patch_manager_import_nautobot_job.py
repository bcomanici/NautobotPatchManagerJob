"""
Nautobot Job: Import racks, devices, cables, and connection data from Patch Manager.

Assumptions
-----------
- Nautobot 2.x job file, placed in a Git-backed Jobs repo or $NAUTOBOT_ROOT/jobs/.
- Patch Manager data formats exist and expose the fields configured below.
- Patch Manager cabinets map to Nautobot Racks.
- Patch Manager equipment maps to Nautobot Devices.
- Patch Manager cables map to Nautobot Cables, using the left/right connection fields from the cable format.
- No /rest/ports request is made; interfaces are created on demand from cable endpoint strings.

Recommended Patch Manager data formats
--------------------------------------
Create these data formats in Patch Manager with the exact headers used in DEFAULT_FORMATS,
or override the headers in the Job form.

Cabinet/Rack format:
- Entity Id
- Cabinet Name
- Cabinet Location
- Cabinet Template
- Cabinet Description

Equipment/Device format:
- Entity Id
- Equipment Label
- Equipment Template
- Equipment Position Cabinet
- Equipment Position
- Equipment Description

Cable format:
- Entity Id
- Cable Label
- Cable Template
- Cable Description
- Connections Left
- Connections Right

"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from django.db import transaction
from django.utils.text import slugify
from nautobot.apps.jobs import BooleanVar, ChoiceVar, IntegerVar, Job, StringVar
from nautobot.dcim.models import Cable, Device, DeviceType, Interface, Location, LocationType, Manufacturer, Rack
from nautobot.extras.models import SecretsGroup, SecretsGroupAssociation, Status


DEFAULT_FORMATS = {
    # Empty string means do not send the format query parameter.
    # Patch Manager then uses its default Standard format for the resource.
    "rack_format": "Cabinet Export",
    "device_format": "Equipment Export",
    "cable_format": "Cable Export",
}

DEFAULT_FIELDS = {
    "entity_id": "Entity Id",
    "rack_name": "Cabinet Name",
    "rack_location": "Cabinet Location",
    "rack_template": "Cabinet Template",
    "rack_description": "Cabinet Description",
    "device_name": "Equipment Label",
    "device_type": "Equipment Template",
    "device_rack": "Equipment Position Cabinet",
    "device_position": "Equipment Position",
    "device_description": "Equipment Description",
    "cable_label": "Cable Label",
    "cable_type": "Cable Template",
    "cable_description": "Cable Description",
    "connection_left": "Connections Left",
    "connection_right": "Connections Right",
}

PORT_TYPE_DEFAULT = "1000base-t"
DEFAULT_MANUFACTURER_NAME = "Patch Manager"
DEFAULT_LOCATION_TYPE_NAME = "Patch Manager Site"
DEFAULT_STATUS_NAME = "Active"
CONNECTION_SPLIT_RE = re.compile(r"\s*\|\s*|\s*;\s*")
PM_TEMPLATE_BRACKET_RE = re.compile(r"\[[^\]]+\]")


@dataclass(frozen=True)
class PMEndpoint:
    raw: str
    device_name: str
    port_name: str


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
                    f"{response.status_code} Client Error for url: {url}; response body: {response.text[:1000]}{hint}",
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
#
class PatchManagerImport(Job):
    """Import racks, devices, cables, and connections from Patch Manager."""

    class Meta:
        name = "Import Patch Manager Inventory"
        description = "Imports Patch Manager cabinets, equipment, cables, and port connections into Nautobot."
        has_sensitive_variables = True

    patch_manager_url = StringVar(description="Patch Manager base URL, for example https://patchmanager.example.org")
    verify_ssl = BooleanVar(default=True, description="Verify Patch Manager TLS certificate")
    dryrun = BooleanVar(default=True, description="Validate and log changes without writing to Nautobot")
    page_size = IntegerVar(default=500, min_value=1, max_value=5000)

    rack_format = StringVar(
        default=DEFAULT_FORMATS["rack_format"],
        description="Optional Patch Manager Cabinets data format. Leave blank to use Patch Manager's default Standard format.",
    )
    device_format = StringVar(
        default=DEFAULT_FORMATS["device_format"],
        description="Optional Patch Manager Equipment data format. Leave blank to use Patch Manager's default Standard format.",
    )
    cable_format = StringVar(
        default=DEFAULT_FORMATS["cable_format"],
        description="Optional Patch Manager Cables data format. Leave blank to use Patch Manager's default Standard format.",
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
        username = self.get_secret_from_group("PatchManagerAPI", "PatchManagerUser")
        password = self.get_secret_from_group("PatchManagerAPI", "PatchManagerPassword")

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
            location_name = self.clean(row.get(self.fields["rack_location"])) or "Patch Manager"
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
            name = self.clean(row.get(self.fields["device_name"]))
            if not name:
                self.logger.warning("Skipping device without name: %s", row)
                continue
            device_type = self.get_or_create_device_type(self.clean(row.get(self.fields["device_type"])))
            rack = self.find_rack(self.clean(row.get(self.fields["device_rack"])))
            location = rack.location if rack else self.get_or_create_location("Patch Manager")
            position, face = self.parse_rack_position(self.clean(row.get(self.fields["device_position"])))

            device, created = Device.objects.update_or_create(
                name=name,
                defaults={
                    "device_type": device_type,
                    "role": None,
                    "status": status,
                    "location": location,
                    "rack": rack,
                    "position": position,
                    "face": face,
                    "comments": self.clean(row.get(self.fields["device_description"])),
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
                self.logger.info("Skipping cable %s; one endpoint is already cabled", label or f"{left.raw} -> {right.raw}")
                continue
            cable = Cable.objects.create(
                termination_a=left_if,
                termination_b=right_if,
                status=status,
                label=label,
                comments=self.clean(row.get(self.fields["cable_description"])),
            )
            self.logger.info("Created cable %s: %s:%s -> %s:%s", cable.label or cable.pk, left.device_name, left.port_name, right.device_name, right.port_name)

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
        interface, _ = Interface.objects.get_or_create(device=device, name=endpoint.port_name, defaults={"type": PORT_TYPE_DEFAULT})
        return interface

    def parse_endpoint(self, value: str) -> Optional[PMEndpoint]:
        """
        Parse Patch Manager connection strings.

        The guide states that cable connection fields use Patch Manager's cable-import connection format
        and may include entity IDs. This parser handles common qualified-label forms such as:
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
        name = parts[-1] if parts else "Patch Manager"
        location_type, _ = LocationType.objects.get_or_create(name=DEFAULT_LOCATION_TYPE_NAME, defaults={"nestable": True})
        status = self.get_status()
        location, _ = Location.objects.get_or_create(
            name=name,
            defaults={"location_type": location_type, "status": status},
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

    def get_status(self) -> Status:
        status = Status.objects.filter(name=DEFAULT_STATUS_NAME).first()
        if not status:
            raise RuntimeError(f"Nautobot status {DEFAULT_STATUS_NAME!r} was not found.")
        return status

    @staticmethod
    def parse_rack_position(value: str) -> Tuple[Optional[int], Optional[str]]:
        if not value:
            return None, None
        match = re.search(r"U?\s*(\d+)", value, re.IGNORECASE)
        position = int(match.group(1)) if match else None
        face = None
        if "rear" in value.lower():
            face = "rear"
        elif "front" in value.lower():
            face = "front"
        return position, face

    @staticmethod
    def clean(value: Any) -> str:
        return "" if value is None else str(value).strip()
