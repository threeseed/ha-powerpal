"""Config flow for Powerpal BLE."""

from __future__ import annotations

import logging
import re
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.components import bluetooth
from homeassistant.components.bluetooth import BluetoothServiceInfoBleak
from homeassistant.const import CONF_NAME
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult

from .const import (
    CONF_ADDRESS,
    CONF_DROP_STALE_MEASUREMENTS,
    CONF_NOTIFICATION_INTERVAL,
    CONF_PAIRING_CODE,
    CONF_PULSES_PER_KWH,
    CONF_STALE_MEASUREMENT_SECONDS,
    DEFAULT_DROP_STALE_MEASUREMENTS,
    DEFAULT_NAME,
    DEFAULT_NOTIFICATION_INTERVAL,
    DEFAULT_PULSES_PER_KWH,
    DEFAULT_STALE_MEASUREMENT_SECONDS,
    DOMAIN,
    MAX_NOTIFICATION_INTERVAL,
    MIN_NOTIFICATION_INTERVAL,
    POWERPAL_SERVICE_UUID,
)

_LOGGER = logging.getLogger(__name__)

MAC_RE = re.compile(r"^[0-9a-fA-F]{2}(:[0-9a-fA-F]{2}){5}$")
PAIRING_RE = re.compile(r"^[0-9]{1,6}$")

CONF_DISCOVERED_ADDRESS = "discovered_address"
MANUAL_ADDRESS = "__manual__"
MAX_FALLBACK_DEVICES = 20
POWERPAL_NAME_MARKERS = ("powerpal", "power pal")


def _normalise_address(address: str) -> str:
    """Normalize a Bluetooth MAC address."""
    return address.strip().upper()


def _service_uuids(discovery_info: BluetoothServiceInfoBleak) -> set[str]:
    """Return lower-case advertised service UUIDs for a discovery."""
    return {str(uuid).lower() for uuid in discovery_info.service_uuids or []}


def _rssi(discovery_info: BluetoothServiceInfoBleak) -> int | None:
    """Return RSSI if available."""
    value = getattr(discovery_info, "rssi", None)
    return value if isinstance(value, int) else None


def _powerpal_match_reason(discovery_info: BluetoothServiceInfoBleak) -> str | None:
    """Return why this discovery looks like a Powerpal device."""
    if POWERPAL_SERVICE_UUID.lower() in _service_uuids(discovery_info):
        return "Powerpal service"

    name = (discovery_info.name or "").lower()
    if any(marker in name for marker in POWERPAL_NAME_MARKERS):
        return "Powerpal name"

    return None


def _format_device_label(
    discovery_info: BluetoothServiceInfoBleak,
    *,
    reason: str | None,
) -> str:
    """Format a device label for the config flow."""
    name = discovery_info.name or "Unknown BLE device"
    address = _normalise_address(discovery_info.address)
    rssi = _rssi(discovery_info)
    signal = f", RSSI {rssi}" if rssi is not None else ""
    prefix = reason or "Unverified nearby BLE"
    return f"{prefix}: {name} ({address}{signal})"


def _candidate_sort_key(discovery_info: BluetoothServiceInfoBleak) -> tuple[int, int, str]:
    """Sort strong Powerpal matches first, then by signal strength."""
    reason = _powerpal_match_reason(discovery_info)
    priority = 0 if reason == "Powerpal service" else 1 if reason == "Powerpal name" else 2
    rssi = _rssi(discovery_info)
    return (priority, -(rssi if rssi is not None else -999), discovery_info.address)


def _validate_user_input(user_input: dict[str, Any]) -> dict[str, str]:
    """Validate the setup form."""
    errors: dict[str, str] = {}
    address = _normalise_address(str(user_input.get(CONF_ADDRESS, "")))
    pairing_code = str(user_input.get(CONF_PAIRING_CODE, "")).strip()

    if not MAC_RE.fullmatch(address):
        errors[CONF_ADDRESS] = "invalid_address"
    if not PAIRING_RE.fullmatch(pairing_code):
        errors[CONF_PAIRING_CODE] = "invalid_pairing_code"
    return errors


def _base_fields(defaults: dict[str, Any]) -> dict[Any, Any]:
    """Build the common setup/options form fields."""
    return {
        vol.Required(CONF_NAME, default=defaults.get(CONF_NAME, DEFAULT_NAME)): str,
        vol.Required(CONF_PAIRING_CODE, default=defaults.get(CONF_PAIRING_CODE, "")): str,
        vol.Required(
            CONF_PULSES_PER_KWH,
            default=defaults.get(CONF_PULSES_PER_KWH, DEFAULT_PULSES_PER_KWH),
        ): vol.All(vol.Coerce(float), vol.Range(min=1)),
        vol.Required(
            CONF_NOTIFICATION_INTERVAL,
            default=defaults.get(CONF_NOTIFICATION_INTERVAL, DEFAULT_NOTIFICATION_INTERVAL),
        ): vol.All(
            vol.Coerce(int),
            vol.Range(
                min=MIN_NOTIFICATION_INTERVAL,
                max=MAX_NOTIFICATION_INTERVAL,
            ),
        ),
        vol.Required(
            CONF_DROP_STALE_MEASUREMENTS,
            default=defaults.get(
                CONF_DROP_STALE_MEASUREMENTS, DEFAULT_DROP_STALE_MEASUREMENTS
            ),
        ): bool,
        vol.Required(
            CONF_STALE_MEASUREMENT_SECONDS,
            default=defaults.get(
                CONF_STALE_MEASUREMENT_SECONDS,
                DEFAULT_STALE_MEASUREMENT_SECONDS,
            ),
        ): vol.All(vol.Coerce(int), vol.Range(min=0, max=86400)),
    }


def _manual_schema(defaults: dict[str, Any]) -> vol.Schema:
    """Build the manual setup/options form schema."""
    fields: dict[Any, Any] = {
        vol.Required(CONF_ADDRESS, default=defaults.get(CONF_ADDRESS, "")): str,
    }
    fields.update(_base_fields(defaults))
    return vol.Schema(fields)


def _discovery_schema(defaults: dict[str, Any], device_options: dict[str, str]) -> vol.Schema:
    """Build the discovered-device setup form schema."""
    default_address = defaults.get(CONF_ADDRESS)
    if default_address not in device_options:
        default_address = next(iter(device_options))

    fields: dict[Any, Any] = {
        vol.Required(CONF_ADDRESS, default=default_address): vol.In(device_options),
    }
    fields.update(_base_fields(defaults))
    return vol.Schema(fields)


class PowerpalBleConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Powerpal BLE."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the flow."""
        self._discovered_address: str | None = None
        self._discovered_name: str | None = None
        self._device_options: dict[str, str] | None = None
        self._verified_candidates_found = False

    async def async_step_bluetooth(
        self, discovery_info: BluetoothServiceInfoBleak
    ) -> FlowResult:
        """Handle Bluetooth discovery triggered by the manifest matcher."""
        self._discovered_address = _normalise_address(discovery_info.address)
        self._discovered_name = discovery_info.name or DEFAULT_NAME
        await self.async_set_unique_id(self._discovered_address)
        self._abort_if_unique_id_configured()
        return await self.async_step_manual()

    async def _async_get_device_options(self) -> dict[str, str]:
        """Return discovered Powerpal candidates, or nearby BLE devices as fallback."""
        request_active_scan = getattr(bluetooth, "async_request_active_scan", None)
        if request_active_scan is not None:
            try:
                await request_active_scan(self.hass)
            except Exception as err:  # noqa: BLE001 - keep the flow usable on scan errors.
                _LOGGER.debug("Failed to request active Bluetooth scan: %s", err)

        service_infos = bluetooth.async_discovered_service_info(self.hass, connectable=True)

        best_by_address: dict[str, BluetoothServiceInfoBleak] = {}
        for info in service_infos:
            address = _normalise_address(info.address)
            current = best_by_address.get(address)
            if current is None or _candidate_sort_key(info) < _candidate_sort_key(current):
                best_by_address[address] = info

        all_infos = sorted(best_by_address.values(), key=_candidate_sort_key)
        verified_infos = [info for info in all_infos if _powerpal_match_reason(info)]
        self._verified_candidates_found = bool(verified_infos)

        if verified_infos:
            infos_to_show = verified_infos
        else:
            # If Powerpal does not advertise its custom service/name, we cannot safely
            # prove compatibility from advertisements alone. Show nearby connectable
            # BLE devices as an address picker, but clearly label them unverified.
            infos_to_show = all_infos[:MAX_FALLBACK_DEVICES]

        options = {
            _normalise_address(info.address): _format_device_label(
                info,
                reason=_powerpal_match_reason(info),
            )
            for info in infos_to_show
        }
        options[MANUAL_ADDRESS] = "Manual entry / device not listed"
        return options

    def _defaults(self) -> dict[str, Any]:
        """Return defaults for the setup form."""
        return {
            CONF_NAME: self._discovered_name or DEFAULT_NAME,
            CONF_ADDRESS: self._discovered_address or "",
            CONF_PAIRING_CODE: "",
            CONF_PULSES_PER_KWH: DEFAULT_PULSES_PER_KWH,
            CONF_NOTIFICATION_INTERVAL: DEFAULT_NOTIFICATION_INTERVAL,
            CONF_DROP_STALE_MEASUREMENTS: DEFAULT_DROP_STALE_MEASUREMENTS,
            CONF_STALE_MEASUREMENT_SECONDS: DEFAULT_STALE_MEASUREMENT_SECONDS,
        }

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial user step."""
        errors: dict[str, str] = {}
        defaults = self._defaults()

        if self._device_options is None:
            self._device_options = await self._async_get_device_options()

        if user_input is not None:
            selected_address = str(user_input.get(CONF_ADDRESS, ""))
            if selected_address == MANUAL_ADDRESS:
                return await self.async_step_manual()

            errors = _validate_user_input(user_input)
            if not errors:
                return await self._async_create_entry_from_input(user_input)

            defaults.update(user_input)

        return self.async_show_form(
            step_id="user",
            data_schema=_discovery_schema(defaults, self._device_options),
            errors=errors,
            description_placeholders={
                "scan_type": "Powerpal-compatible" if self._verified_candidates_found else "nearby connectable BLE"
            },
        )

    async def async_step_manual(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle manual address entry."""
        errors: dict[str, str] = {}
        defaults = self._defaults()

        if user_input is not None:
            errors = _validate_user_input(user_input)
            if not errors:
                return await self._async_create_entry_from_input(user_input)

            defaults.update(user_input)

        return self.async_show_form(
            step_id="manual",
            data_schema=_manual_schema(defaults),
            errors=errors,
        )

    async def _async_create_entry_from_input(self, user_input: dict[str, Any]) -> FlowResult:
        """Create a config entry from valid user input."""
        address = _normalise_address(str(user_input[CONF_ADDRESS]))
        await self.async_set_unique_id(address)
        self._abort_if_unique_id_configured()

        data = dict(user_input)
        name = str(data.pop(CONF_NAME, DEFAULT_NAME)).strip() or DEFAULT_NAME
        data[CONF_ADDRESS] = address
        data[CONF_PAIRING_CODE] = str(data[CONF_PAIRING_CODE]).strip()
        return self.async_create_entry(title=name, data=data)

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Return the options flow handler."""
        return PowerpalBleOptionsFlow()


class PowerpalBleOptionsFlow(config_entries.OptionsFlow):
    """Handle Powerpal BLE options."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage options."""
        errors: dict[str, str] = {}
        defaults = {**self.config_entry.data, **self.config_entry.options}
        defaults.setdefault(CONF_NAME, self.config_entry.title)

        if user_input is not None:
            errors = _validate_user_input(user_input)
            if not errors:
                name = str(user_input.pop(CONF_NAME, self.config_entry.title)).strip()
                user_input[CONF_ADDRESS] = _normalise_address(str(user_input[CONF_ADDRESS]))
                user_input[CONF_PAIRING_CODE] = str(user_input[CONF_PAIRING_CODE]).strip()

                # Home Assistant options flows cannot change the entry title directly.
                # Keeping the field here allows the setup form to be reused safely, but
                # the title will remain unchanged unless the entry is removed/re-added.
                return self.async_create_entry(title=name, data=user_input)

            defaults.update(user_input)

        return self.async_show_form(
            step_id="init",
            data_schema=_manual_schema(defaults),
            errors=errors,
        )
