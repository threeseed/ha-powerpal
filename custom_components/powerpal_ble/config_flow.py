"""Config flow for Powerpal BLE."""

from __future__ import annotations

import re
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
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
)

MAC_RE = re.compile(r"^[0-9a-fA-F]{2}(:[0-9a-fA-F]{2}){5}$")
PAIRING_RE = re.compile(r"^[0-9]{1,6}$")


def _normalise_address(address: str) -> str:
    """Normalize a Bluetooth MAC address."""
    return address.strip().upper()


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


def _schema(defaults: dict[str, Any]) -> vol.Schema:
    """Build the user/options form schema."""
    return vol.Schema(
        {
            vol.Required(CONF_NAME, default=defaults.get(CONF_NAME, DEFAULT_NAME)): str,
            vol.Required(CONF_ADDRESS, default=defaults.get(CONF_ADDRESS, "")): str,
            vol.Required(CONF_PAIRING_CODE, default=defaults.get(CONF_PAIRING_CODE, "")): str,
            vol.Required(
                CONF_PULSES_PER_KWH,
                default=defaults.get(CONF_PULSES_PER_KWH, DEFAULT_PULSES_PER_KWH),
            ): vol.All(vol.Coerce(float), vol.Range(min=1)),
            vol.Required(
                CONF_NOTIFICATION_INTERVAL,
                default=defaults.get(
                    CONF_NOTIFICATION_INTERVAL, DEFAULT_NOTIFICATION_INTERVAL
                ),
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
    )


class PowerpalBleConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Powerpal BLE."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the flow."""
        self._discovered_address: str | None = None
        self._discovered_name: str | None = None

    async def async_step_bluetooth(
        self, discovery_info: BluetoothServiceInfoBleak
    ) -> FlowResult:
        """Handle Bluetooth discovery."""
        self._discovered_address = _normalise_address(discovery_info.address)
        self._discovered_name = discovery_info.name or DEFAULT_NAME
        await self.async_set_unique_id(self._discovered_address)
        self._abort_if_unique_id_configured()
        return await self.async_step_user()

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial user step."""
        errors: dict[str, str] = {}

        defaults = {
            CONF_NAME: self._discovered_name or DEFAULT_NAME,
            CONF_ADDRESS: self._discovered_address or "",
            CONF_PAIRING_CODE: "",
            CONF_PULSES_PER_KWH: DEFAULT_PULSES_PER_KWH,
            CONF_NOTIFICATION_INTERVAL: DEFAULT_NOTIFICATION_INTERVAL,
            CONF_DROP_STALE_MEASUREMENTS: DEFAULT_DROP_STALE_MEASUREMENTS,
            CONF_STALE_MEASUREMENT_SECONDS: DEFAULT_STALE_MEASUREMENT_SECONDS,
        }

        if user_input is not None:
            errors = _validate_user_input(user_input)
            if not errors:
                address = _normalise_address(str(user_input[CONF_ADDRESS]))
                await self.async_set_unique_id(address)
                self._abort_if_unique_id_configured()

                name = str(user_input.pop(CONF_NAME, DEFAULT_NAME)).strip() or DEFAULT_NAME
                user_input[CONF_ADDRESS] = address
                user_input[CONF_PAIRING_CODE] = str(user_input[CONF_PAIRING_CODE]).strip()

                return self.async_create_entry(title=name, data=user_input)

            defaults.update(user_input)

        return self.async_show_form(
            step_id="user",
            data_schema=_schema(defaults),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Return the options flow handler."""
        return PowerpalBleOptionsFlow(config_entry)


class PowerpalBleOptionsFlow(config_entries.OptionsFlow):
    """Handle Powerpal BLE options."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self.config_entry = config_entry

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
            data_schema=_schema(defaults),
            errors=errors,
        )
