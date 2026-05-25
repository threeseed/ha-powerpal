"""Powerpal BLE sensor entities."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    PERCENTAGE,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
    EntityCategory,
    UnitOfEnergy,
    UnitOfPower,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import CONNECTION_BLUETOOTH
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity

from .const import (
    CONF_ADDRESS,
    DOMAIN,
    SENSOR_BATTERY,
    SENSOR_DAILY_ENERGY,
    SENSOR_POWER,
    SENSOR_TOTAL_ENERGY,
)
from .coordinator import PowerpalData, PowerpalRuntime


@dataclass(frozen=True, kw_only=True)
class PowerpalSensorEntityDescription(SensorEntityDescription):
    """Description of a Powerpal sensor."""

    value_fn: Callable[[PowerpalData], int | float | str | None]


SENSOR_DESCRIPTIONS: tuple[PowerpalSensorEntityDescription, ...] = (
    PowerpalSensorEntityDescription(
        key=SENSOR_POWER,
        translation_key=SENSOR_POWER,
        device_class=SensorDeviceClass.POWER,
        native_unit_of_measurement=UnitOfPower.WATT,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        value_fn=lambda data: data.power_w,
    ),
    PowerpalSensorEntityDescription(
        key=SENSOR_TOTAL_ENERGY,
        translation_key=SENSOR_TOTAL_ENERGY,
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
        suggested_display_precision=3,
        value_fn=lambda data: data.total_energy_kwh,
    ),
    PowerpalSensorEntityDescription(
        key=SENSOR_DAILY_ENERGY,
        translation_key=SENSOR_DAILY_ENERGY,
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL,
        suggested_display_precision=3,
        value_fn=lambda data: data.daily_energy_kwh,
    ),
    PowerpalSensorEntityDescription(
        key=SENSOR_BATTERY,
        translation_key=SENSOR_BATTERY,
        device_class=SensorDeviceClass.BATTERY,
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: data.battery_percent,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up Powerpal BLE sensors."""
    runtime: PowerpalRuntime = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        PowerpalSensor(runtime, entry, description)
        for description in SENSOR_DESCRIPTIONS
    )


class PowerpalSensor(SensorEntity, RestoreEntity):
    """A Powerpal BLE sensor."""

    entity_description: PowerpalSensorEntityDescription
    _attr_has_entity_name = True

    def __init__(
        self,
        runtime: PowerpalRuntime,
        entry: ConfigEntry,
        description: PowerpalSensorEntityDescription,
    ) -> None:
        """Initialize the sensor."""
        self.runtime = runtime
        self.entry = entry
        self.entity_description = description
        self._attr_unique_id = f"{entry.unique_id}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.unique_id or runtime.address)},
            name=entry.title,
            manufacturer="Powerpal",
            model="Powerpal BLE energy monitor",
            connections={(CONNECTION_BLUETOOTH, runtime.address)},
        )

    async def async_added_to_hass(self) -> None:
        """Restore state and subscribe to runtime updates."""
        await super().async_added_to_hass()

        if self.entity_description.key == SENSOR_TOTAL_ENERGY:
            last_state = await self.async_get_last_state()
            if last_state and last_state.state not in (
                STATE_UNKNOWN,
                STATE_UNAVAILABLE,
            ):
                try:
                    self.runtime.restore_total_energy(float(last_state.state))
                except ValueError:
                    pass

        self.async_on_remove(self.runtime.async_add_listener(self._handle_runtime_update))

    @callback
    def _handle_runtime_update(self) -> None:
        """Write state when runtime data changes."""
        self.async_write_ha_state()

    @property
    def native_value(self) -> int | float | str | None:
        """Return the sensor value."""
        return self.entity_description.value_fn(self.runtime.data)

    @property
    def available(self) -> bool:
        """Return whether the entity is available."""
        return self.runtime.available and self.native_value is not None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return diagnostic attributes shared by the sensors."""
        data = self.runtime.data
        attrs: dict[str, Any] = {
            "address": self.runtime.address,
            "notification_interval_minutes": self.runtime.notification_interval,
            "pulses_per_kwh": self.runtime.pulses_per_kwh,
            "dropped_measurements": data.dropped_measurements,
        }
        if data.last_measurement_time is not None:
            attrs["last_measurement_time"] = data.last_measurement_time.isoformat()
        if data.last_pulses is not None:
            attrs["last_pulses"] = data.last_pulses
        if data.error:
            attrs["last_error"] = data.error
        return attrs
