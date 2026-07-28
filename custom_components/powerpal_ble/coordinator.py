"""Runtime BLE connection manager for Powerpal BLE."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
import logging
import time
from typing import Any

from bleak import BleakClient, BleakError

try:
    from bleak_retry_connector import establish_connection
except ImportError:  # pragma: no cover - Home Assistant normally provides this
    establish_connection = None  # type: ignore[assignment]

from homeassistant.components import bluetooth
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.util import dt as dt_util

from .const import (
    BATTERY_UUID,
    CONF_ADDRESS,
    CONF_DROP_STALE_MEASUREMENTS,
    CONF_NOTIFICATION_INTERVAL,
    CONF_PAIRING_CODE,
    CONF_PULSES_PER_KWH,
    CONF_STALE_MEASUREMENT_SECONDS,
    DEFAULT_DROP_STALE_MEASUREMENTS,
    DEFAULT_NOTIFICATION_INTERVAL,
    DEFAULT_PULSES_PER_KWH,
    DEFAULT_STALE_MEASUREMENT_SECONDS,
    MEASUREMENT_UUID,
    PAIRING_CODE_UUID,
    READING_BATCH_SIZE_UUID,
)

_LOGGER = logging.getLogger(__name__)

Listener = Callable[[], None]

SERVICE_DISCOVERY_TIMEOUT = 20.0
SERVICE_DISCOVERY_POLL = 0.5

# BLE calls can block forever, which would wedge the reconnect loop silently.
CONNECT_TIMEOUT = 120.0
SETUP_TIMEOUT = 60.0

# Errors that mean the peripheral wants OS-level bonding before it will accept a write.
AUTH_ERROR_MARKERS = (
    "insufficient authentication",
    "insufficient encryption",
    "not authorized",
    "notauthorized",
    "not permitted",
    "notpermitted",
    "not paired",
    "authentication",
)


def _is_auth_error(err: Exception) -> bool:
    """Return True if a GATT error indicates the device wants OS-level bonding."""
    message = str(err).lower()
    return any(marker in message for marker in AUTH_ERROR_MARKERS)


@dataclass(frozen=True)
class PowerpalData:
    """Latest Powerpal sensor data."""

    power_w: float | None = None
    total_energy_kwh: float | None = None
    daily_energy_kwh: float | None = None
    battery_percent: int | None = None
    last_measurement_time: datetime | None = None
    last_pulses: int | None = None
    connected: bool = False
    dropped_measurements: int = 0
    error: str | None = None


class PowerpalRuntime:
    """Keep a BLE connection open and publish decoded Powerpal measurements."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        """Initialize the runtime."""
        self.hass = hass
        self.entry = entry
        self.name = entry.title

        config: dict[str, Any] = {**entry.data, **entry.options}
        self.address: str = str(config[CONF_ADDRESS]).upper()
        self.pairing_code: str = str(config[CONF_PAIRING_CODE])
        self.pulses_per_kwh: float = float(
            config.get(CONF_PULSES_PER_KWH, DEFAULT_PULSES_PER_KWH)
        )
        self.notification_interval: int = int(
            config.get(CONF_NOTIFICATION_INTERVAL, DEFAULT_NOTIFICATION_INTERVAL)
        )
        self.drop_stale_measurements: bool = bool(
            config.get(
                CONF_DROP_STALE_MEASUREMENTS, DEFAULT_DROP_STALE_MEASUREMENTS
            )
        )
        self.stale_measurement_seconds: int = int(
            config.get(CONF_STALE_MEASUREMENT_SECONDS, DEFAULT_STALE_MEASUREMENT_SECONDS)
        )

        self.data = PowerpalData()
        self._listeners: list[Listener] = []
        self._task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None
        self._disconnect_event: asyncio.Event | None = None
        self._client: BleakClient | None = None

        self._base_energy_kwh: float = 0.0
        self._total_pulses: int = 0
        self._daily_pulses: int = 0
        self._daily_key: str | None = None
        self._seen_measurements: set[tuple[int, int]] = set()
        self._seen_measurements_order: deque[tuple[int, int]] = deque(maxlen=96)

    @property
    def available(self) -> bool:
        """Return whether the BLE client is connected."""
        return self.data.connected

    def async_add_listener(self, listener: Listener) -> CALLBACK_TYPE:
        """Subscribe to runtime data changes."""
        self._listeners.append(listener)

        @callback
        def remove_listener() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)

        return remove_listener

    @callback
    def _async_set_data(self, data: PowerpalData) -> None:
        """Store data and notify listeners."""
        self.data = data
        for listener in list(self._listeners):
            listener()

    def restore_total_energy(self, value: float) -> None:
        """Set the restored total energy value before new pulses arrive."""
        if self._total_pulses == 0:
            self._base_energy_kwh = max(value, 0.0)
            self._async_set_data(
                PowerpalData(
                    **{
                        **self.data.__dict__,
                        "total_energy_kwh": self._base_energy_kwh,
                    }
                )
            )

    async def async_start(self) -> None:
        """Start the background BLE connection task."""
        if self._task is not None:
            return

        try:
            scanner_count = bluetooth.async_scanner_count(self.hass, connectable=True)
        except TypeError:
            scanner_count = bluetooth.async_scanner_count(self.hass)
        if scanner_count < 1:
            raise ConfigEntryNotReady("No connectable Bluetooth adapters are available")

        self._stop_event = asyncio.Event()
        self._task = self._async_create_connection_task()
        _LOGGER.warning(
            "Started Powerpal BLE connection loop for %s (%s connectable scanner(s))",
            self.address,
            scanner_count,
        )

    def _async_create_connection_task(self) -> asyncio.Task[None]:
        """Create the connection loop as a background task.

        The loop runs until the entry is unloaded, so it must not be a tracked task:
        bootstrap waits on those before finishing startup and would time out on it.
        """
        coro = self._connection_loop()
        name = f"Powerpal BLE {self.address}"

        entry_background_task = getattr(
            self.entry, "async_create_background_task", None
        )
        if entry_background_task is not None:
            return entry_background_task(self.hass, coro, name)

        hass_background_task = getattr(
            self.hass, "async_create_background_task", None
        )
        if hass_background_task is not None:
            return hass_background_task(coro, name)

        return self.hass.async_create_task(coro, name)

    async def async_stop(self) -> None:
        """Stop the background BLE connection task."""
        if self._stop_event is not None:
            self._stop_event.set()

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        client = self._client
        if client is not None and client.is_connected:
            try:
                await client.disconnect()
            except (BleakError, TimeoutError, asyncio.TimeoutError):
                pass
        self._client = None

    async def _connection_loop(self) -> None:
        """Reconnect forever until unloaded."""
        assert self._stop_event is not None
        retry_delay = 5

        while not self._stop_event.is_set():
            try:
                await self._connect_and_listen()
                retry_delay = 5
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - keep the bridge alive
                _LOGGER.warning("Powerpal BLE connection failed: %s", err)
                self._mark_disconnected(str(err))
                await self._sleep_or_stop(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    async def _sleep_or_stop(self, seconds: float) -> None:
        """Sleep unless unloading."""
        assert self._stop_event is not None
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return

    async def _wait_for_disconnect_or_stop(self) -> None:
        """Wait until the device disconnects or the config entry is unloaded."""
        assert self._stop_event is not None
        assert self._disconnect_event is not None
        stop_task = asyncio.create_task(self._stop_event.wait())
        disconnect_task = asyncio.create_task(self._disconnect_event.wait())
        try:
            await asyncio.wait(
                {stop_task, disconnect_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            stop_task.cancel()
            disconnect_task.cancel()

    async def _connect_and_listen(self) -> None:
        """Connect, authenticate, subscribe, and wait for disconnect."""
        assert self._stop_event is not None
        self._disconnect_event = asyncio.Event()

        ble_device = bluetooth.async_ble_device_from_address(
            self.hass, self.address, connectable=True
        )
        if ble_device is None:
            raise RuntimeError(self._unreachable_message())

        _LOGGER.warning(
            "Opening BLE connection to Powerpal %s (%s)",
            self.address,
            getattr(ble_device, "name", None) or "unnamed",
        )
        try:
            client = await asyncio.wait_for(
                self._establish_client(ble_device), timeout=CONNECT_TIMEOUT
            )
        except (asyncio.TimeoutError, TimeoutError) as err:
            raise RuntimeError(
                f"Timed out after {CONNECT_TIMEOUT:.0f}s opening a BLE connection to "
                f"Powerpal {self.address}"
            ) from err

        self._client = client
        self._mark_connected()
        _LOGGER.warning(
            "BLE link established to Powerpal %s; starting GATT setup", self.address
        )

        try:
            try:
                await asyncio.wait_for(
                    self._async_setup_session(client), timeout=SETUP_TIMEOUT
                )
            except (asyncio.TimeoutError, TimeoutError) as err:
                raise RuntimeError(
                    f"Timed out after {SETUP_TIMEOUT:.0f}s during GATT setup for "
                    f"Powerpal {self.address}"
                ) from err

            _LOGGER.info(
                "Connected to Powerpal %s; waiting for %s-minute measurements",
                self.address,
                self.notification_interval,
            )

            await self._wait_for_disconnect_or_stop()
        finally:
            await self._safe_disconnect(client)
            self._client = None
            self._mark_disconnected(None)

    async def _async_setup_session(self, client: BleakClient) -> None:
        """Authenticate and subscribe on a freshly connected client.

        Each step is logged so a stalled GATT call can be identified; the caller
        bounds the whole sequence with a timeout.
        """
        await self._ensure_services_resolved(client)
        _LOGGER.warning("Powerpal %s: services resolved", self.address)

        await self._write_pairing_code(client)
        _LOGGER.warning("Powerpal %s: pairing code written", self.address)

        await asyncio.sleep(0.5)
        await client.write_gatt_char(
            READING_BATCH_SIZE_UUID,
            int(self.notification_interval).to_bytes(4, byteorder="little"),
            response=False,
        )
        _LOGGER.warning("Powerpal %s: reading batch size written", self.address)

        await self._read_battery(client)
        await self._start_battery_notifications(client)
        _LOGGER.warning("Powerpal %s: battery handled", self.address)

        await client.start_notify(MEASUREMENT_UUID, self._notification_callback)
        _LOGGER.warning("Powerpal %s: subscribed to measurements", self.address)

    def _unreachable_message(self) -> str:
        """Explain why no connectable BLEDevice is available for this address.

        Being advertised is not enough: a passive-only scanner can see the device
        without any adapter being able to open a connection to it.
        """
        last_service_info = getattr(bluetooth, "async_last_service_info", None)
        if last_service_info is None:
            return f"Powerpal {self.address} is not visible to a connectable Bluetooth adapter"

        try:
            passive = last_service_info(self.hass, self.address, connectable=False)
        except Exception:  # noqa: BLE001 - diagnostics must never break the loop
            passive = None

        if passive is not None:
            return (
                f"Powerpal {self.address} is advertising (RSSI "
                f"{getattr(passive, 'rssi', 'unknown')}, via "
                f"{getattr(passive, 'source', 'unknown')}) but no connectable adapter "
                "or proxy can reach it; the scanner seeing it is passive-only or out of range"
            )

        return (
            f"Powerpal {self.address} has not been seen by any Bluetooth scanner; "
            "check the configured MAC address"
        )

    async def _establish_client(self, ble_device: Any) -> BleakClient:
        """Open a Bleak connection using HA's BLEDevice object."""
        if establish_connection is not None:
            try:
                return await establish_connection(  # type: ignore[misc]
                    BleakClient,
                    ble_device,
                    self.name,
                    self._disconnected_callback,
                    max_attempts=3,
                    timeout=30,
                )
            except TypeError:
                return await establish_connection(  # type: ignore[misc]
                    BleakClient,
                    ble_device,
                    self.name,
                    self._disconnected_callback,
                )

        client = BleakClient(
            ble_device,
            disconnected_callback=self._disconnected_callback,
            timeout=30.0,
        )
        await client.connect()
        return client

    async def _ensure_services_resolved(self, client: BleakClient) -> None:
        """Wait until GATT service discovery has completed on this connection.

        Bleak raises "Service Discovery has not been performed yet" from any GATT
        call made before services are resolved, which happens on BlueZ whenever the
        link is re-established underneath us (for example by a pairing attempt).
        """
        deadline = time.monotonic() + SERVICE_DISCOVERY_TIMEOUT
        last_error: Exception | None = None

        while True:
            if not client.is_connected:
                raise BleakError(
                    f"Powerpal {self.address} disconnected before service discovery finished"
                )

            try:
                if client.services:
                    return
            except BleakError as err:
                # Not resolved yet; fall through to an explicit discovery attempt.
                last_error = err

            get_services = getattr(client, "get_services", None)
            if get_services is not None:
                try:
                    if await get_services():
                        return
                except Exception as err:  # noqa: BLE001 - bleak version dependent
                    last_error = err

            if time.monotonic() >= deadline:
                raise BleakError(
                    f"Service discovery did not complete for Powerpal {self.address}: {last_error}"
                )
            await asyncio.sleep(SERVICE_DISCOVERY_POLL)

    async def _write_pairing_code(self, client: BleakClient) -> None:
        """Send the Powerpal pairing code, bonding first only if the device demands it.

        Powerpal authenticates at the application level, so OS-level bonding is not
        normally needed. Pairing eagerly is harmful on BlueZ because it drops and
        rebuilds the ATT link, invalidating service discovery.
        """
        try:
            await client.write_gatt_char(
                PAIRING_CODE_UUID, self._pairing_code_bytes(), response=False
            )
            return
        except Exception as err:  # noqa: BLE001 - backend specific error types
            if not _is_auth_error(err):
                raise
            _LOGGER.debug(
                "Powerpal %s requires bonding before the pairing code write: %s",
                self.address,
                err,
            )

        if not await self._async_pair(client):
            raise BleakError(
                f"Powerpal {self.address} requires bonding but pairing is unsupported here"
            )

        # Pairing commonly resets the link, so re-check discovery before writing again.
        await self._ensure_services_resolved(client)
        await client.write_gatt_char(
            PAIRING_CODE_UUID, self._pairing_code_bytes(), response=False
        )

    async def _async_pair(self, client: BleakClient) -> bool:
        """Attempt OS-level BLE bonding. Return True if it appears to have worked."""
        pair = getattr(client, "pair", None)
        if pair is None:
            return False

        _LOGGER.info("Attempting OS-level pairing with Powerpal %s", self.address)
        try:
            await pair()
            return True
        except TypeError:
            try:
                await pair(2)
                return True
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("Bleak pair(2) failed: %s", err)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Bleak pairing failed or is unsupported: %s", err)
        return False

    def _pairing_code_bytes(self) -> bytes:
        """Return the Powerpal pairing code as a little-endian uint32."""
        return int(self.pairing_code).to_bytes(4, byteorder="little")

    async def _read_battery(self, client: BleakClient) -> None:
        """Read battery level if available."""
        try:
            battery = bytes(await client.read_gatt_char(BATTERY_UUID))
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Could not read Powerpal battery level: %s", err)
            return
        self._process_battery(battery)

    async def _start_battery_notifications(self, client: BleakClient) -> None:
        """Subscribe to battery notifications if available."""
        try:
            await client.start_notify(BATTERY_UUID, self._battery_callback)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Could not subscribe to Powerpal battery notifications: %s", err)

    async def _safe_disconnect(self, client: BleakClient) -> None:
        """Stop notifications and disconnect safely.

        disconnect() is always called, even when Bleak already believes the client
        is gone: the Bluetooth manager only releases the adapter/proxy connection
        slot when the client disconnects, so skipping it leaks a slot every time a
        link drops mid-setup.
        """
        if client.is_connected:
            for uuid in (MEASUREMENT_UUID, BATTERY_UUID):
                try:
                    await client.stop_notify(uuid)
                except Exception:  # noqa: BLE001
                    pass

        try:
            await client.disconnect()
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Powerpal %s disconnect failed: %s", self.address, err)

    def _notification_callback(self, _sender: Any, data: bytearray) -> None:
        """Handle measurement notifications from Bleak."""
        self.hass.loop.call_soon_threadsafe(self._process_measurement, bytes(data))

    def _battery_callback(self, _sender: Any, data: bytearray) -> None:
        """Handle battery notifications from Bleak."""
        self.hass.loop.call_soon_threadsafe(self._process_battery, bytes(data))

    def _disconnected_callback(self, _client: BleakClient) -> None:
        """Handle a Bleak disconnect callback, possibly from another thread."""
        self.hass.loop.call_soon_threadsafe(self._handle_ble_disconnected)

    @callback
    def _handle_ble_disconnected(self) -> None:
        """Mark the client disconnected from the HA event loop."""
        if self._disconnect_event is not None:
            self._disconnect_event.set()
        self._mark_disconnected(None)

    @callback
    def _process_measurement(self, data: bytes) -> None:
        """Decode a Powerpal measurement notification."""
        if len(data) < 6:
            _LOGGER.debug("Ignoring short Powerpal measurement: %s", data.hex())
            return

        unix_time = int.from_bytes(data[0:4], byteorder="little")
        pulses = int.from_bytes(data[4:6], byteorder="little")
        measurement_key = (unix_time, pulses)
        if measurement_key in self._seen_measurements:
            return
        self._remember_measurement(measurement_key)

        now = time.time()
        if self.drop_stale_measurements:
            stale_limit = max(
                self.stale_measurement_seconds,
                self.notification_interval * 60 * 2,
            )
            if unix_time < now - stale_limit or unix_time > now + 60:
                _LOGGER.debug(
                    "Dropping stale Powerpal measurement timestamp=%s pulses=%s data=%s",
                    unix_time,
                    pulses,
                    data.hex(),
                )
                self._async_set_data(
                    PowerpalData(
                        **{
                            **self.data.__dict__,
                            "dropped_measurements": self.data.dropped_measurements + 1,
                        }
                    )
                )
                return

        energy_delta_kwh = pulses / self.pulses_per_kwh
        power_w = energy_delta_kwh / (self.notification_interval / 60.0) * 1000.0

        self._total_pulses += pulses
        total_energy_kwh = self._base_energy_kwh + (
            self._total_pulses / self.pulses_per_kwh
        )

        measurement_time = dt_util.as_local(dt_util.utc_from_timestamp(unix_time))
        day_key = measurement_time.date().isoformat()
        if self._daily_key is None:
            self._daily_key = day_key
        elif day_key != self._daily_key:
            self._daily_key = day_key
            self._daily_pulses = 0

        self._daily_pulses += pulses
        daily_energy_kwh = self._daily_pulses / self.pulses_per_kwh

        self._async_set_data(
            PowerpalData(
                power_w=round(power_w, 3),
                total_energy_kwh=round(total_energy_kwh, 6),
                daily_energy_kwh=round(daily_energy_kwh, 6),
                battery_percent=self.data.battery_percent,
                last_measurement_time=measurement_time,
                last_pulses=pulses,
                connected=self.data.connected,
                dropped_measurements=self.data.dropped_measurements,
                error=None,
            )
        )

    def _remember_measurement(self, measurement_key: tuple[int, int]) -> None:
        """Remember a measurement to avoid double counting duplicates."""
        if len(self._seen_measurements_order) == self._seen_measurements_order.maxlen:
            old_key = self._seen_measurements_order.popleft()
            self._seen_measurements.discard(old_key)
        self._seen_measurements_order.append(measurement_key)
        self._seen_measurements.add(measurement_key)

    @callback
    def _process_battery(self, data: bytes) -> None:
        """Decode a battery update."""
        if not data:
            return
        battery = int(data[0])
        if not 0 <= battery <= 100:
            return
        self._async_set_data(
            PowerpalData(
                **{
                    **self.data.__dict__,
                    "battery_percent": battery,
                }
            )
        )

    @callback
    def _mark_connected(self) -> None:
        """Mark BLE connected."""
        self._async_set_data(
            PowerpalData(
                **{
                    **self.data.__dict__,
                    "connected": True,
                    "error": None,
                }
            )
        )

    @callback
    def _mark_disconnected(self, error: str | None) -> None:
        """Mark BLE disconnected."""
        self._async_set_data(
            PowerpalData(
                **{
                    **self.data.__dict__,
                    "connected": False,
                    "error": error,
                }
            )
        )
