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
# Wide enough for the worst case of bonding plus both pairing-code write attempts.
SETUP_TIMEOUT = 90.0
NOTIFY_TIMEOUT = 20.0

# The device stalls the measurement CCCD write if it is issued straight after a
# batch-size change, so give the link a moment to settle first.
SUBSCRIBE_SETTLE_DELAY = 2.0

# Powerpal rejects reads while it is still digesting the setup writes, so the first
# battery read waits well clear of them and later ones back off instead of hammering.
BATTERY_READ_TIMEOUT = 15.0
BATTERY_FIRST_READ_DELAY = 15.0
BATTERY_RETRY_DELAYS = (30.0, 60.0, 300.0, 900.0)
BATTERY_REFRESH_INTERVAL = 3600.0

# Bail out before the 30s ATT transaction timeout: once that fires the stack is
# obliged to close the ATT bearer, which takes the whole connection down with it.
PAIRING_WRITE_TIMEOUT = 15.0
BOND_TIMEOUT = 20.0


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
        self._battery_task: asyncio.Task[None] | None = None

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
        _LOGGER.debug(
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

        self._cancel_battery_poller()

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

        _LOGGER.debug(
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
        _LOGGER.debug(
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
            self._cancel_battery_poller()
            await self._safe_disconnect(client)
            self._client = None
            self._mark_disconnected(None)

    async def _async_setup_session(self, client: BleakClient) -> None:
        """Authenticate and subscribe on a freshly connected client.

        Each step is logged so a stalled GATT call can be identified; the caller
        bounds the whole sequence with a timeout.
        """
        await self._ensure_services_resolved(client)
        _LOGGER.debug("Powerpal %s: services resolved", self.address)

        batch_size = await self._authenticate(client)
        _LOGGER.debug(
            "Powerpal %s: authenticated (device batch size %s)", self.address, batch_size
        )

        await self._configure_batch_size(client, batch_size)

        await self._subscribe_to_measurements(client)
        _LOGGER.debug("Powerpal %s: subscribed to measurements", self.address)

        # Battery is optional and the device answers reads erratically right after
        # setup, so it runs out of band: it must never delay or abort the session.
        await self._start_battery_notifications(client)
        self._start_battery_poller(client)

    async def _subscribe_to_measurements(self, client: BleakClient) -> None:
        """Enable measurement notifications, bounded and with diagnostics.

        A blocking start_notify usually means one of two things: the peripheral
        already dropped the link (so the CCCD write is never answered), or the
        descriptor needs an encrypted link and the stack is silently waiting on
        bonding. The logging below distinguishes them.
        """
        characteristic = None
        try:
            characteristic = client.services.get_characteristic(MEASUREMENT_UUID)
        except Exception as err:  # noqa: BLE001 - diagnostics only
            _LOGGER.debug("Could not inspect measurement characteristic: %s", err)

        _LOGGER.debug(
            "Powerpal %s: subscribing to measurements (connected=%s, properties=%s)",
            self.address,
            client.is_connected,
            getattr(characteristic, "properties", "unknown"),
        )

        if not client.is_connected:
            raise BleakError(
                f"Powerpal {self.address} dropped the link before the measurement "
                "subscription; the pairing code may have been rejected"
            )

        try:
            await asyncio.wait_for(
                client.start_notify(MEASUREMENT_UUID, self._notification_callback),
                timeout=NOTIFY_TIMEOUT,
            )
        except (asyncio.TimeoutError, TimeoutError) as err:
            raise RuntimeError(
                f"Timed out after {NOTIFY_TIMEOUT:.0f}s subscribing to Powerpal "
                f"{self.address} measurements (connected={client.is_connected}); the "
                "device either dropped the link or requires OS-level bonding"
            ) from err

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

    async def _authenticate(self, client: BleakClient) -> int | None:
        """Write the pairing code and prove the session is authenticated.

        Powerpal gates every characteristic in its service behind the pairing code
        and answers an unauthenticated session with generic ATT errors rather than a
        clean rejection, so the write alone proves nothing. Reading the reading
        batch size back is the confirmation: that read cannot succeed until the code
        has been accepted. Its value is returned so the caller can skip a redundant
        reconfiguration write.

        Bonding must happen before any of this. Powerpal asks for BLE security on
        connect and simply does not answer GATT on an unencrypted link: the write
        gets no ATT response at all, and after 30 seconds the spec obliges the stack
        to close the ATT bearer and drop the connection. The reference firmware
        driver has the same ordering, writing the pairing code only once OS-level
        authentication has completed.
        """
        await self._ensure_bonded(client)

        # Bonding rebuilds the ATT link on BlueZ, which invalidates service discovery
        # and can leave this client attached to a link that no longer exists.
        if not client.is_connected:
            raise BleakError(
                f"Powerpal {self.address} dropped the link while bonding; the bond is "
                "stored now, so the next attempt should connect encrypted"
            )
        await self._ensure_services_resolved(client)

        try:
            return await self._try_authenticate(client, response=True)
        except Exception as err:  # noqa: BLE001 - backend specific error types
            first_error = err
            _LOGGER.debug(
                "Powerpal %s did not answer a write request for pairing code %s: %s",
                self.address,
                self.pairing_code,
                err,
            )

        # Some firmware only accepts the pairing code as a Write Command. That gives
        # no ATT response, so the batch-size read is what actually confirms it.
        if not client.is_connected:
            raise BleakError(
                f"Powerpal {self.address} dropped the link during authentication "
                f"({first_error})"
            )

        try:
            return await self._try_authenticate(client, response=False)
        except Exception as err:  # noqa: BLE001 - backend specific error types
            raise BleakError(
                f"Powerpal {self.address} rejected pairing code {self.pairing_code} "
                f"({err}); check the 6-digit code shown in the Powerpal app, and that "
                "the phone app is not holding the only connection slot"
            ) from err

    async def _ensure_bonded(self, client: BleakClient) -> None:
        """Bond with the device unless the adapter already holds a bond.

        Failure is not fatal: some backends and remote proxies cannot bond at all,
        and the authentication attempt below is a better test than guessing here.
        """
        pair = getattr(client, "pair", None)
        if pair is None:
            _LOGGER.debug(
                "Powerpal %s: backend cannot bond, trying the pairing code unencrypted",
                self.address,
            )
            return

        try:
            try:
                await asyncio.wait_for(pair(), timeout=BOND_TIMEOUT)
            except TypeError:
                # Older Bleak signatures require an explicit protection level.
                await asyncio.wait_for(pair(2), timeout=BOND_TIMEOUT)
            _LOGGER.debug("Powerpal %s: link bonded", self.address)
        except Exception as err:  # noqa: BLE001 - backend specific error types
            if "already" in str(err).lower():
                _LOGGER.debug("Powerpal %s: already bonded", self.address)
                return
            _LOGGER.debug(
                "Powerpal %s: bonding failed (%s); trying the pairing code anyway",
                self.address,
                err,
            )

    async def _try_authenticate(
        self, client: BleakClient, *, response: bool
    ) -> int | None:
        """Write the pairing code and read back a gated characteristic."""
        _LOGGER.debug(
            "Powerpal %s: writing pairing code %s as %s (%s)",
            self.address,
            self.pairing_code,
            "write request" if response else "write command",
            self._pairing_code_bytes().hex(),
        )
        await asyncio.wait_for(
            client.write_gatt_char(
                PAIRING_CODE_UUID, self._pairing_code_bytes(), response=response
            ),
            timeout=PAIRING_WRITE_TIMEOUT,
        )
        return await self._read_batch_size(client)

    async def _read_batch_size(self, client: BleakClient) -> int | None:
        """Return the reading batch size the device is currently configured with."""
        raw = bytes(await client.read_gatt_char(READING_BATCH_SIZE_UUID))
        if len(raw) != 4:
            _LOGGER.debug(
                "Powerpal %s returned an unexpected batch size payload: %s",
                self.address,
                raw.hex(),
            )
            return None
        return int.from_bytes(raw, byteorder="little")

    async def _configure_batch_size(
        self, client: BleakClient, current: int | None
    ) -> None:
        """Set the measurement interval, but only when it actually differs.

        Lowering the batch size makes Powerpal replay every reading it buffered
        under the previous interval, which the stale filter then discards. Skipping
        the write when the device is already configured avoids that burst on every
        reconnect.
        """
        if current == self.notification_interval:
            _LOGGER.debug(
                "Powerpal %s: batch size already %s, leaving it alone",
                self.address,
                current,
            )
            return

        await client.write_gatt_char(
            READING_BATCH_SIZE_UUID,
            int(self.notification_interval).to_bytes(4, byteorder="little"),
            response=True,
        )
        _LOGGER.debug(
            "Powerpal %s: batch size changed from %s to %s",
            self.address,
            current,
            self.notification_interval,
        )
        # The device needs a moment before it will answer the measurement CCCD write.
        await asyncio.sleep(SUBSCRIBE_SETTLE_DELAY)

    def _pairing_code_bytes(self) -> bytes:
        """Return the Powerpal pairing code as a little-endian uint32."""
        return int(self.pairing_code).to_bytes(4, byteorder="little")

    async def _read_battery(self, client: BleakClient) -> bool:
        """Read the battery level once. Return True if a value was decoded."""
        if not client.is_connected:
            return False
        try:
            battery = bytes(
                await asyncio.wait_for(
                    client.read_gatt_char(BATTERY_UUID), timeout=BATTERY_READ_TIMEOUT
                )
            )
        except Exception as err:  # noqa: BLE001 - battery is best effort
            _LOGGER.debug(
                "Could not read Powerpal %s battery level: %s", self.address, err
            )
            return False

        if not self._process_battery(battery):
            _LOGGER.debug(
                "Powerpal %s returned an unusable battery payload: %s",
                self.address,
                battery.hex(),
            )
            return False

        _LOGGER.debug(
            "Powerpal %s battery level: %s%%", self.address, self.data.battery_percent
        )
        return True

    def _start_battery_poller(self, client: BleakClient) -> None:
        """Run the battery read in the background for the life of the connection."""
        self._cancel_battery_poller()
        self._battery_task = asyncio.create_task(
            self._battery_poll_loop(client), name=f"Powerpal BLE battery {self.address}"
        )

    def _cancel_battery_poller(self) -> None:
        """Stop the background battery poller, if one is running."""
        task = self._battery_task
        self._battery_task = None
        if task is not None and not task.done():
            task.cancel()

    async def _battery_poll_loop(self, client: BleakClient) -> None:
        """Keep retrying the battery read, then refresh it periodically.

        Powerpal answers reads with a generic ATT error while it is still busy with
        the setup writes, and it never notifies 0x2A19 on its own, so a one-shot read
        during setup leaves the battery unknown for the whole session. Retrying out
        of band recovers from that and keeps the value fresh on long connections.
        """
        delay = BATTERY_FIRST_READ_DELAY
        failures = 0

        try:
            while True:
                await asyncio.sleep(delay)
                if not client.is_connected:
                    return

                if await self._read_battery(client):
                    failures = 0
                    delay = BATTERY_REFRESH_INTERVAL
                    continue

                delay = BATTERY_RETRY_DELAYS[
                    min(failures, len(BATTERY_RETRY_DELAYS) - 1)
                ]
                failures += 1
                _LOGGER.debug(
                    "Powerpal %s battery read failed (%s in a row); retrying in %.0fs",
                    self.address,
                    failures,
                    delay,
                )
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001 - never let this kill the session
            _LOGGER.debug(
                "Powerpal %s battery poller stopped: %s", self.address, err
            )

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
        _LOGGER.debug("Powerpal %s measurement: %s", self.address, data.hex())

        if len(data) < 6:
            _LOGGER.debug("Ignoring short Powerpal measurement: %s", data.hex())
            return

        unix_time = int.from_bytes(data[0:4], byteorder="little")
        pulses = int.from_bytes(data[4:6], byteorder="little")
        measurement_key = (unix_time, pulses)
        if measurement_key in self._seen_measurements:
            _LOGGER.debug(
                "Ignoring duplicate Powerpal measurement timestamp=%s pulses=%s",
                unix_time,
                pulses,
            )
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
                    "Dropping stale Powerpal measurement timestamp=%s (%.0fs behind "
                    "this host, limit %ss) pulses=%s data=%s",
                    unix_time,
                    now - unix_time,
                    stale_limit,
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
    def _process_battery(self, data: bytes) -> bool:
        """Decode a battery update. Return True if a valid level was stored."""
        if not data:
            return False
        battery = int(data[0])
        if not 0 <= battery <= 100:
            return False
        self._async_set_data(
            PowerpalData(
                **{
                    **self.data.__dict__,
                    "battery_percent": battery,
                }
            )
        )
        return True

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
