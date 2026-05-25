# Powerpal BLE Home Assistant custom integration

This is a first-pass Home Assistant custom integration that connects directly to a Powerpal over Bluetooth LE using Home Assistant's Bluetooth stack. It is intended to replace the ESP32 bridge for users who already have a Home Assistant host with a connectable Bluetooth adapter.

## What it creates

- `sensor.<name>_power` in W
- `sensor.<name>_total_energy` in kWh, restored across Home Assistant restarts
- `sensor.<name>_daily_energy` in kWh
- `sensor.<name>_battery` in percent, when the battery GATT characteristic is readable

## Install

1. Copy `custom_components/powerpal_ble` into your Home Assistant `config/custom_components/` directory.
2. Restart Home Assistant.
3. Go to **Settings → Devices & services → Add integration**.
4. Search for **Powerpal BLE**.
5. Enter:
   - Powerpal BLE MAC address, e.g. `AA:BB:CC:DD:EE:FF`
   - Powerpal pairing code, e.g. `123123`
   - Your electricity meter pulse rate, commonly `1000` pulses/kWh
   - Notification interval in minutes, usually `1`

Turn off the Powerpal mobile app / phone Bluetooth connection while testing, because only one central connection may be available.

## Notes

- This integration is local BLE push. It does not use MQTT and does not use the Powerpal cloud API.
- It uses the documented Powerpal service/characteristics from the community Powerpal BLE work.
- Stale/catch-up measurements are dropped by default so Home Assistant does not attribute old readings to the current time. You can change this in the integration options.
- This is experimental. Expect BLE connection tuning to be needed on some Linux/BlueZ systems.

## Debug logging

Add this to `configuration.yaml` if you need detailed logs:

```yaml
logger:
  logs:
    custom_components.powerpal_ble: debug
```
