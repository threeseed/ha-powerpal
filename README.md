# Powerpal BLE Home Assistant custom integration

Experimental Home Assistant custom integration for connecting directly to a Powerpal over Bluetooth LE using Home Assistant's Bluetooth stack.

## What's new in v0.2.0

- Adds Bluetooth discovery by Powerpal service UUID and likely local-name patterns.
- Adds a setup flow that scans for compatible BLE devices and lets you select a discovered address instead of typing the MAC manually.
- Still allows manual MAC-address entry if the Powerpal is not seen during the scan.

## HACS repository layout

This repository must keep this structure at the repository root:

```text
custom_components/powerpal_ble/manifest.json
custom_components/powerpal_ble/__init__.py
custom_components/powerpal_ble/config_flow.py
custom_components/powerpal_ble/coordinator.py
custom_components/powerpal_ble/sensor.py
custom_components/powerpal_ble/strings.json
custom_components/powerpal_ble/translations/en.json
hacs.json
README.md
```

## Install via HACS custom repository

1. In HACS, open the three-dot menu and choose **Custom repositories**.
2. Paste the GitHub repository URL.
3. Select category **Integration**.
4. Add it, then open the repository in HACS and choose **Download**.
5. Restart Home Assistant.
6. Go to **Settings → Devices & services → Add integration** and search for **Powerpal BLE**.

## Manual install

Copy `custom_components/powerpal_ble` into your Home Assistant config directory:

```text
/config/custom_components/powerpal_ble
```

Then restart Home Assistant and add the integration through **Settings → Devices & services**.

## Notes

- Turn off the Powerpal mobile app / phone Bluetooth connection while testing, because only one central connection may be available.
- This integration is local BLE push. It does not use MQTT and does not use the Powerpal cloud API.
- Discovery depends on what the Powerpal advertises. If it does not show up in the selector, choose manual entry and use the address printed on the device sticker or found with a BLE scanner.
- This is experimental. Expect BLE connection tuning to be needed on some Linux/BlueZ systems.

## Debug logging

Add this to `configuration.yaml` if you need detailed logs:

```yaml
logger:
  logs:
    custom_components.powerpal_ble: debug
    homeassistant.components.bluetooth: debug
```

## Bluetooth discovery

When you add the integration from **Settings → Devices & services → Add integration**, it now asks Home Assistant for a short active Bluetooth scan and shows detected devices. Devices that advertise the Powerpal service UUID are listed first. If no verified Powerpal advertisement is seen, the form shows nearby connectable BLE devices as an unverified fallback so you can choose the likely address without using the terminal.

Powerpal may not always advertise enough information to prove compatibility from advertisements alone. If the correct device is not listed, choose **Manual entry / device not listed** and enter the Bluetooth MAC address manually.

