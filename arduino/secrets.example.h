/*
 * secrets.example.h - WiFi and Azure IoT Hub credentials template
 *
 * Copy to secrets.h for local development.
 * Do not commit secrets.h.
 */

#ifndef SECRETS_H
#define SECRETS_H

const char WIFI_SSID[] = "YOUR_WIFI_SSID";
const char WIFI_PASSWORD[] = "YOUR_WIFI_PASSWORD";

const char IOT_HUB_HOST[] = "YOUR_IOTHUB_NAME.azure-devices.net";
const int IOT_HUB_PORT = 8883;
const char DEVICE_ID[] = "iotdev-0000";
const char MACHINE_ID[] = "MC-0000";

const char SAS_TOKEN[] = "SharedAccessSignature sr=...&sig=...&se=...";

#endif
