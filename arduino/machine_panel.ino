/*
 * ======================================================================
 * Machine Panel Simulator - Arduino Uno WiFi Rev2
 * ======================================================================
 *
 * PURPOSE
 * -------
 * Simulates an industrial machine's sensor panel for a Databricks IoT
 * demo (Zerobus ingest -> DLT -> ML -> AI/BI dashboard).  Publishes
 * telemetry as JSON over MQTT/TLS directly to Azure IoT Hub.  Also
 * emits CSV on Serial for a local Python fallback bridge.
 *
 * WIRING DIAGRAM
 * --------------
 *   Potentiometers (10 k-ohm linear taper, connect VCC-GND-Wiper):
 *     A0  POT_VIB   - Vibration       0-10 mm/s
 *     A1  POT_TEMP  - Temperature     30-100 C
 *     A2  POT_TPUT  - Throughput      0-120 CPM / RPM 0-3000 /
 *                                     Current 0-~13 A (derived)
 *
 *   Buttons (normally-open momentary, one leg to pin, other to GND):
 *     D2  BTN_RUN   - Toggle RUN <-> STOPPED
 *     D3  BTN_FAULT - Inject / clear manual FAULT
 *
 *   LEDs (anode to pin through 220-ohm resistor, cathode to GND):
 *     D10  LED_RUN   - Lit when machine state is RUN
 *     D11  LED_FAULT - Lit when machine state is FAULT
 *
 *   Optional (recommended additions -- wired but not coded by default):
 *     D9   RGB LED (PWM) - green=RUN, yellow=WARNING, red=FAULT
 *     D8   Piezo buzzer  - beep on state change, continuous on FAULT
 *     D4   BTN_ESTOP     - Emergency-stop button (E_STOP fault code)
 *
 * SENSOR FORMULAS
 * ---------------
 *   vibration_mm_s   = raw * 10.0 / 1023.0             (0.0 - 10.0)
 *   temp_c           = 30.0 + raw * 70.0 / 1023.0      (30.0 - 100.0)
 *   throughput_cpm   = map(raw, 0, 1023, 0, 120)        (0 - 120)
 *   rpm              = map(raw, 0, 1023, 0, 3000)        (0 - 3000)
 *   current_amps     = (raw * 10.0/1023.0) * thermalFactor   (0 - ~13)
 *   humidity_pct     = 30.0 + raw * 50.0 / 1023.0       (30.0 - 80.0)
 *
 * STATE MACHINE
 * -------------
 *   States: RUN, STOPPED, FAULT
 *
 *   BTN_RUN (D2):
 *     RUN -> STOPPED     (press once)
 *     STOPPED -> RUN     (press again)
 *     FAULT -> no effect (must clear via BTN_FAULT first)
 *
 *   BTN_FAULT (D3):
 *     RUN/STOPPED -> FAULT  (injects manual fault, code MANUAL_FAULT)
 *     FAULT -> previous     (clears fault, returns to RUN or STOPPED)
 *
 *   Threshold auto-fault (only when currentState == RUN):
 *     temp_c >= 85.0         -> emitted FAULT, code OVERTEMP
 *     vibration >= 9.5       -> emitted FAULT, code VIBRATION
 *     current_amps >= 12.0   -> emitted FAULT, code OVERCURRENT
 *     vibration >= 9.5 && rpm > 2000  -> BEARING_WEAR
 *
 * DEMO USE CASE
 * -------------
 *   1) Power on Arduino -> connects WiFi hotspot -> publishes RUN data.
 *   2) Slowly dial POT_TEMP or POT_VIB above threshold to show
 *      progressive degradation leading to auto-FAULT in the dashboard.
 *   3) Press BTN_FAULT for manual operator fault injection.
 *   4) Press BTN_FAULT again to clear and return to normal.
 *   5) Press BTN_RUN to stop/start the machine.
 *
 * FAULT CODES
 * -----------
 *   OVERTEMP      - Temperature above 85 C
 *   VIBRATION     - Vibration above 9.5 mm/s
 *   OVERCURRENT   - Motor current above 12 A
 *   BEARING_WEAR  - High vibration + high RPM combination
 *   MANUAL_FAULT  - Operator-injected via BTN_FAULT button
 * ======================================================================
 */

#include <SPI.h>
#include <WiFiNINA.h>
#include <PubSubClient.h>
#include "secrets.h"

enum MachineState { RUN, STOPPED, FAULT };

// ----- Pin Assignments -----
const int POT_VIB  = A0;   // Vibration potentiometer wiper
const int POT_TEMP = A1;   // Temperature potentiometer wiper
const int POT_TPUT = A2;   // Throughput / RPM potentiometer wiper

const int BTN_RUN   = 2;   // Run/Stop toggle (INPUT_PULLUP, press to GND)
const int BTN_FAULT = 3;   // Fault inject/clear (INPUT_PULLUP, press to GND)

const int LED_RUN   = 10;  // Green LED: lit during RUN
const int LED_FAULT = 11;  // Red LED:   lit during FAULT

// ----- Timing & Thresholds -----
const unsigned long SAMPLE_INTERVAL_MS = 1000;
const unsigned long DEBOUNCE_MS = 40;
const float TEMP_FAULT_THRESHOLD_C = 85.0;
const float VIBRATION_FAULT_THRESHOLD_MM_S = 9.5;
const float CURRENT_FAULT_THRESHOLD_A = 12.0;

// Credentials loaded from secrets.h (gitignored).
// See secrets.h for WIFI_SSID, WIFI_PASSWORD, IOT_HUB_HOST,
// IOT_HUB_PORT, DEVICE_ID, MACHINE_ID, SAS_TOKEN.
char mqttTopic[96];
char mqttUsername[192];

WiFiSSLClient sslClient;
PubSubClient mqttClient(sslClient);

MachineState currentState = RUN;
MachineState previousNonFaultState = RUN;

unsigned long lastEmitMs = 0;
unsigned long lastRunEdgeMs = 0;
unsigned long lastFaultEdgeMs = 0;

int lastRunButtonReading = HIGH;
int lastFaultButtonReading = HIGH;

struct TelemetrySample {
  float vibration;
  float tempC;
  int throughput;
  int rpm;
  float currentAmps;
  float humidityPct;
  const char* state;
  const char* faultCode;
};

void setup() {
  Serial.begin(115200);
  while (!Serial) {
    ;  // wait for serial monitor attach
  }

  pinMode(BTN_RUN, INPUT_PULLUP);
  pinMode(BTN_FAULT, INPUT_PULLUP);
  pinMode(LED_RUN, OUTPUT);
  pinMode(LED_FAULT, OUTPUT);

  if (WiFi.status() == WL_NO_MODULE) {
    Serial.println("WiFiNINA module not found. Running serial-only fallback mode.");
  } else {
    snprintf(
      mqttTopic,
      sizeof(mqttTopic),
      "devices/%s/messages/events/",
      DEVICE_ID
    );

    snprintf(
      mqttUsername,
      sizeof(mqttUsername),
      "%s/%s/?api-version=2021-04-12",
      IOT_HUB_HOST,
      DEVICE_ID
    );

    mqttClient.setServer(IOT_HUB_HOST, IOT_HUB_PORT);
    // Azure IoT Hub SAS auth requires a larger CONNECT packet than PubSubClient default.
    mqttClient.setBufferSize(1024);
    mqttClient.setSocketTimeout(15);

    Serial.print("MQTT username length: ");
    Serial.println(strlen(mqttUsername));
    Serial.print("SAS token length: ");
    Serial.println(strlen(SAS_TOKEN));

    connectWifi();
    connectMqtt();
  }
}

float mapVibrationMmS(int rawVib) {
  return (rawVib * 10.0) / 1023.0;
}

float mapTempC(int rawTemp) {
  return 30.0 + (rawTemp * 70.0) / 1023.0;
}

int mapThroughputCpm(int rawTput) {
  return map(rawTput, 0, 1023, 0, 120);
}

int mapRpm(int rawTput) {
  return map(rawTput, 0, 1023, 0, 3000);
}

float mapCurrentAmps(int rawTput, float tempC) {
  float baseAmps = (rawTput * 10.0) / 1023.0;
  float thermalFactor = 1.0 + max(0.0f, (tempC - 60.0f)) * 0.02;
  return baseAmps * thermalFactor;
}

float mapHumidityPct(int rawTemp) {
  return 30.0 + (rawTemp * 50.0) / 1023.0;
}

const char* stateToString(MachineState state) {
  switch (state) {
    case RUN:
      return "RUN";
    case STOPPED:
      return "STOPPED";
    case FAULT:
      return "FAULT";
    default:
      return "STOPPED";
  }
}

void updateStateFromButtons() {
  unsigned long nowMs = millis();

  int runReading = digitalRead(BTN_RUN);
  if (lastRunButtonReading == HIGH && runReading == LOW && (nowMs - lastRunEdgeMs) > DEBOUNCE_MS) {
    lastRunEdgeMs = nowMs;
    if (currentState == RUN) {
      currentState = STOPPED;
      previousNonFaultState = STOPPED;
    } else if (currentState == STOPPED) {
      currentState = RUN;
      previousNonFaultState = RUN;
    }
  }
  lastRunButtonReading = runReading;

  int faultReading = digitalRead(BTN_FAULT);
  if (lastFaultButtonReading == HIGH && faultReading == LOW && (nowMs - lastFaultEdgeMs) > DEBOUNCE_MS) {
    lastFaultEdgeMs = nowMs;
    if (currentState == FAULT) {
      currentState = previousNonFaultState;
    } else {
      if (currentState != FAULT) {
        previousNonFaultState = currentState;
      }
      currentState = FAULT;
    }
  }
  lastFaultButtonReading = faultReading;
}

void connectWifi() {
  if (WiFi.status() == WL_CONNECTED) {
    return;
  }

  Serial.print("Connecting WiFi SSID: ");
  Serial.println(WIFI_SSID);
  unsigned long backoff = 3000;
  while (WiFi.status() != WL_CONNECTED) {
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    delay(backoff);
    backoff = min(backoff * 2, 30000UL);
    Serial.print(".");
  }
  Serial.println();
  Serial.print("WiFi connected, IP: ");
  Serial.println(WiFi.localIP());
}

void connectMqtt() {
  if (WiFi.status() != WL_CONNECTED) {
    return;
  }
  unsigned long backoff = 2000;
  while (!mqttClient.connected()) {
    Serial.println("Connecting to Azure IoT Hub MQTT...");
    bool ok = mqttClient.connect(DEVICE_ID, mqttUsername, SAS_TOKEN);
    if (ok) {
      Serial.println("Connected to Azure IoT Hub.");
      return;
    }
    Serial.print("MQTT connect failed, rc=");
    Serial.println(mqttClient.state());
    delay(backoff);
    backoff = min(backoff * 2, 30000UL);
  }
}

void reconnectIfNeeded() {
  if (WiFi.status() != WL_CONNECTED) {
    connectWifi();
  }
  if (WiFi.status() == WL_CONNECTED && !mqttClient.connected()) {
    connectMqtt();
  }
}

TelemetrySample readSample() {
  int rawVib = analogRead(POT_VIB);
  int rawTemp = analogRead(POT_TEMP);
  int rawTput = analogRead(POT_TPUT);

  float vibration = mapVibrationMmS(rawVib);
  float tempC = mapTempC(rawTemp);
  int throughput = mapThroughputCpm(rawTput);
  int rpm = mapRpm(rawTput);
  float currentAmps = mapCurrentAmps(rawTput, tempC);
  float humidityPct = mapHumidityPct(rawTemp);
  const char* emittedState = stateToString(currentState);
  const char* faultCode = "NONE";
  bool thresholdFault = (tempC >= TEMP_FAULT_THRESHOLD_C)
                     || (vibration >= VIBRATION_FAULT_THRESHOLD_MM_S)
                     || (currentAmps >= CURRENT_FAULT_THRESHOLD_A);

  if (currentState == STOPPED) {
    throughput = 0;
    rpm = 0;
    currentAmps = 0.0;
  } else if (currentState == FAULT) {
    throughput = 0;
    rpm = 0;
    tempC = 90.0;
    vibration = max(vibration + 2.5, 10.5);
    currentAmps = max(currentAmps, CURRENT_FAULT_THRESHOLD_A + 1.0);
    faultCode = "MANUAL_FAULT";
  } else if (currentState == RUN && thresholdFault) {
    emittedState = "FAULT";
    throughput = 0;
    rpm = 0;
    if (currentAmps >= CURRENT_FAULT_THRESHOLD_A) {
      faultCode = "OVERCURRENT";
    } else if (vibration >= VIBRATION_FAULT_THRESHOLD_MM_S && rpm > 2000) {
      faultCode = "BEARING_WEAR";
    } else if (tempC >= TEMP_FAULT_THRESHOLD_C) {
      faultCode = "OVERTEMP";
    } else {
      faultCode = "VIBRATION";
    }
  }

  bool emittedFault = strcmp(emittedState, "FAULT") == 0;
  digitalWrite(LED_RUN, strcmp(emittedState, "RUN") == 0 ? HIGH : LOW);
  digitalWrite(LED_FAULT, emittedFault ? HIGH : LOW);

  TelemetrySample s = {vibration, tempC, throughput, rpm, currentAmps, humidityPct, emittedState, faultCode};
  return s;
}

void emitSerialCsv(const TelemetrySample& s) {
  float loadPct = constrain(((float)s.throughput / 120.0) * 100.0, 0.0, 100.0);
  float voltageV = 230.0;
  float currentA = s.currentAmps;
  float powerFactor = 0.92;
  float powerKw = (voltageV * currentA * powerFactor * 1.732) / 1000.0;
  float pressureBar = max(1.0, 2.5 + loadPct / 45.0);
  float flowRateLpm = max(5.0, 40.0 + ((float)s.throughput * 0.95));

  // Expanded CSV (fallback-safe): the Python sender reads first 5 fields and
  // ignores extras, so we can expose richer telemetry for local debugging.
  // vibration,temp,throughput,state,faultCode,rpm,current_amps,humidity_pct,
  // load_pct,power_kw,power_factor,voltage_v,pressure_bar,flow_rate_lpm
  Serial.print(s.vibration, 2);     Serial.print(",");
  Serial.print(s.tempC, 2);         Serial.print(",");
  Serial.print(s.throughput);       Serial.print(",");
  Serial.print(s.state);            Serial.print(",");
  Serial.print(s.faultCode);        Serial.print(",");
  Serial.print(s.rpm);              Serial.print(",");
  Serial.print(currentA, 2);        Serial.print(",");
  Serial.print(s.humidityPct, 1);   Serial.print(",");
  Serial.print(loadPct, 2);         Serial.print(",");
  Serial.print(powerKw, 3);         Serial.print(",");
  Serial.print(powerFactor, 3);     Serial.print(",");
  Serial.print(voltageV, 1);        Serial.print(",");
  Serial.print(pressureBar, 3);     Serial.print(",");
  Serial.println(flowRateLpm, 2);
}

void epochToIso8601(unsigned long epoch, char* buf, size_t bufLen) {
  unsigned long s = epoch;
  int sec  = s % 60; s /= 60;
  int mn   = s % 60; s /= 60;
  int hr   = s % 24; s /= 24;
  int days = (int)s;
  int year = 1970;
  while (true) {
    int diy = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
    if (days < diy) break;
    days -= diy;
    year++;
  }
  int md[] = {31,28,31,30,31,30,31,31,30,31,30,31};
  if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) md[1] = 29;
  int month = 0;
  while (days >= md[month]) { days -= md[month]; month++; }
  snprintf(buf, bufLen, "%04d-%02d-%02dT%02d:%02d:%02dZ",
           year, month + 1, days + 1, hr, mn, sec);
}

void publishMqttJson(const TelemetrySample& s) {
  if (WiFi.status() != WL_CONNECTED) {
    connectWifi();
  }
  if (!mqttClient.connected()) {
    connectMqtt();
  }
  mqttClient.loop();

  char vibBuf[16], tempBuf[16], curBuf[16], humBuf[16];
  dtostrf(s.vibration, 1, 2, vibBuf);
  dtostrf(s.tempC, 1, 2, tempBuf);
  dtostrf(s.currentAmps, 1, 2, curBuf);
  dtostrf(s.humidityPct, 1, 1, humBuf);

  char* vib = vibBuf;   while (*vib == ' ') vib++;
  char* temp = tempBuf;  while (*temp == ' ') temp++;
  char* cur = curBuf;    while (*cur == ' ') cur++;
  char* hum = humBuf;    while (*hum == ' ') hum++;

  char payload[576];
  float loadPct = constrain(((float)s.throughput / 120.0) * 100.0, 0.0, 100.0);
  float voltageV = 230.0;
  float currentA = s.currentAmps;
  float powerFactor = 0.92;
  float powerKw = (voltageV * currentA * powerFactor * 1.732) / 1000.0;
  float pressureBar = max(1.0, 2.5 + loadPct / 45.0);
  float flowRateLpm = max(5.0, 40.0 + ((float)s.throughput * 0.95));
  char loadBuf[16], pkwBuf[16], pfBuf[16], voltBuf[16], pressureBuf[16], flowBuf[16];
  dtostrf(loadPct, 1, 2, loadBuf);
  dtostrf(powerKw, 1, 3, pkwBuf);
  dtostrf(powerFactor, 1, 3, pfBuf);
  dtostrf(voltageV, 1, 1, voltBuf);
  dtostrf(pressureBar, 1, 3, pressureBuf);
  dtostrf(flowRateLpm, 1, 2, flowBuf);
  char* load = loadBuf; while (*load == ' ') load++;
  char* pkw = pkwBuf; while (*pkw == ' ') pkw++;
  char* pf = pfBuf; while (*pf == ' ') pf++;
  char* volt = voltBuf; while (*volt == ' ') volt++;
  char* pressure = pressureBuf; while (*pressure == ' ') pressure++;
  char* flow = flowBuf; while (*flow == ' ') flow++;

  // NTP-synced device timestamp; falls back to null if NTP not yet available
  char tsBuf[32];
  bool hasTs = false;
  unsigned long epoch = WiFi.getTime();
  if (epoch > 0) {
    epochToIso8601(epoch, tsBuf, sizeof(tsBuf));
    hasTs = true;
  }

  if (strcmp(s.faultCode, "NONE") == 0) {
    if (hasTs) {
      snprintf(
        payload, sizeof(payload),
        "{\"machine_id\":\"%s\",\"vibration_mm_s\":%s,\"temp_c\":%s,\"throughput_cpm\":%d,"
        "\"rpm\":%d,\"current_amps\":%s,\"humidity_pct\":%s,"
        "\"load_pct\":%s,\"power_kw\":%s,\"power_factor\":%s,\"voltage_v\":%s,\"pressure_bar\":%s,\"flow_rate_lpm\":%s,"
        "\"state\":\"%s\",\"fault_code\":null,\"ts\":\"%s\"}",
        MACHINE_ID, vib, temp, s.throughput,
        s.rpm, cur, hum, load, pkw, pf, volt, pressure, flow, s.state, tsBuf
      );
    } else {
      snprintf(
        payload, sizeof(payload),
        "{\"machine_id\":\"%s\",\"vibration_mm_s\":%s,\"temp_c\":%s,\"throughput_cpm\":%d,"
        "\"rpm\":%d,\"current_amps\":%s,\"humidity_pct\":%s,"
        "\"load_pct\":%s,\"power_kw\":%s,\"power_factor\":%s,\"voltage_v\":%s,\"pressure_bar\":%s,\"flow_rate_lpm\":%s,"
        "\"state\":\"%s\",\"fault_code\":null,\"ts\":null}",
        MACHINE_ID, vib, temp, s.throughput,
        s.rpm, cur, hum, load, pkw, pf, volt, pressure, flow, s.state
      );
    }
  } else {
    if (hasTs) {
      snprintf(
        payload, sizeof(payload),
        "{\"machine_id\":\"%s\",\"vibration_mm_s\":%s,\"temp_c\":%s,\"throughput_cpm\":%d,"
        "\"rpm\":%d,\"current_amps\":%s,\"humidity_pct\":%s,"
        "\"load_pct\":%s,\"power_kw\":%s,\"power_factor\":%s,\"voltage_v\":%s,\"pressure_bar\":%s,\"flow_rate_lpm\":%s,"
        "\"state\":\"%s\",\"fault_code\":\"%s\",\"ts\":\"%s\"}",
        MACHINE_ID, vib, temp, s.throughput,
        s.rpm, cur, hum, load, pkw, pf, volt, pressure, flow, s.state, s.faultCode, tsBuf
      );
    } else {
      snprintf(
        payload, sizeof(payload),
        "{\"machine_id\":\"%s\",\"vibration_mm_s\":%s,\"temp_c\":%s,\"throughput_cpm\":%d,"
        "\"rpm\":%d,\"current_amps\":%s,\"humidity_pct\":%s,"
        "\"load_pct\":%s,\"power_kw\":%s,\"power_factor\":%s,\"voltage_v\":%s,\"pressure_bar\":%s,\"flow_rate_lpm\":%s,"
        "\"state\":\"%s\",\"fault_code\":\"%s\",\"ts\":null}",
        MACHINE_ID, vib, temp, s.throughput,
        s.rpm, cur, hum, load, pkw, pf, volt, pressure, flow, s.state, s.faultCode
      );
    }
  }

  bool published = mqttClient.publish(mqttTopic, payload);
  if (!published) {
    Serial.println("MQTT publish failed.");
  }
}

void emitSample() {
  TelemetrySample s = readSample();
  emitSerialCsv(s);
  publishMqttJson(s);
}

void loop() {
  updateStateFromButtons();
  reconnectIfNeeded();

  if (WiFi.status() == WL_CONNECTED) {
    mqttClient.loop();
  }

  unsigned long nowMs = millis();
  if (nowMs - lastEmitMs >= SAMPLE_INTERVAL_MS) {
    lastEmitMs = nowMs;
    emitSample();
  }
}
