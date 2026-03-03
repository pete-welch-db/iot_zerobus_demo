// Arduino Uno WiFi Rev2 machine panel simulator for manufacturing demo.
// Primary mode: publishes telemetry JSON directly to Azure IoT Hub over MQTT/TLS.
// Fallback mode: emits CSV over serial for local Python bridge.
//
// Wiring guide:
// - POT_VIB  (A0): potentiometer signal pin -> A0, outer pins -> 5V and GND.
// - POT_TEMP (A1): potentiometer signal pin -> A1, outer pins -> 5V and GND.
// - POT_TPUT (A2): potentiometer signal pin -> A2, outer pins -> 5V and GND.
// - BTN_RUN  (D2): momentary button from D2 to GND (uses INPUT_PULLUP).
// - BTN_FAULT(D3): momentary button from D3 to GND (uses INPUT_PULLUP).
// - LED_RUN  (D10): LED + 220 ohm resistor to GND.
// - LED_FAULT(D11): LED + 220 ohm resistor to GND.
//
// Demo actions:
// - Press RUN button to toggle RUN <-> STOPPED.
// - Press FAULT button to force/clear FAULT.
// - Raise temperature/vibration pots above threshold to auto-emit FAULT risk.

#include <SPI.h>
#include <WiFiNINA.h>
#include <PubSubClient.h>

enum MachineState { RUN, STOPPED, FAULT };

const int POT_VIB = A0;
const int POT_TEMP = A1;
const int POT_TPUT = A2;

const int BTN_RUN = 2;
const int BTN_FAULT = 3;

const int LED_RUN = 10;
const int LED_FAULT = 11;

const unsigned long SAMPLE_INTERVAL_MS = 1000;
const unsigned long DEBOUNCE_MS = 40;
const float TEMP_FAULT_THRESHOLD_C = 85.0;
const float VIBRATION_FAULT_THRESHOLD_MM_S = 9.5;

// ---------- WiFi / IoT Hub Direct Publish Config ----------
// Best practice: replace these placeholders locally or inject from a private header.
const char WIFI_SSID[] = "YOUR_WIFI_SSID";
const char WIFI_PASSWORD[] = "YOUR_WIFI_PASSWORD";

const char IOT_HUB_HOST[] = "iothub-zerobus-demo-welch.azure-devices.net";
const int IOT_HUB_PORT = 8883;
const char DEVICE_ID[] = "arduino-panel";
const char MACHINE_ID[] = "MACH_A";
const char SAS_TOKEN[] = "SharedAccessSignature sr=<resource>&sig=<signature>&se=<expiry>";

// Topic and username follow Azure IoT Hub device MQTT convention.
char mqttTopic[96];
char mqttUsername[192];

WiFiSSLClient sslClient;
PubSubClient mqttClient(sslClient);

MachineState currentState = RUN;
MachineState previousNonFaultState = RUN;

unsigned long lastEmitMs = 0;
unsigned long lastRunEdgeMs = 0;
unsigned long lastFaultEdgeMs = 0;
unsigned long cycleCount = 0;
float runtimeHours = 0.0;

int lastRunButtonReading = HIGH;
int lastFaultButtonReading = HIGH;

struct TelemetrySample {
  float vibration;
  float tempC;
  int throughput;
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
  while (WiFi.status() != WL_CONNECTED) {
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    delay(5000);
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
  while (!mqttClient.connected()) {
    Serial.println("Connecting to Azure IoT Hub MQTT...");
    bool ok = mqttClient.connect(DEVICE_ID, mqttUsername, SAS_TOKEN);
    if (ok) {
      Serial.println("Connected to Azure IoT Hub.");
    } else {
      Serial.print("MQTT connect failed, rc=");
      Serial.println(mqttClient.state());
      delay(3000);
    }
  }
}

TelemetrySample readSample() {
  int rawVib = analogRead(POT_VIB);
  int rawTemp = analogRead(POT_TEMP);
  int rawTput = analogRead(POT_TPUT);

  float vibration = mapVibrationMmS(rawVib);
  float tempC = mapTempC(rawTemp);
  int throughput = mapThroughputCpm(rawTput);
  const char* emittedState = stateToString(currentState);
  const char* faultCode = "NONE";
  bool thresholdFault = (tempC >= TEMP_FAULT_THRESHOLD_C) || (vibration >= VIBRATION_FAULT_THRESHOLD_MM_S);

  if (currentState == STOPPED) {
    throughput = 0;
  } else if (currentState == FAULT) {
    throughput = 0;
    tempC = 90.0;
    // Keep FAULT vibration obviously abnormal.
    vibration = max(vibration + 2.5, 10.5);
    faultCode = "OVERTEMP";
  } else if (currentState == RUN && thresholdFault) {
    // Auto-surface process risk as FAULT when pots exceed limits.
    emittedState = "FAULT";
    throughput = 0;
    faultCode = tempC >= TEMP_FAULT_THRESHOLD_C ? "OVERTEMP" : "VIBRATION";
  }

  bool emittedFault = strcmp(emittedState, "FAULT") == 0;
  digitalWrite(LED_RUN, strcmp(emittedState, "RUN") == 0 ? HIGH : LOW);
  digitalWrite(LED_FAULT, emittedFault ? HIGH : LOW);

  TelemetrySample s = {vibration, tempC, throughput, emittedState, faultCode};
  return s;
}

void emitSerialCsv(const TelemetrySample& s) {
  Serial.print(s.vibration, 2);
  Serial.print(",");
  Serial.print(s.tempC, 2);
  Serial.print(",");
  Serial.print(s.throughput);
  Serial.print(",");
  Serial.print(s.state);
  Serial.print(",");
  Serial.println(s.faultCode);
}

void publishMqttJson(const TelemetrySample& s) {
  if (WiFi.status() != WL_CONNECTED) {
    connectWifi();
  }
  if (!mqttClient.connected()) {
    connectMqtt();
  }
  mqttClient.loop();

  // AVR printf formatting does not reliably support %f; convert floats explicitly.
  char vibBuf[16];
  char tempBuf[16];
  dtostrf(s.vibration, 1, 2, vibBuf);
  dtostrf(s.tempC, 1, 2, tempBuf);

  char* vib = vibBuf;
  while (*vib == ' ') vib++;
  char* temp = tempBuf;
  while (*temp == ' ') temp++;

  cycleCount++;
  runtimeHours += (float)SAMPLE_INTERVAL_MS / 3600000.0;
  float loadPct = constrain(((float)s.throughput / 120.0) * 100.0, 0.0, 100.0);
  int rpm = 800 + (s.throughput * 18);
  float humidityRh = constrain(45.0 + ((s.tempC - 60.0) * 0.4), 20.0, 80.0);
  float voltageV = 230.0;
  float currentA = 4.0 + (loadPct * 0.16);
  float powerFactor = 0.92;
  float powerKw = (voltageV * currentA * powerFactor * 1.732) / 1000.0;
  float pressureBar = max(1.0, 2.5 + loadPct / 45.0);
  float flowRateLpm = max(5.0, 40.0 + ((float)s.throughput * 0.95));

  char loadBuf[16];
  char humidityBuf[16];
  char currentBuf[16];
  char powerKwBuf[16];
  char powerFactorBuf[16];
  char pressureBuf[16];
  char flowBuf[16];
  char runtimeBuf[16];
  dtostrf(loadPct, 1, 2, loadBuf);
  dtostrf(humidityRh, 1, 2, humidityBuf);
  dtostrf(currentA, 1, 2, currentBuf);
  dtostrf(powerKw, 1, 3, powerKwBuf);
  dtostrf(powerFactor, 1, 3, powerFactorBuf);
  dtostrf(pressureBar, 1, 3, pressureBuf);
  dtostrf(flowRateLpm, 1, 2, flowBuf);
  dtostrf(runtimeHours, 1, 4, runtimeBuf);

  char* load = loadBuf; while (*load == ' ') load++;
  char* humidity = humidityBuf; while (*humidity == ' ') humidity++;
  char* current = currentBuf; while (*current == ' ') current++;
  char* powerKwStr = powerKwBuf; while (*powerKwStr == ' ') powerKwStr++;
  char* powerFactorStr = powerFactorBuf; while (*powerFactorStr == ' ') powerFactorStr++;
  char* pressure = pressureBuf; while (*pressure == ' ') pressure++;
  char* flow = flowBuf; while (*flow == ' ') flow++;
  char* runtime = runtimeBuf; while (*runtime == ' ') runtime++;

  // ts remains null on-device; Databricks falls back to IoT Hub enqueue time.
  char payload[700];
  if (strcmp(s.faultCode, "NONE") == 0) {
    snprintf(
      payload,
      sizeof(payload),
      "{\"schema_version\":\"2.0\",\"machine_id\":\"%s\",\"vibration_mm_s\":%s,\"temp_c\":%s,\"throughput_cpm\":%d,\"state\":\"%s\",\"fault_code\":null,\"rpm\":%d,\"load_pct\":%s,\"humidity_rh\":%s,\"current_a\":%s,\"power_kw\":%s,\"power_factor\":%s,\"voltage_v\":230.0,\"pressure_bar\":%s,\"flow_rate_lpm\":%s,\"cycle_count\":%lu,\"runtime_hours\":%s,\"ts\":null}",
      MACHINE_ID,
      vib,
      temp,
      s.throughput,
      s.state,
      rpm,
      load,
      humidity,
      current,
      powerKwStr,
      powerFactorStr,
      pressure,
      flow,
      cycleCount,
      runtime
    );
  } else {
    snprintf(
      payload,
      sizeof(payload),
      "{\"schema_version\":\"2.0\",\"machine_id\":\"%s\",\"vibration_mm_s\":%s,\"temp_c\":%s,\"throughput_cpm\":%d,\"state\":\"%s\",\"fault_code\":\"%s\",\"rpm\":%d,\"load_pct\":%s,\"humidity_rh\":%s,\"current_a\":%s,\"power_kw\":%s,\"power_factor\":%s,\"voltage_v\":230.0,\"pressure_bar\":%s,\"flow_rate_lpm\":%s,\"cycle_count\":%lu,\"runtime_hours\":%s,\"ts\":null}",
      MACHINE_ID,
      vib,
      temp,
      s.throughput,
      s.state,
      s.faultCode,
      rpm,
      load,
      humidity,
      current,
      powerKwStr,
      powerFactorStr,
      pressure,
      flow,
      cycleCount,
      runtime
    );
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

  if (WiFi.status() == WL_CONNECTED) {
    mqttClient.loop();
  }

  unsigned long nowMs = millis();
  if (nowMs - lastEmitMs >= SAMPLE_INTERVAL_MS) {
    lastEmitMs = nowMs;
    emitSample();
  }
}
