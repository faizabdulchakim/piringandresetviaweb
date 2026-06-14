#include <ESP8266WiFi.h>
#include <ESP8266WebServer.h>
#include <EEPROM.h>
#include <ESP8266HTTPClient.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>

#define EEPROM_SIZE 1024
#define CONFIG_MAGIC 0xA5
#define CONFIG_VERSION 3

ESP8266WebServer server(80);
WiFiClient espClient;
PubSubClient mqttClient(espClient);

const char* API_ACTIVATE_URL = "";

// Hardware identity: harus unik per device saat produksi.
const char* HW_DEVICE_ID = "ESP32-001";
const char* HW_USERNAME = "esp32_user_001";
const char* HW_PASSWORD = "esp-secret-001";

const char* mqttHostFallback = "";
const uint16_t mqttPortFallback = 1883;
const char* mqttUser = "";
const char* mqttPass = "";

struct DeviceConfig {
  uint8_t magic;
  uint8_t version;
  char ssid[33];
  char pass[65];
  char pairToken[65];
  char mqttHost[65];
  uint16_t mqttPort;
  char mqttClientId[65];
  char mqttTopicCmd[129];
  char mqttTopicRes[129];
  char mqttTopicPing[129];
  char mqttTopicPong[129];
  uint8_t isPaired;
  char apiUrl[129];
  char mqttUser[33];
  char mqttPass[65];
};

DeviceConfig cfg;

unsigned long lastPingMs = 0;
unsigned long lastActivationAttemptMs = 0;
const int WIFI_CONNECT_RETRIES = 60;  // 60 x 500ms = 30s

bool isExternalResetButtonPress() {
  String reason = ESP.getResetReason();
  reason.toLowerCase();
  return reason.indexOf("external") >= 0;
}

const char* wifiStatusToText(int status) {
  switch (status) {
    case WL_IDLE_STATUS:
      return "WL_IDLE_STATUS";
    case WL_NO_SSID_AVAIL:
      return "WL_NO_SSID_AVAIL";
    case WL_SCAN_COMPLETED:
      return "WL_SCAN_COMPLETED";
    case WL_CONNECTED:
      return "WL_CONNECTED";
    case WL_CONNECT_FAILED:
      return "WL_CONNECT_FAILED";
    case WL_CONNECTION_LOST:
      return "WL_CONNECTION_LOST";
    case WL_DISCONNECTED:
      return "WL_DISCONNECTED";
    default:
      return "WL_UNKNOWN";
  }
}

String jsonExtractString(const String& json, const String& key) {
  int keyPos = json.indexOf("\"" + key + "\"");
  if (keyPos < 0) {
    return "";
  }

  int colonPos = json.indexOf(':', keyPos);
  if (colonPos < 0) {
    return "";
  }

  int i = colonPos + 1;
  while (i < (int)json.length() && (json[i] == ' ' || json[i] == '\n' || json[i] == '\r' || json[i] == '\t')) {
    i++;
  }

  if (i >= (int)json.length() || json[i] != '"') {
    return "";
  }
  i++;

  int start = i;
  while (i < (int)json.length()) {
    if (json[i] == '"' && json[i - 1] != '\\') {
      break;
    }
    i++;
  }

  if (i >= (int)json.length()) {
    return "";
  }

  return json.substring(start, i);
}

long jsonExtractNumber(const String& json, const String& key, long defaultValue) {
  int keyPos = json.indexOf("\"" + key + "\"");
  if (keyPos < 0) {
    return defaultValue;
  }

  int colonPos = json.indexOf(':', keyPos);
  if (colonPos < 0) {
    return defaultValue;
  }

  int i = colonPos + 1;
  while (i < (int)json.length() && (json[i] == ' ' || json[i] == '\n' || json[i] == '\r' || json[i] == '\t')) {
    i++;
  }

  int start = i;
  while (i < (int)json.length() && (isDigit(json[i]) || json[i] == '-')) {
    i++;
  }

  if (start == i) {
    return defaultValue;
  }

  return json.substring(start, i).toInt();
}

bool jsonContainsTrue(const String& json, const String& key) {
  int keyPos = json.indexOf("\"" + key + "\"");
  if (keyPos < 0) {
    return false;
  }

  int colonPos = json.indexOf(':', keyPos);
  if (colonPos < 0) {
    return false;
  }

  String rest = json.substring(colonPos + 1);
  rest.trim();
  return rest.startsWith("true");
}

void clearConfig() {
  memset(&cfg, 0, sizeof(cfg));
  cfg.magic = CONFIG_MAGIC;
  cfg.version = CONFIG_VERSION;
  cfg.mqttPort = mqttPortFallback;
}

void clearPairingHistoryOnly() {
  memset(cfg.pairToken, 0, sizeof(cfg.pairToken));
  memset(cfg.mqttHost, 0, sizeof(cfg.mqttHost));
  memset(cfg.mqttClientId, 0, sizeof(cfg.mqttClientId));
  memset(cfg.mqttTopicCmd, 0, sizeof(cfg.mqttTopicCmd));
  memset(cfg.mqttTopicRes, 0, sizeof(cfg.mqttTopicRes));
  memset(cfg.mqttTopicPing, 0, sizeof(cfg.mqttTopicPing));
  memset(cfg.mqttTopicPong, 0, sizeof(cfg.mqttTopicPong));
  cfg.isPaired = 0;
  cfg.mqttPort = mqttPortFallback;
}

void saveConfig() {
  EEPROM.begin(EEPROM_SIZE);
  uint8_t* raw = (uint8_t*)&cfg;
  for (unsigned int i = 0; i < sizeof(cfg); i++) {
    EEPROM.write(i, raw[i]);
  }
  EEPROM.commit();
}

void loadConfig() {
  EEPROM.begin(EEPROM_SIZE);
  uint8_t* raw = (uint8_t*)&cfg;
  for (unsigned int i = 0; i < sizeof(cfg); i++) {
    raw[i] = EEPROM.read(i);
  }

  if (cfg.magic != CONFIG_MAGIC || cfg.version != CONFIG_VERSION) {
    clearConfig();
    saveConfig();
  }
}

void publishMqtt(const char* payload) {
  if (!mqttClient.connected()) {
    return;
  }
  mqttClient.publish(cfg.mqttTopicPing, payload);
}

void publishMqttToTopic(const char* topic, const String& payload, bool retain = false) {
  if (!mqttClient.connected() || topic == nullptr || strlen(topic) == 0) {
    return;
  }
  mqttClient.publish(topic, payload.c_str(), retain);
}

void publishCommandResponse(const String& action, const String& status, const String& detail = "") {
  if (strlen(cfg.mqttTopicRes) == 0) {
    return;
  }

  String payload = "{";
  payload += "\"action\":\"" + action + "\",";
  payload += "\"status\":\"" + status + "\",";
  payload += "\"detail\":\"" + detail + "\"";
  payload += "}";
  publishMqttToTopic(cfg.mqttTopicRes, payload, false);
}

void handleMqttCommand(const String& commandPayload) {
  String action = jsonExtractString(commandPayload, "action");
  if (action.length() == 0) {
    action = commandPayload;
  }
  action.trim();
  action.toLowerCase();

  Serial.print("MQTT command payload: ");
  Serial.println(commandPayload);

  if (action == "clear_pairing_history" || action == "clear_pairing" || action == "reset_connection") {
    publishCommandResponse(action, "ok", "Clearing pairing history and WiFi credentials");
    clearConfig();
    saveConfig();
    delay(1000);
    ESP.restart();
    return;
  }

  if (action == "factory_reset" || action == "reset_all") {
    publishCommandResponse(action, "ok", "Factory reset");
    clearConfig();
    saveConfig();
    delay(1000);
    ESP.restart();
    return;
  }

  publishCommandResponse(action, "error", "Unknown action");
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String msg;
  for (unsigned int i = 0; i < length; i++) {
    msg += (char)payload[i];
  }
  msg.trim();

  Serial.print("MQTT recv: ");
  Serial.println(msg);

  if (strlen(cfg.mqttTopicCmd) > 0 && strcmp(topic, cfg.mqttTopicCmd) == 0) {
    handleMqttCommand(msg);
    return;
  }

  if (strlen(cfg.mqttTopicPong) > 0 && strcmp(topic, cfg.mqttTopicPong) == 0) {
    msg.toLowerCase();
    Serial.println("MQTT pong received");
  }
}

void connectMqtt() {
  if (WiFi.status() != WL_CONNECTED) {
    return;
  }

  if (!cfg.isPaired || strlen(cfg.mqttTopicCmd) == 0) {
    return;
  }

  const char* host = strlen(cfg.mqttHost) > 0 ? cfg.mqttHost : mqttHostFallback;
  uint16_t port = cfg.mqttPort > 0 ? cfg.mqttPort : mqttPortFallback;
  const char* mUser = strlen(cfg.mqttUser) > 0 ? cfg.mqttUser : mqttUser;
  const char* mPass = strlen(cfg.mqttPass) > 0 ? cfg.mqttPass : mqttPass;

  mqttClient.setServer(host, port);
  mqttClient.setCallback(mqttCallback);

  while (!mqttClient.connected()) {
    String clientId = strlen(cfg.mqttClientId) > 0
      ? String(cfg.mqttClientId)
      : "esp8266-" + String(ESP.getChipId(), HEX);

    Serial.print("Connecting MQTT...");

    bool ok = mqttClient.connect(
      clientId.c_str(),
      mUser,
      mPass,
      cfg.mqttTopicPing,
      1,
      true,
      "berhenti"
    );

    if (ok) {
      Serial.println("connected");
      mqttClient.subscribe(cfg.mqttTopicCmd);
      mqttClient.subscribe(cfg.mqttTopicPong);
      publishMqtt("ping");
      Serial.println("MQTT send: ping");
      break;
    }

    Serial.print("failed, rc=");
    Serial.print(mqttClient.state());
    Serial.println(" retry in 3 seconds");
    delay(3000);
  }
}

void setupWebRoutes() {
  server.on("/", handleRoot);
  server.on("/save", handleSave);
  server.on("/reset", handleReset);
  server.on("/clear_pairing_history", handleClearPairingHistory);
}

bool activateDevice() {
  if (WiFi.status() != WL_CONNECTED) {
    return false;
  }

  if (strlen(cfg.pairToken) == 0) {
    Serial.println("No pair token. Skip activation.");
    return false;
  }

  String activationUrl = strlen(cfg.apiUrl) > 0 ? String(cfg.apiUrl) : String(API_ACTIVATE_URL);
  Serial.print("Activating device to: ");
  Serial.println(activationUrl);

  HTTPClient http;
  bool ok = false;
  WiFiClientSecure secureClient;
  WiFiClient client;

  if (activationUrl.startsWith("https://")) {
    secureClient.setInsecure();
    ok = http.begin(secureClient, activationUrl);
  } else {
    ok = http.begin(client, activationUrl);
  }

  if (!ok) {
    Serial.println("HTTP/HTTPS begin failed");
    return false;
  }

  http.addHeader("Content-Type", "application/json");

  String payload = "{";
  payload += "\"pair_token\":\"" + String(cfg.pairToken) + "\",";
  payload += "\"device_id\":\"" + String(HW_DEVICE_ID) + "\",";
  payload += "\"hardware_username\":\"" + String(HW_USERNAME) + "\",";
  payload += "\"hardware_password\":\"" + String(HW_PASSWORD) + "\"";
  payload += "}";

  int code = http.POST(payload);
  String body = http.getString();
  http.end();

  Serial.print("Activate status: ");
  Serial.println(code);
  Serial.println(body);

  if (code == 401) {
    Serial.println("Invalid or expired pair token. Resetting config and rebooting to AP mode...");
    clearConfig();
    saveConfig();
    delay(1000);
    ESP.restart();
    return false;
  }

  if (code < 200 || code >= 300) {
    return false;
  }

  if (!jsonContainsTrue(body, "status")) {
    return false;
  }

  String host = jsonExtractString(body, "host");
  String clientId = jsonExtractString(body, "client_id");
  String mUser = jsonExtractString(body, "username");
  String mPass = jsonExtractString(body, "password");
  String topicCmd = jsonExtractString(body, "cmd");
  String topicRes = jsonExtractString(body, "res");
  String topicPing = jsonExtractString(body, "ping");
  String topicPong = jsonExtractString(body, "pong");
  long port = jsonExtractNumber(body, "port", mqttPortFallback);

  if (topicCmd.length() == 0 || topicPing.length() == 0 || topicPong.length() == 0) {
    Serial.println("Activation response missing topics");
    return false;
  }

  memset(cfg.mqttHost, 0, sizeof(cfg.mqttHost));
  memset(cfg.mqttClientId, 0, sizeof(cfg.mqttClientId));
  memset(cfg.mqttUser, 0, sizeof(cfg.mqttUser));
  memset(cfg.mqttPass, 0, sizeof(cfg.mqttPass));
  memset(cfg.mqttTopicCmd, 0, sizeof(cfg.mqttTopicCmd));
  memset(cfg.mqttTopicRes, 0, sizeof(cfg.mqttTopicRes));
  memset(cfg.mqttTopicPing, 0, sizeof(cfg.mqttTopicPing));
  memset(cfg.mqttTopicPong, 0, sizeof(cfg.mqttTopicPong));

  if (host.length() > 0) {
    host.toCharArray(cfg.mqttHost, sizeof(cfg.mqttHost));
  }
  if (clientId.length() > 0) {
    clientId.toCharArray(cfg.mqttClientId, sizeof(cfg.mqttClientId));
  }
  if (mUser.length() > 0) {
    mUser.toCharArray(cfg.mqttUser, sizeof(cfg.mqttUser));
  }
  if (mPass.length() > 0) {
    mPass.toCharArray(cfg.mqttPass, sizeof(cfg.mqttPass));
  }

  topicCmd.toCharArray(cfg.mqttTopicCmd, sizeof(cfg.mqttTopicCmd));
  topicRes.toCharArray(cfg.mqttTopicRes, sizeof(cfg.mqttTopicRes));

  topicPing.toCharArray(cfg.mqttTopicPing, sizeof(cfg.mqttTopicPing));
  topicPong.toCharArray(cfg.mqttTopicPong, sizeof(cfg.mqttTopicPong));
  cfg.mqttPort = (uint16_t)port;
  cfg.isPaired = 1;

  saveConfig();
  Serial.println("Activation success. MQTT contract saved.");
  return true;
}

void handleRoot() {
  String currentApiUrl = strlen(cfg.apiUrl) > 0 ? String(cfg.apiUrl) : String(API_ACTIVATE_URL);
  String html = "<h2>WiFi Setup</h2>"
                "<form method='POST' action='/save'>"
                "SSID: <input name='ssid' value='" + String(cfg.ssid) + "'><br>"
                "Password: <input name='pass' value='" + String(cfg.pass) + "' type='password'><br><br>"
                "Pair Token: <input name='pair_token' value='" + String(cfg.pairToken) + "'><br><br>"
                "API URL: <input name='api_url' value='" + currentApiUrl + "' style='width: 300px;'><br><br>"
                "<input type='submit' value='Save WiFi'>"
                "</form>"
                "<br><br>"
                "<a href='/reset'>🔄 Reset WiFi Settings</a>";
  server.send(200, "text/html", html);
}

void handleSave() {
  String ssid = server.arg("ssid");
  String pass = server.arg("pass");
  String pairToken = server.arg("pair_token");
  String apiUrl = server.arg("api_url");

  ssid.trim();
  pass.trim();
  pairToken.trim();
  apiUrl.trim();

  if (ssid.length() == 0) {
    server.send(400, "text/plain", "SSID is required");
    return;
  }

  memset(cfg.ssid, 0, sizeof(cfg.ssid));
  memset(cfg.pass, 0, sizeof(cfg.pass));
  memset(cfg.pairToken, 0, sizeof(cfg.pairToken));
  memset(cfg.apiUrl, 0, sizeof(cfg.apiUrl));
  memset(cfg.mqttHost, 0, sizeof(cfg.mqttHost));
  memset(cfg.mqttClientId, 0, sizeof(cfg.mqttClientId));
  memset(cfg.mqttTopicCmd, 0, sizeof(cfg.mqttTopicCmd));
  memset(cfg.mqttTopicRes, 0, sizeof(cfg.mqttTopicRes));
  memset(cfg.mqttTopicPing, 0, sizeof(cfg.mqttTopicPing));
  memset(cfg.mqttTopicPong, 0, sizeof(cfg.mqttTopicPong));

  ssid.toCharArray(cfg.ssid, sizeof(cfg.ssid));
  pass.toCharArray(cfg.pass, sizeof(cfg.pass));
  pairToken.toCharArray(cfg.pairToken, sizeof(cfg.pairToken));
  apiUrl.toCharArray(cfg.apiUrl, sizeof(cfg.apiUrl));
  cfg.mqttPort = mqttPortFallback;
  cfg.isPaired = 0;

  server.send(200, "text/plain", "Saved! Rebooting...");

  saveConfig();
  delay(1000);
  ESP.restart();
}

void handleReset() {
  clearConfig();
  saveConfig();
  server.send(200, "text/plain", "WiFi credentials cleared! Rebooting...");
  delay(1000);
  ESP.restart();
}

void handleClearPairingHistory() {
  clearConfig();
  saveConfig();
  server.send(200, "text/plain", "Pairing history and WiFi credentials cleared! Rebooting...");
  delay(1000);
  ESP.restart();
}

void startAP() {
  Serial.println("\nFailed to connect. Starting AP mode...");

  // IP untuk AP mode
  IPAddress apIP(192, 168, 4, 1);
  IPAddress netMsk(255, 255, 255, 0);
  WiFi.softAPConfig(apIP, apIP, netMsk);

  // mulai AP
  WiFi.softAP("MyDevice_Setup");
  Serial.print("AP IP: ");
  Serial.println(WiFi.softAPIP());

  // web server routes
  setupWebRoutes();
  server.begin();
  Serial.println("Web server started");
}

void setup() {
  Serial.begin(115200);
  delay(500);

  Serial.println();
  Serial.println("Booting...");

  if (isExternalResetButtonPress()) {
    Serial.println("External reset detected. Running factory reset logic...");
    clearConfig();
    saveConfig();
  }

  loadConfig();

  if (strlen(cfg.ssid) > 0) {
    Serial.print("Trying to connect to saved WiFi: ");
    Serial.println(cfg.ssid);

    WiFi.mode(WIFI_STA);
    WiFi.disconnect();
    delay(200);
    WiFi.begin(cfg.ssid, cfg.pass);

    int retries = 0;
    while (WiFi.status() != WL_CONNECTED && retries < WIFI_CONNECT_RETRIES) {
      delay(500);
      Serial.print(".");
      if ((retries + 1) % 10 == 0) {
        Serial.print(" [");
        Serial.print(wifiStatusToText(WiFi.status()));
        Serial.print("]");
      }
      retries++;
    }

    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("\nConnected to WiFi!");
      Serial.print("IP Address: ");
      Serial.println(WiFi.localIP());

      if (!cfg.isPaired && strlen(cfg.pairToken) > 0) {
        activateDevice();
      }

      connectMqtt();

      // tetap aktifkan web server agar bisa reset / save meskipun sudah connect
      setupWebRoutes();
      server.begin();
      return;
    }

    Serial.println();
    Serial.print("WiFi connect failed. Last status: ");
    Serial.println(wifiStatusToText(WiFi.status()));
  }

  // kalau gagal connect, masuk AP
  startAP();
}

void loop() {
  server.handleClient(); // selalu layani web server

  if (WiFi.status() == WL_CONNECTED) {
    if (!cfg.isPaired && strlen(cfg.pairToken) > 0 && (millis() - lastActivationAttemptMs >= 10000)) {
      lastActivationAttemptMs = millis();
      activateDevice();
    }

    if (!mqttClient.connected()) {
      connectMqtt();
    }
    mqttClient.loop();

    // heartbeat ping berkala ke app
    if (millis() - lastPingMs >= 10000) {
      publishMqtt("ping");
      lastPingMs = millis();
    }
  }
}
