#include <ESP8266WiFi.h>
#include <ESP8266WebServer.h>
#include <EEPROM.h>
#include <ESP8266HTTPClient.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>

#define EEPROM_SIZE 2048
#define CONFIG_MAGIC 0xA5
#define CONFIG_VERSION 6

// ==========================================
// CLASS WRAPPER: WEBSOCKET SECURE (WSS)
// ==========================================
class WSSMqttClient : public Client {
private:
    WiFiClientSecure client;
    const char* host;
    uint16_t port;
    const char* path;
    bool connected_ws = false;

    // Ring Buffer untuk menampung data payload WebSocket yang masuk
    uint8_t* rxBuf = nullptr;
    const size_t rxBufSize = 1024;
    size_t rxHead = 0; // Pointer tulis
    size_t rxTail = 0; // Pointer baca

    // Mengirim frame biner WebSocket (opcode 0x02) ke Broker
    void sendFrame(const uint8_t* data, size_t len) {
        if (!client.connected()) return;

        // Header Frame: FIN (1) + Opcode Biner (2) = 0x82
        uint8_t header = 0x82;
        client.write(&header, 1);

        // Masking key (wajib untuk data dari Client ke Server)
        uint8_t mask[4] = { 
            (uint8_t)random(256), 
            (uint8_t)random(256), 
            (uint8_t)random(256), 
            (uint8_t)random(256) 
        };

        if (len < 126) {
            uint8_t lenByte = 0x80 | (uint8_t)len;
            client.write(&lenByte, 1);
        } else if (len <= 65535) {
            uint8_t lenByte = 0x80 | 126;
            client.write(&lenByte, 1);
            uint8_t lenBytes[2] = {
                (uint8_t)(len >> 8),
                (uint8_t)(len & 0xFF)
            };
            client.write(lenBytes, 2);
        } else {
            return; // Payload terlalu besar
        }

        client.write(mask, 4);

        // Lakukan masking ke data sebelum dikirim
        uint8_t* masked = (uint8_t*)malloc(len);
        if (masked) {
            for (size_t i = 0; i < len; i++) {
                masked[i] = data[i] ^ mask[i % 4];
            }
            client.write(masked, len);
            free(masked);
        }
    }

    // Melakukan handshake HTTP Upgrade ke WebSocket
    bool performHandshake() {
        if (path == nullptr) return false;
        String pathStr = String(path);
        if (!pathStr.startsWith("/")) {
            pathStr = "/" + pathStr;
        }

        String req = "GET " + pathStr + " HTTP/1.1\r\n";
        req += "Host: " + String(host) + ":" + String(port) + "\r\n";
        req += "Upgrade: websocket\r\n";
        req += "Connection: Upgrade\r\n";
        req += "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"; // Kunci statis standar
        req += "Sec-WebSocket-Protocol: mqtt\r\n"; // Menggunakan sub-protokol mqtt
        req += "Sec-WebSocket-Version: 13\r\n\r\n";

        client.print(req);

        // Baca header respons
        unsigned long start = millis();
        String response = "";
        while (client.connected() && millis() - start < 5000) {
            if (client.available()) {
                char c = client.read();
                response += c;
                if (response.endsWith("\r\n\r\n")) {
                    break;
                }
            }
            yield();
        }

        // Cek status code 101 Switching Protocols
        if (response.indexOf("101 Switching Protocols") >= 0 || response.indexOf("101 ") >= 0) {
            connected_ws = true;
            rxHead = 0;
            rxTail = 0;
            return true;
        }

        client.stop();
        return false;
    }

    // Membaca stream TCP dan mengekstrak payload dari frame WebSocket
    void parseIncoming() {
        if (!client.connected()) {
            connected_ws = false;
            return;
        }

        while (client.available() >= 2) {
            uint8_t header = client.read();
            uint8_t lenByte = client.read();

            uint8_t opcode = header & 0x0F;
            bool masked = (lenByte & 0x80) != 0;
            uint32_t payloadLen = lenByte & 0x7F;

            if (payloadLen == 126) {
                unsigned long start = millis();
                while (client.available() < 2) {
                    if (!client.connected() || (millis() - start > 2000)) return;
                    yield();
                }
                payloadLen = (client.read() << 8) | client.read();
            } else if (payloadLen == 127) {
                unsigned long start = millis();
                while (client.available() < 8) {
                    if (!client.connected() || (millis() - start > 2000)) return;
                    yield();
                }
                payloadLen = 0;
                for (int i = 0; i < 8; i++) {
                    payloadLen = (payloadLen << 8) | client.read();
                }
            }

            uint8_t maskKey[4] = {0};
            if (masked) {
                unsigned long start = millis();
                while (client.available() < 4) {
                    if (!client.connected() || (millis() - start > 2000)) return;
                    yield();
                }
                client.read(maskKey, 4);
            }

            // Baca payload data
            for (uint32_t i = 0; i < payloadLen; i++) {
                unsigned long start = millis();
                while (!client.available()) {
                    if (!client.connected() || (millis() - start > 2000)) return;
                    yield();
                }
                uint8_t val = client.read();
                if (masked) {
                    val ^= maskKey[i % 4];
                }

                // Masukkan data frame text/binary/continuation ke dalam ring buffer baca
                if (opcode == 0x00 || opcode == 0x01 || opcode == 0x02) {
                    size_t nextHead = (rxHead + 1) % rxBufSize;
                    if (nextHead != rxTail) {
                        rxBuf[rxHead] = val;
                        rxHead = nextHead;
                    }
                }
            }

            // Tangani frame khusus WebSocket
            if (opcode == 0x08) { // Frame Close
                connected_ws = false;
                client.stop();
                return;
            } else if (opcode == 0x09) { // Frame Ping -> Jawab dengan Pong
                uint8_t pongFrame[2] = { 0x8A, 0x00 };
                client.write(pongFrame, 2);
            }
        }
    }

public:
    WSSMqttClient(const char* h, uint16_t p, const char* pathStr) 
      : host(h), port(p), path(pathStr) {
        rxBuf = (uint8_t*)malloc(rxBufSize);
        
        // Lewati verifikasi sertifikat root CA
        // Ini menghindari perlunya sinkronisasi waktu lewat NTP.
        client.setInsecure();
    }

    ~WSSMqttClient() {
        if (rxBuf) free(rxBuf);
    }

    void setDestination(const char* h, uint16_t p, const char* pathStr) {
        host = h;
        port = p;
        path = pathStr;
    }

    int connect(IPAddress ip, uint16_t port) override { return 0; }
    int connect(const char *host, uint16_t port) override {
        connected_ws = false;
        this->host = host;
        this->port = port;
        if (client.connect(host, port)) {
            return performHandshake() ? 1 : 0;
        }
        return 0;
    }

    size_t write(uint8_t b) override {
        return write(&b, 1);
    }

    size_t write(const uint8_t *buf, size_t size) override {
        sendFrame(buf, size);
        return size;
    }

    int available() override {
        parseIncoming();
        return (rxHead >= rxTail) ? (rxHead - rxTail) : (rxBufSize - rxTail + rxHead);
    }

    int read() override {
        parseIncoming();
        if (rxHead == rxTail) return -1;
        uint8_t val = rxBuf[rxTail];
        rxTail = (rxTail + 1) % rxBufSize;
        return val;
    }

    int read(uint8_t *buf, size_t size) override {
        size_t bytesRead = 0;
        while (bytesRead < size) {
            int val = read();
            if (val == -1) break;
            buf[bytesRead++] = (uint8_t)val;
        }
        return bytesRead;
    }

    int peek() override {
        parseIncoming();
        if (rxHead == rxTail) return -1;
        return rxBuf[rxTail];
    }

    void flush() override {
        client.flush();
    }

    void stop() override {
        connected_ws = false;
        client.stop();
    }

    uint8_t connected() override {
        return (client.connected() && connected_ws) ? 1 : 0;
    }

    operator bool() override {
        return connected() == 1;
    }
};

ESP8266WebServer server(80);
WSSMqttClient wssClient(nullptr, 0, nullptr);
PubSubClient mqttClient(wssClient);

const char* API_ACTIVATE_URL = "";

// Motor Pins mapping (L298N driver)
const int pinIN1 = D3; // d1 wemos  M!
const int pinIN2 = D4; // d2 wemos  M1
const int pinIN3 = D5; // d5 wemos  M2
const int pinIN4 = D6; // d6 wemos  M2
const int pinIN5 = D2; // d0 wemos  M3
const int pinIN6 = D8; // d3 wemos  M3
const int pinIN7 = D9; // d4 wemos  M4
const int pinIN8 = D7; // d7 wemos  M4

// Hardware identity: harus unik per device saat produksi.
const char* HW_DEVICE_ID = "ESP32-001";
const char* HW_USERNAME = "esp32_user_001";
const char* HW_PASSWORD = "esp-secret-001";

const char* mqttHostFallback = "";
const uint16_t mqttPortFallback = 1883;
const char* mqttUser = "";
const char* mqttPass = "";
const char* mqttBasepathFallback = "mqtt";

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
  char mqttPath[65];
  uint8_t bootCount;
};

DeviceConfig cfg;

unsigned long lastPingMs = 0;
unsigned long lastActivationAttemptMs = 0;
const int WIFI_CONNECT_RETRIES = 60;  // 60 x 500ms = 30s
bool bootCountCleared = false;

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

void runMotor1Forward() { digitalWrite(pinIN1, LOW); digitalWrite(pinIN2, HIGH); }
void runMotor1Backward() { digitalWrite(pinIN1, HIGH); digitalWrite(pinIN2, LOW); }
void runMotor2Forward() { digitalWrite(pinIN3, LOW); digitalWrite(pinIN4, HIGH); }
void runMotor2Backward() { digitalWrite(pinIN3, HIGH); digitalWrite(pinIN4, LOW); }
void runMotor3Forward() { digitalWrite(pinIN5, LOW); digitalWrite(pinIN6, HIGH); }
void runMotor3Backward() { digitalWrite(pinIN5, HIGH); digitalWrite(pinIN6, LOW); }
void runMotor4Forward() { digitalWrite(pinIN7, LOW); digitalWrite(pinIN8, HIGH); }
void runMotor4Backward() { digitalWrite(pinIN7, HIGH); digitalWrite(pinIN8, LOW); }
void stopMotor() {
  digitalWrite(pinIN1, HIGH); digitalWrite(pinIN2, HIGH);
  digitalWrite(pinIN3, HIGH); digitalWrite(pinIN4, HIGH);
  digitalWrite(pinIN5, HIGH); digitalWrite(pinIN6, HIGH);
  digitalWrite(pinIN7, HIGH); digitalWrite(pinIN8, HIGH);
}

void handleMqttCommand(const String& commandPayload) {
  String action = jsonExtractString(commandPayload, "action");
  if (action.length() == 0) {
    action = commandPayload;
  }
  action.trim();
  action.toLowerCase();

  // Serial.print("MQTT command payload: ");
  // Serial.println(commandPayload);

  // Perintah Pengendalian Motor Robot (RC Car)
  if (action == "maju") {
    // Serial.println("Robot Action: MAJU");
    stopMotor();
    runMotor1Forward();
    runMotor2Forward();
    runMotor3Forward();
    runMotor4Forward();
    return;
  } else if (action == "mundur") {
    // Serial.println("Robot Action: MUNDUR");
    stopMotor();
    runMotor1Backward();
    runMotor2Backward();
    runMotor3Backward();
    runMotor4Backward();
    return;
  } else if (action == "depankanan") {
    // Serial.println("Robot Action: DEPAN KANAN");
    stopMotor();
    runMotor1Forward();
    runMotor3Forward();
    return;
  } else if (action == "depankiri") {
    // Serial.println("Robot Action: DEPAN KIRI");
    stopMotor();
    runMotor2Forward();
    runMotor4Forward();
    return;
  } else if (action == "belakangkanan") {
    // Serial.println("Robot Action: BELAKANG KANAN");
    stopMotor();
    runMotor1Backward();
    runMotor3Backward();
    return;
  } else if (action == "belakangkiri") {
    // Serial.println("Robot Action: BELAKANG KIRI");
    stopMotor();
    runMotor2Backward();
    runMotor4Backward();
    return;
  } else if (action == "putarkanan") {
    // Serial.println("Robot Action: PUTAR KANAN");
    stopMotor();
    runMotor2Backward();
    runMotor4Backward();
    runMotor1Forward();
    runMotor3Forward();
    return;
  } else if (action == "putarkiri") {
    // Serial.println("Robot Action: PUTAR KIRI");
    stopMotor();
    runMotor2Forward();
    runMotor4Forward();
    runMotor1Backward();
    runMotor3Backward();
    return;
  } else if (action == "geserkanan") {
    // Serial.println("Robot Action: GESER KANAN");
    stopMotor();
    runMotor1Backward();
    runMotor3Forward();
    runMotor2Forward();
    runMotor4Backward();
    return;
  } else if (action == "geserkiri") {
    // Serial.println("Robot Action: GESER KIRI");
    stopMotor();
    runMotor1Forward();
    runMotor3Backward();
    runMotor2Backward();
    runMotor4Forward();
    return;
  } else if (action == "berhenti") {
    // Serial.println("Robot Action: BERHENTI");
    stopMotor();
    return;
  }

  // Perintah Administrasi / Pairing
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

  // Serial.print("MQTT recv: ");
  // Serial.println(msg);

  if (strlen(cfg.mqttTopicCmd) > 0 && strcmp(topic, cfg.mqttTopicCmd) == 0) {
    handleMqttCommand(msg);
    return;
  }

  if (strlen(cfg.mqttTopicPong) > 0 && strcmp(topic, cfg.mqttTopicPong) == 0) {
    msg.toLowerCase();
    // Serial.println("MQTT pong received");
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
  const char* path = strlen(cfg.mqttPath) > 0 ? cfg.mqttPath : mqttBasepathFallback;
  const char* mUser = strlen(cfg.mqttUser) > 0 ? cfg.mqttUser : mqttUser;
  const char* mPass = strlen(cfg.mqttPass) > 0 ? cfg.mqttPass : mqttPass;

  wssClient.setDestination(host, port, path);
  mqttClient.setServer(host, port);
  mqttClient.setCallback(mqttCallback);

  while (!mqttClient.connected()) {
    String clientId = strlen(cfg.mqttClientId) > 0
      ? String(cfg.mqttClientId)
      : "esp8266-" + String(ESP.getChipId(), HEX);

    // Serial.print("Connecting MQTT...");

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
      // Serial.println("connected");
      mqttClient.subscribe(cfg.mqttTopicCmd);
      mqttClient.subscribe(cfg.mqttTopicPong);
      publishMqtt("ping");
      // Serial.println("MQTT send: ping");
      break;
    }

    // Serial.print("failed, rc=");
    // Serial.print(mqttClient.state());
    // Serial.println(" retry in 3 seconds");
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
    // Serial.println("No pair token. Skip activation.");
    return false;
  }

  String activationUrl = strlen(cfg.apiUrl) > 0 ? String(cfg.apiUrl) : String(API_ACTIVATE_URL);
  // Serial.print("Activating device to: ");
  // Serial.println(activationUrl);

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
    // Serial.println("HTTP/HTTPS begin failed");
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

  // Serial.print("Activate status: ");
  // Serial.println(code);
  // Serial.println(body);

  if (code == 401) {
    // Serial.println("Invalid or expired pair token. Resetting config and rebooting to AP mode...");
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
  String mPath = jsonExtractString(body, "path");
  String topicCmd = jsonExtractString(body, "cmd");
  String topicRes = jsonExtractString(body, "res");
  String topicPing = jsonExtractString(body, "ping");
  String topicPong = jsonExtractString(body, "pong");
  long port = jsonExtractNumber(body, "port", mqttPortFallback);

  if (topicCmd.length() == 0 || topicPing.length() == 0 || topicPong.length() == 0) {
    // Serial.println("Activation response missing topics");
    return false;
  }

  memset(cfg.mqttHost, 0, sizeof(cfg.mqttHost));
  memset(cfg.mqttClientId, 0, sizeof(cfg.mqttClientId));
  memset(cfg.mqttUser, 0, sizeof(cfg.mqttUser));
  memset(cfg.mqttPass, 0, sizeof(cfg.mqttPass));
  memset(cfg.mqttPath, 0, sizeof(cfg.mqttPath));
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
  if (mPath.length() > 0) {
    mPath.toCharArray(cfg.mqttPath, sizeof(cfg.mqttPath));
  }

  topicCmd.toCharArray(cfg.mqttTopicCmd, sizeof(cfg.mqttTopicCmd));
  topicRes.toCharArray(cfg.mqttTopicRes, sizeof(cfg.mqttTopicRes));

  topicPing.toCharArray(cfg.mqttTopicPing, sizeof(cfg.mqttTopicPing));
  topicPong.toCharArray(cfg.mqttTopicPong, sizeof(cfg.mqttTopicPong));
  cfg.mqttPort = (uint16_t)port;
  cfg.isPaired = 1;

  saveConfig();
  // Serial.println("Activation success. MQTT contract saved.");
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
  // Serial.println("\nFailed to connect. Starting AP mode...");

  // IP untuk AP mode
  IPAddress apIP(192, 168, 4, 1);
  IPAddress netMsk(255, 255, 255, 0);
  WiFi.softAPConfig(apIP, apIP, netMsk);

  // mulai AP
  WiFi.softAP("MyDevice_Setup");
  // Serial.print("AP IP: ");
  // Serial.println(WiFi.softAPIP());

  // web server routes
  setupWebRoutes();
  server.begin();
  // Serial.println("Web server started");
}

void setup() {
  pinMode(pinIN1, OUTPUT);
  pinMode(pinIN2, OUTPUT);
  pinMode(pinIN3, OUTPUT);
  pinMode(pinIN4, OUTPUT);
  pinMode(pinIN5, OUTPUT);
  pinMode(pinIN6, OUTPUT);
  pinMode(pinIN7, OUTPUT);
  pinMode(pinIN8, OUTPUT);
  stopMotor();

  // Serial.begin(115200);
  delay(500);

  // Serial.println();
  // Serial.println("Booting...");

  // Load config first to read bootCount
  loadConfig();

  // Increment bootCount on startup
  cfg.bootCount++;
  // Serial.print("Boot count: ");
  // Serial.println(cfg.bootCount);

  if (cfg.bootCount >= 5) {
    // Serial.println("5x power cycles detected! Factory resetting...");
    clearConfig();
    saveConfig();
    // After reset, ssid is empty, so it will fall through to startAP()
  } else {
    saveConfig();
  }

  if (strlen(cfg.ssid) > 0) {
    // Serial.print("Trying to connect to saved WiFi: ");
    // Serial.println(cfg.ssid);

    WiFi.mode(WIFI_STA);
    WiFi.disconnect();
    delay(200);
    WiFi.begin(cfg.ssid, cfg.pass);

    int retries = 0;
    while (WiFi.status() != WL_CONNECTED && retries < WIFI_CONNECT_RETRIES) {
      delay(500);
      // Serial.print(".");
      if ((retries + 1) % 10 == 0) {
        // Serial.print(" [");
        // Serial.print(wifiStatusToText(WiFi.status()));
        // Serial.print("]");
      }
      retries++;
    }

    if (WiFi.status() == WL_CONNECTED) {
      // Serial.println("\nConnected to WiFi!");
      // Serial.print("IP Address: ");
      // Serial.println(WiFi.localIP());

      if (!cfg.isPaired && strlen(cfg.pairToken) > 0) {
        activateDevice();
      }

      connectMqtt();

      // tetap aktifkan web server agar bisa reset / save meskipun sudah connect
      setupWebRoutes();
      server.begin();
      return;
    }

    // Serial.println();
    // Serial.print("WiFi connect failed. Last status: ");
    // Serial.println(wifiStatusToText(WiFi.status()));
    // Serial.println("Auto-reconnect is active in background. AP mode will NOT start.");
    
    // Still initialize web server and routes in case it connects in background
    setupWebRoutes();
    server.begin();
    return;
  }

  // Kalau SSID kosong (belum diset / baru direset), masuk AP
  startAP();
}

void loop() {
  server.handleClient(); // selalu layani web server

  // Reset bootCount to 0 after 3 seconds of stable uptime
  if (!bootCountCleared && millis() > 3000) {
    bootCountCleared = true;
    if (cfg.bootCount > 0) {
      cfg.bootCount = 0;
      saveConfig();
      // Serial.println("Normal uptime reached. Boot count reset to 0.");
    }
  }

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
