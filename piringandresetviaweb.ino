#include <ESP8266WiFi.h>
#include <ESP8266WebServer.h>
#include <EEPROM.h>
#include <PubSubClient.h>

#define EEPROM_SIZE 96   // 32 byte SSID + 64 byte PASS

ESP8266WebServer server(80);
WiFiClient espClient;
PubSubClient mqttClient(espClient);

const char* mqttHost = "faizabdul.xyz";
const uint16_t mqttPort = 1883;
const char* mqttUser = "robot";
const char* mqttPass = "bismillah";
const char* mqttTopicPing = "testTopic/ping";
const char* mqttTopicPong = "testTopic/pong";

unsigned long lastPingMs = 0;

void publishMqtt(const char* payload) {
  if (!mqttClient.connected()) {
    return;
  }
  mqttClient.publish(mqttTopicPing, payload);
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  if (strcmp(topic, mqttTopicPong) != 0) {
    return;
  }

  String msg;
  for (unsigned int i = 0; i < length; i++) {
    msg += (char)payload[i];
  }
  msg.trim();
  msg.toLowerCase();

  Serial.print("MQTT recv: ");
  Serial.println(msg);
}

void connectMqtt() {
  if (WiFi.status() != WL_CONNECTED) {
    return;
  }

  while (!mqttClient.connected()) {
    String clientId = "esp8266-" + String(ESP.getChipId(), HEX);
    Serial.print("Connecting MQTT...");

    bool ok = mqttClient.connect(
      clientId.c_str(),
      mqttUser,
      mqttPass,
      mqttTopicPing,
      1,
      true,
      "berhenti"
    );

    if (ok) {
      Serial.println("connected");
      mqttClient.subscribe(mqttTopicPong);
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
}

void saveCredentials(const char* ssid, const char* pass) {
  EEPROM.begin(EEPROM_SIZE);
  for (int i = 0; i < 32; i++) {
    EEPROM.write(i, i < strlen(ssid) ? ssid[i] : 0);
  }
  for (int i = 0; i < 64; i++) {
    EEPROM.write(32 + i, i < strlen(pass) ? pass[i] : 0);
  }
  EEPROM.commit();
}

void loadCredentials(char* ssid, char* pass) {
  EEPROM.begin(EEPROM_SIZE);
  for (int i = 0; i < 32; i++) {
    ssid[i] = char(EEPROM.read(i));
  }
  ssid[31] = '\0';
  for (int i = 0; i < 64; i++) {
    pass[i] = char(EEPROM.read(32 + i));
  }
  pass[63] = '\0';
}

void clearCredentials() {
  EEPROM.begin(EEPROM_SIZE);
  for (int i = 0; i < EEPROM_SIZE; i++) {
    EEPROM.write(i, 0);
  }
  EEPROM.commit();
}

void handleRoot() {
  String html = "<h2>WiFi Setup</h2>"
                "<form method='POST' action='/save'>"
                "SSID: <input name='ssid'><br>"
                "Password: <input name='pass' type='password'><br><br>"
                "<input type='submit' value='Save WiFi'>"
                "</form>"
                "<br><br>"
                "<a href='/reset'>🔄 Reset WiFi Settings</a>";
  server.send(200, "text/html", html);
}

void handleSave() {
  String ssid = server.arg("ssid");
  String pass = server.arg("pass");

  server.send(200, "text/plain", "Saved! Rebooting...");

  saveCredentials(ssid.c_str(), pass.c_str());
  delay(1000);
  ESP.restart();
}

void handleReset() {
  clearCredentials();
  server.send(200, "text/plain", "WiFi credentials cleared! Rebooting...");
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
  WiFi.softAP("MyDevice_Setup", "12345678");
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

  char ssid[32];
  char pass[64];
  loadCredentials(ssid, pass);

  if (strlen(ssid) > 0) {
    Serial.print("Trying to connect to saved WiFi: ");
    Serial.println(ssid);

    WiFi.begin(ssid, pass);
    int retries = 0;
    while (WiFi.status() != WL_CONNECTED && retries < 20) {
      delay(500);
      Serial.print(".");
      retries++;
    }

    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("\nConnected to WiFi!");
      Serial.print("IP Address: ");
      Serial.println(WiFi.localIP());

      mqttClient.setServer(mqttHost, mqttPort);
      mqttClient.setCallback(mqttCallback);
      connectMqtt();

      // tetap aktifkan web server agar bisa reset / save meskipun sudah connect
      setupWebRoutes();
      server.begin();
      return;
    }
  }

  // kalau gagal connect, masuk AP
  startAP();
}

void loop() {
  server.handleClient(); // selalu layani web server

  if (WiFi.status() == WL_CONNECTED) {
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
