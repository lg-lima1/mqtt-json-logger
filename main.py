import sys

import paho.mqtt.client as mqtt

MQTT_HOST = "localhost"
MQTT_PORT = 1883 
MQTT_TOPIC = "Test_Log1"

def on_connect(client, userdata, flags, rc):
    print("[MQTT] Connected to broker")
    print("[MQTT] Subscribing to topic " + MQTT_TOPIC)
    client.subscribe(MQTT_TOPIC)

def on_disconnect(client, userdata, rc):
    print("[MQTT] Disconnected from broker")

def on_message(client, userdata, msg):
    print("[MQTT] Received message from topic " + msg.topic)
    print("[MQTT]     Payload: " + str(msg.payload))

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.connect(MQTT_HOST, MQTT_PORT, 60)

    client.loop_forever()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)