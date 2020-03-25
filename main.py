import sys, time

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
    t_startTime = time.time_ns()
    time.sleep(0.1)
    t_endTime = time.time_ns()
    t_totalTime = t_endTime - t_startTime
    print("[TIME] Finished in " + str(t_totalTime/1e9) + " s")


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