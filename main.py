import os, sys, time, json

import paho.mqtt.client as mqtt

MQTT_HOST = "localhost"
MQTT_PORT = 1883 
MQTT_TOPIC = "Test_Log1"

LOG_PATH = "C:/Users/batman/Desktop/log_files/Test_Log1"

# File System
def json_write(file, data):
    _file = file + ".json"
    print("[FILE] Writing JSON file " + _file)
    with open(_file, 'w', newline='\n') as jsonfile:
        json.dump(data, jsonfile, sort_keys=False, indent='\t')

def logpath_files():
    files = []
    for _filename in os.listdir(LOG_PATH):
        _filepath = os.path.join(LOG_PATH, _filename)
        if os.path.isfile(_filepath):
            files.append(_filename)
    return files

def write2file(data):
    _count = str(len(logpath_files()) + 1).zfill(4)
    _filename = time.strftime("%Y-%m-%d-%H-%M-%S") + "-" + _count
    _path = os.path.join(LOG_PATH, _filename)
    json_write(_path, data)

# MQTT
def on_connect(client, userdata, flags, rc):
    print("[MQTT] Connected to broker")
    print("[MQTT] Subscribing to topic " + MQTT_TOPIC)
    client.subscribe(MQTT_TOPIC)

def on_disconnect(client, userdata, rc):
    print("[MQTT] Disconnected from broker")

def on_message(client, userdata, msg):
    print("[MQTT] Received message from topic " + msg.topic)
    t_startTime = time.time_ns()
    
    # Where the magic happens
    try:
        _payload = json.loads(msg.payload)
        write2file(_payload)
    except Exception as e:
        print(e)        

    t_endTime = time.time_ns()
    t_totalTime = t_endTime - t_startTime
    print("[TIME] Finished in " + str(t_totalTime/1e9) + " s")

# Main
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()

# Main Call
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)