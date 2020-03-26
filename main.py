import os, sys, time, json

from configparser import ConfigParser

import paho.mqtt.client as mqtt

class Config(ConfigParser):
    def __init__(self, config_file):
        super(Config, self).__init__()

        self.read(config_file)
        self.validate_config()
        self.validate_path()

    def validate_path(self):
        _path = self["LOGGING"]["Path"]
        if not os.path.isdir(_path):
            print("[FILE] Creating new folder at " + _path)
            os.mkdir(_path)

    def validate_config(self):
        required_values = {
            'MQTT': {
                'Host': None,
                'Port': None,
                'Topic': None
            },
            'LOGGING': {
                'Path': None
            }
        }

        for section, keys in required_values.items():
            if section not in self:
                raise Exception(
                    'Missing section %s in the config file' % section)

            for key, values in keys.items():
                if key not in self[section] or self[section][key] == '':
                    raise Exception((
                        'Missing value for %s under section %s in ' +
                        'the config file') % (key, section))

                if values:
                    if self[section][key] not in values:
                        raise Exception((
                            'Invalid value for %s under section %s in ' +
                            'the config file') % (key, section))

# File System
def json_write(file, data):
    _file = file + ".json"
    print("[FILE] Writing JSON file " + _file)
    with open(_file, 'w', newline='\n') as jsonfile:
        json.dump(data, jsonfile, sort_keys=False, indent='\t')

def path_files(dir):
    files = []
    for _filename in os.listdir(dir):
        _filepath = os.path.join(dir, _filename)
        if os.path.isfile(_filepath):
            files.append(_filename)
    return files

def write2file(data, dir):
    _count = str(len(path_files(dir)) + 1).zfill(4)
    _filename = time.strftime("%Y-%m-%d-%H-%M-%S") + "-" + _count
    _file = os.path.join(dir, _filename)
    json_write(_file, data)

# MQTT
def on_connect(client, userdata, flags, rc):
    _topic = userdata["MQTT"]["Topic"]
    print("[MQTT] Connected to broker")
    print("[MQTT] Subscribing to topic " + _topic)
    client.subscribe(_topic, qos=1)

def on_disconnect(client, userdata, rc):
    print("[MQTT] Disconnected from broker")

def on_message(client, userdata, msg):
    _path = userdata["LOGGING"]["Path"]
    print("[MQTT] Received message from topic " + msg.topic)
    t_startTime = time.time_ns()
    
    # Where the magic happens
    try:
        _payload = json.loads(msg.payload)
        write2file(_payload, _path)
    except Exception as e:
        print(e)        

    t_endTime = time.time_ns()
    t_totalTime = t_endTime - t_startTime
    print("[TIME] Finished in " + str(t_totalTime/1e9) + " s")

# Main
def main(cfg):
    _host = cfg["MQTT"]["Host"]
    _port = int(cfg["MQTT"]["Port"])

    client = mqtt.Client(userdata=cfg)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.connect(_host, _port, 60)
    client.loop_forever()

# Main Call
if __name__ == "__main__":
    try:
        cfg = Config('config.ini')
        try:
            main(cfg)
        except KeyboardInterrupt:
            sys.exit(0)
    except Exception as e:
        raise e
