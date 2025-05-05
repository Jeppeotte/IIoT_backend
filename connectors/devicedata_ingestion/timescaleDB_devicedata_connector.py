import sys
import yaml
import paho.mqtt.client as mqttclient
import psycopg2
from pathlib import Path
from dataclasses import dataclass
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

@dataclass()
class MQTTConfig:
    broker: str
    port: int

@dataclass()
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    dbname: str

@dataclass()
class Config:
    mqtt: MQTTConfig
    timescaleDB: DatabaseConfig
    postgreSQL: DatabaseConfig

#Directory for running locally
#local_dir = "/home/jeppe/projectfolder"
#mounted_dir = Path(local_dir)
#Directory for docker container
mounted_dir = Path("/mounted_dir")


class ConfigReader:
    def __init__(self):
        self.config_path = mounted_dir.joinpath("system_conf/system_configuration.yaml")
        self.load()

    def load(self):
        # Check if the config file actually exist
        if not self.config_path.exists():
            logger.error(f"Config file not found: {self.config_path}")
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path,'r') as f:
            config_file = yaml.safe_load(f)

        self.config = Config(**config_file)


    def timescaleDB(self) -> DatabaseConfig:
        return self.config.timescaleDB

    def postgreSQL(self) -> DatabaseConfig:
        return self.config.postgreSQL

    def mqtt(self) -> MQTTConfig:
        return self.config.mqtt

# Database setup
class TimescaleDB:
    def __init__(self):
        self.configs = ConfigReader()
        self.conn = None
        self.connect()

    def connect(self):
        #Establish connection to timescaleDB
        try:
            self.conn = psycopg2.connect(**self.configs.timescaleDB())
            logger.info("Succesfully connected to TimescaleDB")

        except Exception as e:
            logger.error(f" DB connection error: {e}")

    def insert_metrics(self,group_id,device_id,sensor_id,value,timestamp):
        # Insert received metrics from MQTT broker
        try:
            with self.conn.cursor() as cursor:
                query = f"""
                    INSERT INTO {group_id}(time, device_id, sensor_id, metric_value)
                    VALUES (to_timestamp(%s), %s, %s, %s)
                    ON CONFLICT (time, device_id, sensor_id) DO NOTHING;
                """
                cursor.execute(query,(timestamp, device_id, sensor_id,
                                      json.dumps(value)))
                self.conn.commit()
        except psycopg2.OperationalError:
            logger.error("Lost connection to DB")

class MQTTConnector:
    def __init__(self):
        self.configs = ConfigReader()
        self.db = TimescaleDB()
        self.client = mqttclient.Client(mqttclient.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("Connected to MQTT broker")
            #Subscribe to all group topics and all difference devices within that group
            client.subscribe("spBv1.0/+/DDATA/#")


    def on_message(self, client, userdata, msg):
        try:
            # Get the information of the device from the topic
            topic_parts = msg.topic.split('/')
            group_id = topic_parts[1]
            device_id = topic_parts[4]
            payload = json.loads(msg.payload.decode())

            for metric in payload["metrics"]:
                self.db.insert_metrics(
                    group_id,
                    device_id,
                    metric['name'],
                    metric['value'],
                    metric['timestamp']
                )
        except Exception as e:
            logger.error(f"Error with processing the messages: {e}")

    def run(self):
        broker = self.configs.mqtt()['broker']
        port = self.configs.mqtt()['port']
        self.client.connect(broker, port)
        self.client.loop_forever()

if __name__ == "__main__":
    connector = MQTTConnector()
    connector.run()



