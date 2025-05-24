import sys
import signal
import paho.mqtt.client as mqttclient
import psycopg2
from pathlib import Path
import json
import logging
import atexit
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

#Directory for docker container
mounted_dir = Path("/mounted_dir")

# Database setup
class TimescaleDB:
    def __init__(self):
        self.conn = None
        self.connect()
        atexit.register(self.cleanup)  # Register cleanup on exit

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname="devicedata",
                user="postgres",
                password="admin",
                host="localhost",
                port=5433
            )
            logger.info("Successfully connected to TimescaleDB")
        except Exception as e:
            logger.error(f"DB connection error: {e}")

    def cleanup(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Closed database connection")

    # [Rest of TimescaleDB methods remain the same...]


class MQTTConnector:
    def __init__(self):
        self.should_exit = False
        self.db = TimescaleDB()
        self.client = mqttclient.Client(mqttclient.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # Register signal handlers
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)

    def handle_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.should_exit = True
        self.client.disconnect()

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("Connected to MQTT broker")
            client.subscribe("spBv1.0/+/DDATA/#")

    def on_message(self, client, userdata, msg):
        try:
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
            logger.error(f"Error processing message: {e}")

    def run(self):
        self.client.connect("localhost", 1883)

        # Start mqtt loop
        self.client.loop_start()

        # Main loop that can be interrupted
        while not self.should_exit:
            time.sleep(1)

        # Cleanup
        self.client.loop_stop()
        logger.info("MQTT client stopped")


if __name__ == "__main__":
    connector = MQTTConnector()
    connector.run()