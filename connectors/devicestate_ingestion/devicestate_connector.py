import time
import sys
import signal
import paho.mqtt.client as mqttclient
import psycopg2
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


class PostgresDB:
    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="admin",
                host="localhost",
                port=5432
            )
            logger.info("DB connection successful")
        except Exception as e:
            logger.error(f"DB connection failed: {e}")
            sys.exit(1)

    def insert_metrics(self, topic_parts, timestamp, state_key, state):
        message_type = topic_parts[2]
        node_id = topic_parts[3]
        device_id = topic_parts[4] if len(topic_parts) > 4 else None

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO device_states(time, node_id, device_id, message_type, state_key, state)
                       VALUES (to_timestamp(%s), %s, %s, %s, %s, %s)
                       ON CONFLICT (time, node_id, message_type) DO NOTHING""",
                    (timestamp, node_id, device_id, message_type, state_key, state)
                )
                self.conn.commit()
        except Exception as e:
            logger.error(f"DB insert failed: {e}")


class MQTTConnector:
    def __init__(self):
        self.db = PostgresDB()
        self.client = mqttclient.Client(mqttclient.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # Set flag for clean shutdown
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        logger.info(f"Received shutdown signal {signum}")
        self.running = False
        self.client.disconnect()

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("MQTT connected")
            client.subscribe([
                ("spBv1.0/+/DBIRTH/#", 0),
                ("spBv1.0/+/DDEATH/#", 0),
                ("spBv1.0/+/NBIRTH/#", 0),
                ("spBv1.0/+/NDEATH/#", 0),
                ("spBv1.0/+/STATE/#", 0)
            ])

    def on_message(self, client, userdata, msg):
        try:
            topic_parts = msg.topic.split('/')
            payload = json.loads(msg.payload.decode())
            # Payload timestamp
            pl_timestamp = payload.get("timestamp", time.time())

            if "status" in payload:
                state_key, state = next(iter(payload["status"].items()))
                self.db.insert_metrics(topic_parts, pl_timestamp, state_key, state)

        except Exception as e:
            logger.error(f"Message processing error: {e}")

    def run(self):
        self.client.connect("localhost", 1883)

        # MQTT loop start
        self.client.loop_start()

        # Minimal main loop
        while self.running:
            time.sleep(1)

        self.client.loop_stop()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    MQTTConnector().run()