import json
import requests
from kafka import KafkaProducer, KafkaClient
from kafka.admin import NewTopic, KafkaAdminClient
import os
import sys
import time
from datetime import datetime
import logging
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import ParseDict
from requests.adapters import HTTPAdapter, Retry
from kafka.errors import TopicAlreadyExistsError

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafkasetup.config.config import ApiConfig, KafkaConfig


class GTFSKafkaProducer:
    def __init__(self):  # 30 seconds default interval
        self.last_processed_time = None
        self.producer = None
        self.check_interval = 30  # Interval in seconds
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def check_kafka(self) -> bool:
        """
        Check if Kafka is accessible using KafkaAdminClient
        """
        if not KafkaConfig.bootstrap_servers:
            self.logger.critical('Kafka Bootstrap Server configuration is not found. Please check Kafka Config')
            return False

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KafkaConfig.bootstrap_servers,
                client_id='connection_checker'
            )
            topics = admin_client.list_topics()
            admin_client.close()
            self.logger.info('Connection to kafka server successful. Available topics: %s', topics)
            return True
        except Exception as e:
            self.logger.error("Failed to connect to kafka: %s", str(e))
            return False

    def create_topic(self, topic_name: str, replication=1, partition=1):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KafkaConfig.bootstrap_servers
            )

            list_all_topics = admin.list_topics()
            if topic_name in list_all_topics:
                self.logger.info(f"{topic_name} is already present")
                admin.close()
                return True

            topic = []
            topic.append(NewTopic(name=topic_name, num_partitions=partition, replication_factor=replication))

            self.logger.info(f"Created new: {topic_name}")
            admin.create_topics(new_topics=topic, validate_only=False)
            admin.close()
            return True
        except TopicAlreadyExistsError:
            self.logger.error('Topic already exists')
            return True
        except Exception as e:
            self.logger.error(f"Error creating topic: {str(e)}")
            return False

    def fetch_data(self, api_url):
        if not ApiConfig.token:
            self.logger.critical('API token not found in Config')
            return None

        try:
            session = requests.session()
            retries = Retry(total=4, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
            session.mount("https://", HTTPAdapter(max_retries=retries))

            response = session.get(api_url, headers={
                "Cache-Control": "no-cache",
                "Ocp-Apim-Subscription-Key": ApiConfig.token
            })

            if response.status_code == 200:
                self.logger.info(f'Response received: {response.status_code}')
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(response.content)
                feedDict = MessageToDict(feed)

                if feedDict.get('entity'):
                    self.logger.info('Live updates found')
                    return feedDict.get('entity')
                else:
                    self.logger.info('No live updates found')

                    return {"Updates": 'No train services are live',
                            "Time": str(datetime.now())}
            else:
                self.logger.critical(f'Failed to receive response: {response.status_code}')
                return None
        except Exception as e:
            self.logger.error(f'Exception found: {str(e)}')
            return None

    def initialize_producer(self):
        try:
            if not self.producer:
                self.producer = KafkaProducer(
                    bootstrap_servers=KafkaConfig.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all'
                )
            return True
        except Exception as e:
            self.logger.error(f"Failed to create producer: {str(e)}")
            return False

    def send_message(self, topic, message):
        try:
            if not self.producer:
                if not self.initialize_producer():
                    return "Failed to initialize producer"

            future = self.producer.send(topic, message)
            record_metadata = future.get(timeout=10)
            return f"Message sent successfully to topic {topic} at partition {record_metadata.partition}, offset {record_metadata.offset}"
        except Exception as e:
            return f"Failed to send message: {str(e)}"

    def process_data(self):
        """
        Main processing function
        """
        self.logger.info("Starting data processing cycle...")

        if not self.check_kafka():
            self.logger.error("Kafka connection failed")
            return

        topic_name = "Live-Melb-GTFS-Vehicle"
        if not self.create_topic(topic_name=topic_name):
            self.logger.error("Topic creation/verification failed")
            return

        message = self.fetch_data(ApiConfig.url)
        if message:
            result = self.send_message(topic_name, message)
            self.logger.info('Published message: %s', result)
            self.last_processed_time = datetime.now()
        else:
            self.logger.info("No new data to process")

    def start(self):
        """
        Start continuous processing
        """
        self.logger.info(f"Starting GTFS data producer")

        try:
            while True:
                self.process_data()
                time.sleep(self.check_interval)  # Wait for the defined interval
        except KeyboardInterrupt:
            self.logger.info("Shutting down producer...")
            if self.producer:
                self.producer.close()
            self.logger.info("Producer shutdown complete")


if __name__ == "__main__":
    producer = GTFSKafkaProducer()
    producer.start()
