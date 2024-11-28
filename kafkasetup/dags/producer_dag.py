from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import requests
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import logging
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from requests.adapters import HTTPAdapter, Retry
from kafka.errors import TopicAlreadyExistsError
from airflow.utils.dates import days_ago

# Import your config
from config import ApiConfig, KafkaConfig

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

class GTFSKafkaHandler:
    def __init__(self):
        self.producer = None
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    # Your existing methods converted to static methods
    @staticmethod
    def check_kafka():
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KafkaConfig.bootstrap_servers,
                client_id='connection_checker'
            )
            topics = admin_client.list_topics()
            admin_client.close()
            logging.info('Connection to kafka server successful. Topics: %s', topics)
            return True
        except Exception as e:
            logging.error("Failed to connect to kafka: %s", str(e))
            return False

    @staticmethod
    def create_topic(topic_name: str, replication=1, partition=1):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KafkaConfig.bootstrap_servers
            )
            
            list_all_topics = admin.list_topics()
            if topic_name in list_all_topics:
                logging.info(f"{topic_name} is already present")
                admin.close()
                return True

            topic = [NewTopic(name=topic_name, 
                            num_partitions=partition, 
                            replication_factor=replication)]
            admin.create_topics(new_topics=topic, validate_only=False)
            admin.close()
            return True
        except TopicAlreadyExistsError:
            return True
        except Exception as e:
            logging.error(f"Error creating topic: {str(e)}")
            return False

    @staticmethod
    def fetch_data(api_url):
        if not ApiConfig.token:
            logging.critical('API token not found in Config')
            return None

        try:
            session = requests.session()
            retries = Retry(total=4, backoff_factor=0.1, 
                          status_forcelist=[500, 502, 503, 504])
            session.mount("https://", HTTPAdapter(max_retries=retries))

            response = session.get(api_url, headers={
                "Cache-Control": "no-cache",
                "Ocp-Apim-Subscription-Key": ApiConfig.token
            })

            if response.status_code == 200:
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(response.content)
                feedDict = MessageToDict(feed)

                if feedDict.get('entity'):
                    return feedDict.get('entity')
                return {"Updates": 'No train services are live',
                       "Time": str(datetime.now())}
            else:
                logging.critical(f'Failed to receive response: {response.status_code}')
                return None
        except Exception as e:
            logging.error(f'Exception found: {str(e)}')
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
            logging.error(f"Failed to create producer: {str(e)}")
            return False

    def send_message(self, topic, message):
        try:
            if not self.producer:
                if not self.initialize_producer():
                    return "Failed to initialize producer"

            future = self.producer.send(topic, message)
            record_metadata = future.get(timeout=10)
            return f"Message sent successfully to topic {topic}"
        except Exception as e:
            return f"Failed to send message: {str(e)}"

def process_gtfs_data():
    """
    Main processing function for Airflow task
    """
    handler = GTFSKafkaHandler()
    
    if not handler.check_kafka():
        raise Exception("Kafka connection failed")

    topic_name = "Live-Melb-GTFS-Vehicle"
    if not handler.create_topic(topic_name=topic_name):
        raise Exception("Topic creation/verification failed")

    message = handler.fetch_data(ApiConfig.url)
    if message:
        result = handler.send_message(topic_name, message)
        logging.info('Published message: %s', result)
        if handler.producer:
            handler.producer.close()
        return result
    else:
        raise Exception("No data fetched from API")

# Create the DAG
weekday_dag = DAG(
    'gtfs_kafka_producer',
    default_args=default_args,
    description='DAG for GTFS Kafka Producer',
    schedule_interval=timedelta(seconds=30),  # Match your original 30-second interval
    catchup=False,
    schedule=None
)



# Create the task
weekday_dag = DAG(
    'weekday_schedule',
    default_args=default_args,
    description='Weekday schedule - every 2 minutes from 5 AM to 11 PM',
    schedule_interval='*/2 5-23 * * 1-5',  # Every 2 minutes, 5 AM to 11 PM, Mon-Fri
    start_date=days_ago(1),
    catchup=False
)

weekday_task = PythonOperator(
    task_id='weekday_task',
    python_callable=process_gtfs_data,
    dag=weekday_dag
)

# Weekend DAG
weekend_dag = DAG(
    'weekend_schedule',
    default_args=default_args,
    description='Weekend schedule - every 2 minutes all day',
    schedule_interval='*/5 * * * 0,6',  # Every 5 minutes, all day, Sat-Sun
    start_date=days_ago(1),
    catchup=False
)

weekend_task = PythonOperator(
    task_id='weekend_task',
    python_callable=process_gtfs_data,
    dag=weekend_dag
)