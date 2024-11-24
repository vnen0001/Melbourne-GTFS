from dataclasses import dataclass
from dotenv import load_dotenv
import os

load_dotenv()
@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.environ.get('bootstrap-server')
    topic_name:str = "melbgtfs"

@dataclass
class DatabaseConfig:
    databse_url: str = os.environ.get('database-server')
    port: str = '5432'
    database: str =os.environ.get('databse-name')
    user: str = os.environ.get('databse-user')
    password: str = os.environ.get('databse-password')

@dataclass
class ApiConfig:
    token:str = os.environ.get('api-token')
    url:str = os.environ.get('api-url')