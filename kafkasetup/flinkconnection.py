from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common.restart_strategy import RestartStrategies

from pyflink.table.expressions import col
from datetime import datetime
import logging
import os
from config.config import KafkaConfig, DatabaseConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GTFSStreamProcessor:
    def __init__(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.enable_checkpointing(60000)  # Enable checkpointing every 60 seconds
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        self.t_env = StreamTableEnvironment.create(self.env, settings)
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        kafka_jar = os.path.join(current_dir, "flink-connector-kafka-1.20.0.jar")
        jdbc_jar = os.path.join(current_dir, "flink-connector-jdbc-3.20-1.19.jar")
        postgres_jar = os.path.join(current_dir, "postgresql-42.7.4.jar")
    
        self.t_env.get_config().get_configuration().set_string(
        "pipeline.classpaths", 
        f"file://{kafka_jar};file://{jdbc_jar};file://{postgres_jar}"
        )
        # Set the JAR files in the configuration
        # self.t_env.get_config().get_configuration().set_string(
        #     "pipeline.classpaths", 
        #     f"file://{jdbc_jar};file://{postgres_jar}"
        # )
        # Set up parallelism and restart strategy
        # self.env.set_parallelism(3)  # Adjust based on your needs
        self.env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10000))  # 3 retries, 10 second delay
        
    def create_source_table(self) ->None:
        source_ddl = f"""
        CREATE TABLE vehicle_position (
            payload STRING,
            proctime AS PROCTIME()  -- Adding processing time
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'Live-Melb-GTFS-Vehicle',
            'properties.bootstrap.servers' = '{KafkaConfig.bootstrap_servers}',
            'properties.group.id' = 'flink_consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'raw'
        )
        """
        self.t_env.execute_sql(source_ddl)
        
    def create_processed_sink(self):
        """Create sink table for processed data"""
        sink_ddl = f"""
        CREATE TABLE processed_positions (
            train_id STRING ,
            latitude DOUBLE,
            longitude DOUBLE,
            bearing DOUBLE,
            `timestamp` BIGINT,
            trip_id STRING,
            start_time STRING,
            processing_time TIMESTAMP(3),
            CONSTRAINT pk PRIMARY KEY (train_id) NOT ENFORCED
            
        ) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://{DatabaseConfig.databse_url}:{DatabaseConfig.port}/{DatabaseConfig.database}',
    'table-name' = 'live_vehicle_positions',
    'username' = '{DatabaseConfig.user}',
    'password' = '{DatabaseConfig.password}',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '10',
        'sink.buffer-flush.interval' = '500ms',
        'sink.max-retries' = '3'
)
        """
        self.t_env.execute_sql(sink_ddl)
    def create_parsed_view(self):
        """Create view for parsed vehicle data with error handling"""
        parsed_view = """
        CREATE VIEW parsed_vehicles AS
        SELECT
            JSON_VALUE(payload, '$.content[*].id') as record_id,
            JSON_VALUE(payload, '$.content[*].vehicle.vehicle.id') as train_id,
            JSON_VALUE(payload, '$.content[*].vehicle.trip.tripId') as trip_id,
            CAST(JSON_VALUE(payload, '$.content[*].vehicle.position.latitude') AS DOUBLE) as latitude,
            CAST(JSON_VALUE(payload, '$.content[*].vehicle.position.longitude') AS DOUBLE) as longitude,
            CAST(JSON_VALUE(payload, '$.content[*].vehicle.position.bearing') AS DOUBLE) as bearing,
            CAST(JSON_VALUE(payload, '$.content[*].vehicle.timestamp') AS BIGINT) as `timestamp`,
            JSON_VALUE(payload, '$.content[*].vehicle.trip.startTime') as start_time,
            JSON_VALUE(payload, '$.content[*].vehicle.trip.startDate') as start_date,
            proctime
        FROM vehicle_position
        WHERE payload IS NOT NULL
        """
        self.t_env.execute_sql(parsed_view)
    def process_sink(self):
        """Process and sink the stream with error handling"""
        insert_sql = """
        INSERT INTO processed_positions
        SELECT 
            train_id,
            ROUND(latitude, 6) as latitude,
            ROUND(longitude, 6) as longitude,
            ROUND(bearing, 2) as bearing,
            `timestamp`,
            trip_id,
            start_time,
            CAST(proctime AS TIMESTAMP(3)) as processing_time
        FROM parsed_vehicles
        WHERE train_id IS NOT NULL
            AND latitude IS NOT NULL 
            AND longitude IS NOT NULL
        """
        self.t_env.execute_sql(insert_sql)
        print(self.t_env.execute_sql(insert_sql))

    # def create_basic_postgres_table(self) -> str:
    #     """Create the PostgreSQL table if it doesn't exist"""
    #     create_table_sql = """
    #     CREATE TABLE IF NOT EXISTS live_vehicle_positions (
    #         train_id VARCHAR(50),
    #         latitude DOUBLE PRECISION,
    #         longitude DOUBLE PRECISION,
    #         bearing DOUBLE PRECISION,
    #         `timestamp` BIGINT,
    #         trip_id VARCHAR(100),
    #         start_time VARCHAR(50),
    #         processing_time TIMESTAMP,
    #         PRIMARY KEY (train_id, `timestamp`)
    #     );

    #     CREATE INDEX IF NOT EXISTS idx_timestamp ON live_vehicle_positions(`timestamp`);
    #     CREATE INDEX IF NOT EXISTS idx_train_id ON live_vehicle_positions(train_id);
    #     """
    #     return create_table_sql

def main():
    try:
        processor = GTFSStreamProcessor()
        
        # Set up tables and views
        processor.create_source_table()
        processor.create_processed_sink()
        processor.create_parsed_view()
        
        logger.info("Starting GTFS to PostgreSQL sink...")
        processor.process_sink()
        
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        raise

if __name__ == '__main__':
    main()