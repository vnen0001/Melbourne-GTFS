from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance()\
        .in_streaming_mode()\
        .build()
    t_env = StreamTableEnvironment.create(env, settings)

    source_ddl = """
        CREATE TABLE vehicle_position (
            payload STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'Live-Melb-GTFS-Vehicle',
            'properties.bootstrap.servers' = '35.208.38.25:9092',
            'properties.group.id' = 'flink_consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'raw'
        )
    """
    t_env.execute_sql(source_ddl)

    parsed_view = """
        CREATE VIEW parsed_vehicles AS
        SELECT 
            JSON_VALUE(payload, '$[0].id') as record_id,
            JSON_VALUE(payload, '$[0].vehicle.vehicle.id') as train_id,
            JSON_VALUE(payload, '$[0].vehicle.trip.tripId') as trip_id,
            CAST(JSON_VALUE(payload, '$[0].vehicle.position.latitude') AS DOUBLE) as latitude,
            CAST(JSON_VALUE(payload, '$[0].vehicle.position.longitude') AS DOUBLE) as longitude,
            CAST(JSON_VALUE(payload, '$[0].vehicle.position.bearing') AS DOUBLE) as bearing,
            JSON_VALUE(payload, '$[0].vehicle.timestamp') as `timestamp`,
            JSON_VALUE(payload, '$[0].vehicle.trip.startTime') as start_time,
            JSON_VALUE(payload, '$[0].vehicle.trip.startDate') as start_date
        FROM vehicle_position
        WHERE payload IS NOT NULL
    """
    t_env.execute_sql(parsed_view)

    vehicle_query = """
        SELECT 
            train_id,
            ROUND(latitude, 4) as lat,
            ROUND(longitude, 4) as lon,
            ROUND(bearing, 2) as bearing,
            `timestamp`,
            trip_id,
            start_time
        FROM parsed_vehicles
        WHERE train_id IS NOT NULL
        LIMIT 5
    """

    print("\nQuerying vehicle positions:")
    try:
        result = t_env.execute_sql(vehicle_query)
        with result.collect() as results:
            for row in results:
                print(f"Train: {row[0]}")
                print(f"Location: ({row[1]}, {row[2]})")
                print(f"Bearing: {row[3]}°")
                print(f"Timestamp: {row[4]}")
                print(f"Trip: {row[5]}")
                print(f"Start Time: {row[6]}")
                print("---")
    except Exception as e:
        print(f"Error executing query: {str(e)}")

    debug_query = """
        SELECT 
            payload,
            CAST(CHAR_LENGTH(payload) AS INT) as msg_length
        FROM vehicle_position
        LIMIT 1
    """
    
    print("\nDebug raw message:")
    try:
        debug_result = t_env.execute_sql(debug_query)
        with debug_result.collect() as results:
            for row in results:
                message = row[0]
                print(f"Message length: {row[1]} characters")
                print("First 200 characters:")
                print(message[:200])
    except Exception as e:
        print(f"Debug query error: {str(e)}")

if __name__ == '__main__':
    main()