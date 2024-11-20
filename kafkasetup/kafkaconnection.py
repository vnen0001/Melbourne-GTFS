from config.config import DatabaseConfig, KafkaConfig
from database.db_handler import DatabaseHandler

def main():
    kafka_config= KafkaConfig()
    db_config = DatabaseConfig()
    def check_database_connection(db_handler: DatabaseHandler) -> bool:
        """
        Simple database connection check
        Returns True if connection successful, False otherwise
        """
        success, message = db_handler.test_connection()
        if success:
            print(f"Database connected successfully! Version: {message}")
            return True
        else:
            print(f"Database connection failed: {message}")
            return False
    
    db_handler = DatabaseHandler(db_config.__dict__)
    print(db_handler)
    print(check_database_connection(db_handler))


if __name__ == "__main__":
    main()
