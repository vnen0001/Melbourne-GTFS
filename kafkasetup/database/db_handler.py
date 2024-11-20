import psycopg2
from typing import Dict,Any,Tuple
class DatabaseHandler():
    def __init__(self,config: Dict[str,Any]):
        self.config = config

    def test_connection(self) -> Tuple[bool, str]:
        """Test the database connection and return status and version"""
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()[0]
                    return True, version
        except Exception as e:
            return False, str(e)

    def connect(self):
        return psycopg2.connect(
            host=self.config['databse_url'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )


        
