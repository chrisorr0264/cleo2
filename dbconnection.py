import psycopg2
from psycopg2 import pool
import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger('main.dbconnection')

load_dotenv()

class DBConnection:
    _instance = None
    db_pool = None

    @staticmethod
    def get_instance():
        if DBConnection._instance is None:
            DBConnection()
        return DBConnection._instance

    def __init__(self):
        if DBConnection._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            DBConnection._instance = self
            self.initialize_pool()

    def initialize_pool(self):
        try:
            self.db_pool = pool.SimpleConnectionPool(
                1,  # minconn
                20, # maxconn
                user=os.getenv('DB_USERNAME'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_SERVER'),
                port=os.getenv('DB_PORT'),
                database=os.getenv('DB_NAME')
            )
            if self.db_pool:
                logger.info("Connection pool created successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error while connecting to PostgreSQL: {error}")

    def get_connection(self):
        try:
            if self.db_pool:
                connection = self.db_pool.getconn()
                if connection:
                    logger.debug("Successfully received a connection from the connection pool")
                    return connection
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error while getting connection: {error}")
        return None

    def return_connection(self, connection):
        try:
            if self.db_pool:
                self.db_pool.putconn(connection)
                logger.debug("Connection returned to the pool successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error while returning connection: {error}")

    def close_pool(self):
        try:
            if self.db_pool:
                self.db_pool.closeall()
                logger.info("Connection pool closed successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error while closing connection pool: {error}")
