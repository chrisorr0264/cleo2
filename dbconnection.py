import psycopg2
from psycopg2 import pool, sql
from settings import *
from logger_config import get_logger

# Initialize logger
logger = get_logger(__name__)

# Database connection parameters
db_params = {
    'dbname': DB_NAME,
    'user': DB_USERNAME,
    'password': DB_PASSWORD,
    'host': DB_SERVER,
    'port': DB_PORT       
}

try:
    # Create a connection pool
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **db_params)

    if db_pool:
        logger.detail("Connection pool created successfully", extra={'class_name': 'dbconnection', 'function_name': 'dbconnection'})
except Exception as e:
    logger.error(f"Error while connecting to PostgreSQL: {e}", extra={'class_name': 'dbconnection', 'function_name': 'dbconnection'})

# Function to get a connection from the pool
def get_connection():
    try:
        conn = db_pool.getconn()
        if conn:
            logger.detail("Successfully received a connection from the connection pool", extra={'class_name': 'dbconnection', 'function_name': 'dbconnection'})
            return conn
    except Exception as e:
        logger.error(f"Error while getting connection: {e}", extra={'class_name': 'dbconnection', 'function_name': 'dbconnection'})

# Function to return a connection to the pool
def return_connection(conn):
    try:
        db_pool.putconn(conn)
        logger.detail("Connection returned to the pool successfully", extra={'class_name': 'dbconnection', 'function_name': 'dbconnection'})
    except Exception as e:
        logger.error(f"Error while returning connection: {e}", extra={'class_name': 'dbconnection', 'function_name': 'dbconnection'})

# Function to close all pool connections
def close_pool():
    try:
        db_pool.closeall()
        logger.detail("Connection pool closed successfully", extra={'class_name': 'dbconnection', 'function_name': 'dbconnection'})
    except Exception as e:
        logger.error(f"Error while closing connection pool: {e}", extra={'class_name': 'dbconnection', 'function_name': 'dbconnection'})
