import os
import threading

import snowflake.connector
from dotenv import dotenv_values
from loguru import logger

# Get absolute path to project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Load .env from project root
config = dotenv_values(os.path.join(PROJECT_ROOT, ".env"))

# Thread-local storage so each thread gets its own Snowflake connection. Global conn.cursor() can cause failures.
_thread_local = threading.local()

def get_snowflake_connection():
    """Return a Snowflake connection scoped to the current thread."""
    conn = getattr(_thread_local, "connection", None)
    if conn is None or conn.is_closed():
        conn = snowflake.connector.connect(
            user=config["SNOWFLAKE_USER"],
            password=config["SNOWFLAKE_PASSWORD"],
            account=config["SNOWFLAKE_ACCOUNT"],
            warehouse=config["SNOWFLAKE_WAREHOUSE"],
            database=config["SNOWFLAKE_DATABASE"],
            schema=config["SNOWFLAKE_SCHEMA"],
        )
        _thread_local.connection = conn
    return conn

def main():
    logger.info("Starting pipeline...")

if __name__ == "__main__":
    main()
