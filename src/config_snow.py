
import threading

import boto3
import snowflake.connector
from dotenv import dotenv_values

# Load .env from project root
config = dotenv_values(".env")

# Thread-local storage so each thread gets its own Snowflake connection. Global conn.cursor() can cause failures.
_thread_local = threading.local()

# Initialize AWS session and S3 client once at the module level
def initialize_aws():
    aws_session = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])
    aws_s3_client = aws_session.client('s3')
    return aws_session, aws_s3_client

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
