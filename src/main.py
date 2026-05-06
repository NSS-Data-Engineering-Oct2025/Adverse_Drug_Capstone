import threading

import boto3
import asyncio
import snowflake.connector
from dotenv import dotenv_values
from loguru import logger

import ingest
import upload

# Load .env from project root
config = dotenv_values(".env")

# Thread-local storage so each thread gets its own Snowflake connection. Global conn.cursor() can cause failures.
_thread_local = threading.local()

# Initialize AWS session and S3 client once at the module level
aws_session = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])
aws_s3_client = aws_session.client('s3')

# Snowflake connection will be created on demand in get_snowflake_connection() to ensure thread safety.
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

faers_target_tables = ["DEMO", "DRUG", "REAC"]

def main():
    logger.info("Starting pipeline...")

    fda_drugs_df = ingest.drugsfda_from_api(config["OPENFDA_API_KEY"])
    logger.info("Ingested data from DrugsFDA dataset.")

    upload.polars_to_s3_parquet(client=aws_s3_client, data=fda_drugs_df, file_name=config["OPENFDA_RAW_FILE_NAME"], config=config)
    logger.info(f"Uploaded {config['OPENFDA_RAW_FILE_NAME']} to S3.")

    for table in faers_target_tables:
        faers_target_df = asyncio.run(ingest.get_full_faers_async(table))
        logger.info(f"Ingested FAERS {table} data from API.")
        
        upload.polars_to_s3_parquet(client=aws_s3_client, data=faers_target_df, file_name=f"{table.lower()}_faers_raw.parquet", config=config)
        logger.info(f"Uploaded {table}_FAERS_RAW.parquet to S3.")
        
    
    #Move all data from S3 to Snowflake
    upload.s3_parquet_to_snowflake(aws_session, get_snowflake_connection(), config)
    logger.info("Ingested files from S3 to Snowflake.")
        
    logger.info("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
