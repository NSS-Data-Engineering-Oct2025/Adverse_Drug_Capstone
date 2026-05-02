import threading

import boto3
import snowflake.connector
from dotenv import dotenv_values
from loguru import logger

import ingest
import upload

# Load .env from project root
config = dotenv_values(".env")

# Thread-local storage so each thread gets its own Snowflake connection. Global conn.cursor() can cause failures.
_thread_local = threading.local()

openfda_api_key = config["OPENFDA_API_KEY"]

aws_session = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])
aws_s3_client = aws_session.client('s3')

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

    fda_drugs_df = ingest.drugsfda_from_api(openfda_api_key)
    # logger.info(f"Loaded {len(fda_drugs_df)} records from DrugsFDA dataset.")
    # logger.info(f"Sample records:\n{fda_drugs_df.head()}")

    # # Select the 'results' column and unnest it to get the drug records
    # drug_records_df = (
    #     fda_drugs_df
    #     .select("results")
    #     .explode("results")  # Converts the list into one row per drug record
    #     .unnest("results")   # Now that it's a struct, we can unnest into columns
    # )

    # # 3. Now flatten the internal products list
    # flat_df = (
    #     drug_records_df
    #     .explode("products")
    #     .unnest("products")
    # )

    # logger.info(f"Transformed to flat structure with {len(flat_df)} records.")
    # logger.info(f"Sample transformed records:\n{flat_df.head()}")

    upload.polars_to_s3_parquet(
        client=aws_s3_client,
        data=fda_drugs_df,
        bucket=config["AWS_BUCKET_NAME"],
        file_name=config["OPENFDA_RAW_FILE_NAME"],
        folder=config["PROJECT_FOLDER"]
    )

    upload.s3_parquet_to_snowflake(aws_session, get_snowflake_connection(), config)

    logger.info("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
