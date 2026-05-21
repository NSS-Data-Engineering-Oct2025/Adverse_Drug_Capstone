import asyncio
import threading
from pathlib import Path

import boto3
from dotenv import dotenv_values
from loguru import logger

import config_snow
import ingest_census
import ingest_faers
import ingest_openfda
import upload

# Load .env from project root
config = dotenv_values(".env")

# Thread-local storage so each thread gets its own Snowflake connection. Global conn.cursor() can cause failures.
_thread_local = threading.local()

# Initialize AWS session and S3 client once at the module level
aws_session = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])
aws_s3_client = aws_session.client('s3')

# Snowflake connection will be created on demand in get_snowflake_connection() to ensure thread safety.

faers_target_tables = ["DEMO", "DRUG", "REAC"]

#Files to move to Snowflake
files_to_snowflake = {
    "openfda_drugs": Path(config['OPENFDA_RAW_FILE_NAME']),
    "census_fips": Path(config["CENSUS_FIPS_RAW_FILE_NAME"]),
    "demo_faers": Path(config["DEMO_FAERS_RAW_FILE_NAME"]),
    "drug_faers": Path(config["DRUG_FAERS_RAW_FILE_NAME"]),
    "reac_faers": Path(config["REAC_FAERS_RAW_FILE_NAME"])
}

def main():
    logger.info("Starting pipeline...")

    #fda drugs data from API and upload to S3
    fda_drugs_df = ingest_openfda.drugsfda_from_api(config["OPENFDA_API_KEY"])
    logger.info("Ingested data from DrugsFDA dataset.")
    upload.polars_to_s3_parquet(client=aws_s3_client, data=fda_drugs_df, file_name=config["OPENFDA_RAW_FILE_NAME"], config=config)
    logger.info(f"Uploaded {config['OPENFDA_RAW_FILE_NAME']} to S3.")

    #census fips data from API and upload to S3
    census_fips_df = ingest_census.fetch_census_fips(config["CENSUS_API_URL"], config["CENSUS_API_KEY"])
    logger.info("Ingested data from Census FIPS dataset.")
    upload.polars_to_s3_parquet(client=aws_s3_client, data=census_fips_df, file_name=config["CENSUS_FIPS_RAW_FILE_NAME"], config=config)
    logger.info(f"Uploaded {config['CENSUS_FIPS_RAW_FILE_NAME']} to S3.")

    #FAERS data from API and upload to S3
    for table in faers_target_tables:
        faers_target_df = asyncio.run(ingest_faers.get_full_faers_async(table))
        logger.info(f"Ingested FAERS {table} data from API.")
        upload.polars_to_s3_parquet(client=aws_s3_client, data=faers_target_df, file_name=f"{table.lower()}_faers_raw.parquet", config=config)
        logger.info(f"Uploaded {table}_FAERS_RAW.parquet to S3.")


    #Move all data from S3 to Snowflake
    upload.s3_parquet_to_snowflake(aws_session, config_snow.get_snowflake_connection(), files_to_snowflake, config)
    logger.info("Ingested files from S3 to Snowflake.")

    logger.info("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
