import pendulum
from airflow.sdk import dag, task


@task
def snow_upload_faers_demo():
    import sys
    sys.path.append("/opt/airflow/workspace/src")
    from pathlib import Path

    import boto3
    from dotenv import dotenv_values

    import config_snow
    import upload

    config = dotenv_values("/opt/airflow/workspace/.env")

    # Initialize AWS session and S3 client once at the module level
    aws_session = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])

    files_to_snowflake = {
        "demo_faers": Path(config["DEMO_FAERS_RAW_FILE_NAME"])
    }

    upload.s3_parquet_to_snowflake(aws_session, config_snow.get_snowflake_connection(), files_to_snowflake, config)

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)

def snow_upload_faers_demo_pipeline():
    snow_upload_faers_demo()

snow_upload_faers_demo_pipeline()
