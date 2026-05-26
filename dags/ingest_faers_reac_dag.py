import pendulum
from airflow.sdk import dag, task


@task
def ingest_faers_reac_dag():
    import sys
    sys.path.append("/opt/airflow/workspace/src")

    import asyncio

    import boto3
    from dotenv import dotenv_values

    import ingest_faers_full
    import upload

    config = dotenv_values("/opt/airflow/workspace/.env")

    # Initialize AWS session and S3 client once at the module level
    aws_session = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])
    aws_s3_client = aws_session.client('s3')

    #FAERS data from API and upload to S3
    faers_table = "REAC"

    faers_target_df = asyncio.run(ingest_faers_full.get_full_faers_async(faers_table))
    upload.polars_to_s3_parquet(client=aws_s3_client, data=faers_target_df, file_name=f"{faers_table.lower()}_faers_raw.parquet", config=config)

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)

def ingest_faers_reac_pipeline():
    ingest_faers_reac_dag()

ingest_faers_reac_pipeline()
