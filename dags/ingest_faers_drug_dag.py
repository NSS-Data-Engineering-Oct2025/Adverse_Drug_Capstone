import pendulum
from airflow.sdk import dag, task


@task
def ingest_faers_drug_to_s3():
    import sys
    sys.path.append("/opt/airflow/workspace/src")

    import asyncio

    import boto3
    from dotenv import dotenv_values

    import ingest_faers_quarterly

    config = dotenv_values("/opt/airflow/workspace/.env")

    aws_session = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])
    aws_s3_client = aws_session.client('s3')

    faers_table = "DRUG"

    # Each quarter is fetched, parsed, uploaded to S3, and freed from memory
    # before the next batch starts. Returns the list of S3 keys written.
    uploaded_keys = asyncio.run(
        ingest_faers_quarterly.get_full_faers_async(
            target_file_suffix=faers_table,
            s3_client=aws_s3_client,
            config=config,
            concurrency=4,   # tune down if worker runs out of memory
        )
    )

    return uploaded_keys   # XCom'd to the next task if needed


@task
def combine_faers_drug_in_snowflake(uploaded_keys: list[str]):
    import sys
    sys.path.append("/opt/airflow/workspace/src")

    import boto3
    import snowflake.connector
    from dotenv import dotenv_values

    import ingest_faers_quarterly

    config = dotenv_values("/opt/airflow/workspace/.env")

    if not uploaded_keys:
        from loguru import logger
        logger.warning("No quarterly files were uploaded; skipping Snowflake load.")
        return

    aws_session = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])
    aws_credentials = aws_session.get_credentials().get_frozen_credentials()

    snowflake_conn = snowflake.connector.connect(
        user=config["SNOWFLAKE_USER"],
        password=config["SNOWFLAKE_PASSWORD"],
        account=config["SNOWFLAKE_ACCOUNT"],
        warehouse=config["SNOWFLAKE_WAREHOUSE"],
        database=config["SNOWFLAKE_DATABASE"],
        schema=config["SNOWFLAKE_SCHEMA"],
    )

    try:
        # Ensure the S3 stage exists (idempotent)
        cur = snowflake_conn.cursor()
        cur.execute(f"""
            CREATE OR REPLACE TEMPORARY STAGE drugs_s3_stage
            URL = 's3://{config['AWS_BUCKET_NAME']}/{config['PROJECT_FOLDER']}/'
            CREDENTIALS = (
                AWS_KEY_ID = '{aws_credentials.access_key}',
                AWS_SECRET_KEY = '{aws_credentials.secret_key}',
                AWS_TOKEN = '{aws_credentials.token}'
            )
        """)
        cur.close()

        ingest_faers_quarterly.combine_quarterly_parquets_in_snowflake(
            snowflake_conn=snowflake_conn,
            table_name="drug",
            config=config,
            uploaded_keys=uploaded_keys,
        )
    finally:
        snowflake_conn.close()


@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)
def ingest_faers_drug_pipeline():
    keys = ingest_faers_drug_to_s3()
    combine_faers_drug_in_snowflake(keys)


ingest_faers_drug_pipeline()
