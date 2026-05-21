import pendulum
from airflow.sdk import dag, task


@task
def ingest_openfda_dag():
    import sys
    sys.path.append("/opt/airflow/workspace/src")

    import boto3
    from dotenv import dotenv_values

    import ingest_openfda
    import upload

    config = dotenv_values("/opt/airflow/workspace/.env")

    # Initialize AWS session and S3 client once at the module level
    aws_session = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])
    aws_s3_client = aws_session.client('s3')

    #fda drugs data from API and upload to S3
    fda_drugs_df = ingest_openfda.drugsfda_from_api(config["OPENFDA_API_KEY"])
    upload.polars_to_s3_parquet(client=aws_s3_client, data=fda_drugs_df, file_name=config["OPENFDA_RAW_FILE_NAME"], config=config)

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)

def ingest_openfda_pipeline():
    ingest_openfda_dag()

ingest_openfda_pipeline()
