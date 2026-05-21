import pendulum
from airflow.sdk import dag, task


@task
def ingest_census_dag():
    import sys
    sys.path.append("/opt/airflow/workspace/src")

    import boto3
    from dotenv import dotenv_values

    import ingest_census
    import upload

    config = dotenv_values("/opt/airflow/workspace/.env")

    # Initialize AWS session and S3 client once at the module level
    aws_session = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])
    aws_s3_client = aws_session.client('s3')

    #census fips data from API and upload to S3
    census_fips_df = ingest_census.fetch_census_fips(config["CENSUS_API_URL"], config["CENSUS_API_KEY"])
    upload.polars_to_s3_parquet(client=aws_s3_client, data=census_fips_df, file_name=config["CENSUS_FIPS_RAW_FILE_NAME"], config=config)

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)

def ingest_census_pipeline():
    ingest_census_dag()

ingest_census_pipeline()
