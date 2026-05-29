import pendulum
from airflow.sdk import dag, task


@task
def ingest_clinical_trials_to_s3():
    import sys
    sys.path.append("/opt/airflow/workspace/src")

    import asyncio

    import boto3
    from dotenv import dotenv_values

    import ingest_clinical_trials
    import upload

    config = dotenv_values("/opt/airflow/workspace/.env")

    aws_session   = boto3.Session(profile_name=config["SSO_PROFILE_NAME"])
    aws_s3_client = aws_session.client("s3")

    # Fetch all studies from ClinicalTrials.gov — pass query_params to narrow
    # the pull, e.g. {"query.cond": "diabetes"}, or leave empty for everything.
    query_params = {}

    ct_df = asyncio.run(
        ingest_clinical_trials.get_full_clinical_trials_async(
            query_params=query_params,
        )
    )

    file_name = config["CLINICAL_TRIALS_RAW_FILE_NAME"]

    upload.polars_to_s3_parquet(
        client=aws_s3_client,
        data=ct_df,
        file_name=file_name,
        config=config,
    )


@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)
def ingest_clinical_trials_pipeline():
    ingest_clinical_trials_to_s3()


ingest_clinical_trials_pipeline()
