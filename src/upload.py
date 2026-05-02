import time
from io import BytesIO
from pathlib import Path

from loguru import logger


def polars_to_s3_parquet(client, data, bucket, file_name, folder = None):
    try:
        s3_key = f"{folder}/{file_name}"
        parquet_buffer = BytesIO()
        data.write_parquet(parquet_buffer)
        client.put_object(Bucket=bucket, Key=s3_key, Body=parquet_buffer.getvalue())
        logger.info(f"File {file_name} uploaded to S3 bucket {bucket} successfully at {folder}.")

    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}", exc_info=True)
        raise e


def s3_parquet_to_snowflake(aws_session, snowflake_conn, config):
    try:
        # Get AWS Credentials
        aws_credentials = aws_session.get_credentials().get_frozen_credentials()

        # Establish Snowflake Connection
        conn= snowflake_conn
        curr = conn.cursor()

        # Create File Formats
        curr.execute("""
            CREATE OR REPLACE FILE FORMAT csv_ingest_format
            TYPE = CSV
            PARSE_HEADER = TRUE
            FIELD_DELIMITER = ','
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        """)

        curr.execute("""
            CREATE OR REPLACE FILE FORMAT parquet_ingest_format
            TYPE = PARQUET
        """)

        # Create a Temporary Stage to access S3 (Equivalent to DuckDB Create Secret)
        curr.execute(f"""
            CREATE OR REPLACE TEMPORARY STAGE drugs_s3_stage
            URL = 's3://{config['AWS_BUCKET_NAME']}/{config['PROJECT_FOLDER']}/'
            CREDENTIALS = (
                AWS_KEY_ID = '{aws_credentials.access_key}',
                AWS_SECRET_KEY = '{aws_credentials.secret_key}',
                AWS_TOKEN = '{aws_credentials.token}'
            )
        """)

        #Files to move to Snowflake
        files = {
            "openfda_drugs": Path(config['OPENFDA_RAW_FILE_NAME'])
        }

        # Main Ingestion
        for table_name, file_name in files.items():
            start_time = time.time()
            table_name = table_name.upper()

            try:
                if file_name.suffix == ".csv":
                    logger.info(f"Native SQL Load: {table_name}")

                    # Infer schema and create table
                    curr.execute(f"""
                        CREATE OR REPLACE TABLE {table_name}
                        USING TEMPLATE (
                            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                            FROM TABLE(INFER_SCHEMA(
                                LOCATION=>'@nppes_s3_stage/{file_name}',
                                FILE_FORMAT=>'csv_ingest_format'
                            ))
                        );
                    """)

                    # Load data
                    curr.execute(f"""COPY INTO {table_name}
                                FROM @nppes_s3_stage/{file_name}
                                FILE_FORMAT=(FORMAT_NAME='csv_ingest_format')
                                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                                ON_ERROR='CONTINUE';
                                """)
                    
                elif file_name.suffix == ".parquet":
                    logger.info(f"Parquet Load: {table_name}")

                    # Infer schema and create table
                    curr.execute(f"""
                        CREATE OR REPLACE TABLE {table_name}
                        USING TEMPLATE (
                            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                            FROM TABLE(INFER_SCHEMA(
                                LOCATION=>'@drugs_s3_stage/{file_name}',
                                FILE_FORMAT=>'parquet_ingest_format'
                            ))
                        );
                    """)

                    # Load data
                    curr.execute(f"""COPY INTO {table_name}
                                FROM @drugs_s3_stage/{file_name}
                                FILE_FORMAT=(FORMAT_NAME='parquet_ingest_format')
                                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                                ON_ERROR='CONTINUE';
                                """)

                duration = round((time.time() - start_time) / 60, 2)
                logger.info(f"{table_name} finished in {duration} mins.")

            except Exception as e:
                logger.error(f"Error on {table_name}: {e}")

    except Exception as e:
        logger.error(f"Critical Error: {e}", exc_info=True)

    finally:
        curr.close()
        conn.close()

        logger.info(" All data loaded successfully.")
