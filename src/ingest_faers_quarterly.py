import asyncio
import io
import zipfile

import httpx
import polars as pl
from loguru import logger


async def learn_schema_for_table(target_file_suffix: str):
    async with httpx.AsyncClient(timeout=60.0) as client:
        for year in range(2026, 2011, -1):  # newest → oldest
            for quarter in range(4, 0, -1):
                url = f"https://fis.fda.gov/content/Exports/faers_ascii_{year}q{quarter}.zip"
                res = await client.get(url)
                if res.status_code != 200:
                    continue

                with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                    matches = [
                        f for f in z.namelist()
                        if target_file_suffix.upper() in f.upper()
                        and f.lower().endswith(".txt")
                    ]
                    if not matches:
                        continue

                    with z.open(matches[0]) as f:
                        dummy_df = pl.read_csv(
                            f,
                            separator="$",
                            encoding="latin-1",
                            quote_char=None,
                            infer_schema_length=10000,
                            truncate_ragged_lines=True
                        )
                        return dummy_df.columns

    raise RuntimeError(f"Could not learn schema for {target_file_suffix}")


async def build_schema_override(target_file_suffix: str):
    cols = await learn_schema_for_table(target_file_suffix)
    return {col: pl.String for col in cols}


async def fetch_parse_and_upload_quarter(
    client: httpx.AsyncClient,
    year: int,
    quarter: int,
    target_file_suffix: str,
    schema_override: dict,
    s3_client,
    config: dict,
) -> str | None:
    """
    Fetch one quarter, parse it, upload as a parquet file to S3, and return
    the S3 key on success (or None on failure / not found).
    """
    url = f"https://fis.fda.gov/content/Exports/faers_ascii_{year}q{quarter}.zip"
    logger.info(f"Fetching {year} Q{quarter}...")

    try:
        response = await client.get(url)
        if response.status_code == 404:
            logger.warning(f"Skipping {year} Q{quarter}: File not found.")
            return None

        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            matches = [
                f for f in z.namelist()
                if target_file_suffix.upper() in f.upper()
                and f.lower().endswith(".txt")
                and not f.startswith("__MACOSX")
            ]
            if not matches:
                logger.warning(f"{target_file_suffix} not found in {year} Q{quarter}. Skipping.")
                return None

            target_file = matches[0]

            with z.open(target_file) as f:
                raw = f.read()
                text = raw.decode("latin-1", errors="replace")
                faers_df = pl.read_csv(
                    io.StringIO(text),
                    separator="$",
                    ignore_errors=True,
                    quote_char=None,
                    infer_schema_length=10000,
                    truncate_ragged_lines=True,
                    schema_overrides=schema_override
                )

                faers_df = faers_df.with_columns([
                    pl.lit(year).alias("src_year"),
                    pl.lit(quarter).alias("src_quarter")
                ])

                logger.info(f"Loaded {len(faers_df)} rows from {year} Q{quarter}")

        # Upload this quarter's dataframe to S3 immediately, then free memory
        file_name = f"{target_file_suffix.lower()}_faers_{year}_q{quarter}.parquet"
        s3_key = f"{config['PROJECT_FOLDER']}/{file_name}"

        parquet_buffer = io.BytesIO()
        faers_df.write_parquet(parquet_buffer)
        s3_client.put_object(
            Bucket=config['AWS_BUCKET_NAME'],
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
        )
        logger.info(f"Uploaded {year} Q{quarter} → s3://{config['AWS_BUCKET_NAME']}/{s3_key}")

        return s3_key

    except Exception as e:
        logger.error(f"Error processing {year} Q{quarter}: {e}", exc_info=True)
        return None


async def get_full_faers_async(
    target_file_suffix: str = "DEMO",
    s3_client=None,
    config: dict = None,
    concurrency: int = 4,
) -> list[str]:
    """
    Download every available FAERS quarter for *target_file_suffix*, upload
    each one as an individual parquet file to S3, and return the list of S3
    keys that were successfully written.

    Parameters
    ----------
    target_file_suffix : str
        The FAERS table name (e.g. "DRUG", "DEMO", "REAC").
    s3_client :
        A boto3 S3 client used for uploads.
    config : dict
        Must contain 'AWS_BUCKET_NAME' and 'PROJECT_FOLDER'.
    concurrency : int
        Max simultaneous downloads. Keep this low (4–8) to avoid hammering
        the FDA server and blowing up memory. Each quarter can be hundreds of
        MB in RAM, so concurrency × quarter_size must fit in your Airflow
        worker's memory budget.

    Returns
    -------
    list[str]
        S3 keys of every successfully uploaded quarterly parquet file.
    """
    if s3_client is None or config is None:
        raise ValueError("s3_client and config are required for per-quarter S3 uploads.")

    logger.info(f"Learning file structure for {target_file_suffix}...")
    schema_override = await build_schema_override(target_file_suffix)

    # Build the full list of (year, quarter) combos to attempt
    quarters = [
        (year, quarter)
        for year in range(2012, 2027)
        for quarter in range(1, 5)
    ]

    uploaded_keys: list[str] = []
    semaphore = asyncio.Semaphore(concurrency)

    async def bounded_fetch(year, quarter):
        async with semaphore:
            return await fetch_parse_and_upload_quarter(
                client=client,
                year=year,
                quarter=quarter,
                target_file_suffix=target_file_suffix,
                schema_override=schema_override,
                s3_client=s3_client,
                config=config,
            )

    async with httpx.AsyncClient(timeout=120.0) as client:
        tasks = [bounded_fetch(year, quarter) for year, quarter in quarters]
        results = await asyncio.gather(*tasks)

    uploaded_keys = [key for key in results if key is not None]
    logger.info(f"Finished. {len(uploaded_keys)} quarters uploaded to S3.")
    return uploaded_keys


def combine_quarterly_parquets_in_snowflake(
    snowflake_conn,
    table_name: str,
    config: dict,
    uploaded_keys: list[str],
):
    """
    Create (or replace) a Snowflake table by loading ALL quarterly parquet
    files whose names match the pattern  <table_name>_faers_*_q*.parquet
    from the project S3 stage in a single COPY INTO statement.

    This avoids ever concatenating the data locally — Snowflake handles it.

    Parameters
    ----------
    snowflake_conn :
        An open Snowflake connection (e.g. from snowflake.connector.connect).
    table_name : str
        The FAERS table suffix used when uploading, e.g. "drug", "demo".
    config : dict
        Must contain 'AWS_BUCKET_NAME' and 'PROJECT_FOLDER'.
    uploaded_keys : list[str]
        S3 keys returned by get_full_faers_async. The first key is used to
        infer the schema; all keys are loaded via COPY INTO.
    """
    cur = snowflake_conn.cursor()
    sf_table = f"{table_name.upper()}_FAERS"
    project_folder = config["PROJECT_FOLDER"]

    # Strip the project folder prefix to get bare filenames relative to the stage root.
    # uploaded_keys look like "my-project/drug_faers_2012_q1.parquet"
    file_names = [
        key.removeprefix(f"{project_folder}/")
        for key in uploaded_keys
    ]
    first_file = file_names[0]

    # Build an explicit FILES = (...) list — more reliable than PATTERN matching
    # because it is not sensitive to regex escaping or stage path quirks.
    files_clause = ", ".join(f"'{f}'" for f in file_names)

    try:
        cur.execute("""
            CREATE OR REPLACE FILE FORMAT parquet_ingest_format
            TYPE = PARQUET
            SNAPPY_COMPRESSION = TRUE
        """)

        # Infer schema from ONE concrete file path
        cur.execute(f"""
            CREATE OR REPLACE TABLE {sf_table}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(INFER_SCHEMA(
                    LOCATION => '@drugs_s3_stage/{first_file}',
                    FILE_FORMAT => 'parquet_ingest_format'
                ))
            )
        """)
        logger.info(f"Table {sf_table} created from schema of {first_file}.")

        # Explicit FILES list avoids PATTERN regex gotchas.
        # MATCH_BY_COLUMN_NAME is required when loading multi-column parquet into
        # a structured table — without it Snowflake expects a single VARIANT column.
        cur.execute(f"""
            COPY INTO {sf_table}
            FROM @drugs_s3_stage/
            FILE_FORMAT = (FORMAT_NAME = 'parquet_ingest_format')
            FILES = ({files_clause})
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
        """)

        # Fetch and log per-file results so we can see what loaded (and what did not)
        copy_results = cur.fetchall()
        total_rows = 0
        errors = []
        for row in copy_results:
            # row columns: file, status, rows_parsed, rows_loaded, error_limit,
            #              errors_seen, first_error, first_error_line, ...
            file_name, status = row[0], row[1]
            rows_loaded = row[3]
            total_rows += int(rows_loaded or 0)
            if status != "LOADED":
                errors.append(f"{file_name}: {status} (loaded={rows_loaded})")
            else:
                logger.debug(f"  {file_name}: {rows_loaded} rows loaded")

        logger.info(f"COPY INTO complete. Total rows loaded into {sf_table}: {total_rows:,}")
        if errors:
            logger.warning(f"{len(errors)} files had issues:\n" + "\n".join(errors))

    except Exception as e:
        logger.error(f"Error combining quarters into Snowflake: {e}", exc_info=True)
        raise

    finally:
        cur.close()
