import asyncio
import io
import zipfile

import httpx
import polars as pl
from loguru import logger


def drugsfda_from_api(api_key):
    try:
        index_url = "https://api.fda.gov/download.json"
        params = {"api_key": api_key}

        # Use a single client session
        client = httpx.Client(timeout=60.0)

        # Get the index and extract the direct download URL
        index_res = client.get(index_url, params=params)
        index_res.raise_for_status()
        download_url = index_res.json()['results']['drug']['drugsfda']['partitions'][0]['file']

        # Download the data ZIP
        file_res = client.get(download_url, params=params)
        file_res.raise_for_status()
        client.close() # Clean up the client manually

        # Wrap the bytes in a ZipFile object and use .Path() to reach the JSON
        zf = zipfile.ZipFile(io.BytesIO(file_res.content))
        internal_file = zipfile.Path(zf, zf.namelist()[0])

        # Read directly into Polars
        return pl.read_json(internal_file.read_bytes())

    except Exception as e:
        logger.error(f"Error ingesting data from DrugsFDA API: {e}", exc_info=True)
        raise e

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
                        df = pl.read_csv(
                            f,
                            separator="$",
                            encoding="latin-1",
                            quote_char=None,
                            infer_schema_length=10000,
                            truncate_ragged_lines=True
                        )
                        return df.columns

    raise RuntimeError(f"Could not learn schema for {target_file_suffix}")


async def build_schema_override(target_file_suffix: str):
    cols = await learn_schema_for_table(target_file_suffix)
    return {col: pl.String for col in cols}

async def fetch_and_parse_quarter(
    client: httpx.AsyncClient,
    year: int,
    quarter: int,
    target_file_suffix: str,
    schema_override: dict
):
    url = f"https://fis.fda.gov/content/Exports/faers_ascii_{year}q{quarter}.zip"
    logger.info(f"Fetching {year} Q{quarter}...")

    try:
        response = await client.get(url)
        if response.status_code == 404:
            logger.warning(f"Skipping {year} Q{quarter}: File not found.")
            return None

        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            # Find matching file
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
                df = pl.read_csv(
                    f,
                    separator="$",
                    ignore_errors=True,
                    encoding="latin-1",
                    quote_char=None,
                    infer_schema_length=10000,
                    truncate_ragged_lines=True,
                    schema_overrides=schema_override
                )

                df = df.with_columns([
                    pl.lit(year).alias("src_year"),
                    pl.lit(quarter).alias("src_quarter")
                ])

                logger.info(f"Loaded {len(df)} rows from {year} Q{quarter}")
                return df

    except Exception as e:
        logger.error(f"Error processing {year} Q{quarter}: {e}", exc_info=True)
        return None


async def get_full_faers_async(target_file_suffix: str = "DEMO"):
    concat_frames = []

    logger.info(f"Learning file structure for {target_file_suffix}...")
    schema_override = await build_schema_override(target_file_suffix)
    # common_overrides = {
    #     "age": pl.String,
    #     "age_cod": pl.String,
    #     "gndr_cod": pl.String,
    #     "weight": pl.String,
    #     "nda_num": pl.String
    # }

    async with httpx.AsyncClient(timeout=120.0) as client:
        tasks = []

        for year in range(2012, 2027):
            for quarter in range(1, 5):
                tasks.append(
                    fetch_and_parse_quarter(
                        client,
                        year,
                        quarter,
                        target_file_suffix,
                        schema_override
                    )
                )

        # Run all downloads concurrently
        results = await asyncio.gather(*tasks)

    # Filter out None results
    concat_frames = [quarter_df for quarter_df in results if quarter_df is not None]

    if not concat_frames:
        return pl.DataFrame()

    return pl.concat(concat_frames, how="diagonal")
