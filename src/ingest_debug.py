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


def get_full_faers(target_file_suffix: str = "DEMO"):
    frames = []
    
    common_overrides = {
        "age": pl.String,
        "age_cod": pl.String,
        "gndr_cod": pl.String,
        "weight": pl.String,
        "nda_num": pl.String  # Crucial for joining to Drugs@FDA later
    }
    
    # Using a single client for connection pooling
    with httpx.Client(timeout=120.0) as client:
        for year in range(2024, 2027):
            for quarter in range(1, 5):
                url = f"https://fis.fda.gov/content/Exports/faers_ascii_{year}q{quarter}.zip"
                
                logger.info(f"Fetching {year} Q{quarter}...")
                try:
                    response = client.get(url)
                    # Handle missing quarters or URL changes gracefully
                    if response.status_code == 404:
                        logger.warning(f"Skipping {year} Q{quarter}: File not found.")
                        continue
                    response.raise_for_status()

                    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                        # Match the target file (DEMO, DRUG, etc.)
                        try:
                            target_file = [
                                f for f in z.namelist() 
                                if target_file_suffix.upper() in f.upper()
                                and f.lower().endswith(".txt") # <-- This stops the PDF errors
                                and not f.startswith("__MACOSX") # <-- Skips hidden metadata files
                            ][0]
                        except IndexError:
                            logger.warning(f"{target_file_suffix} file not found in {year} Q{quarter} ZIP. Skipping.")
                            continue
                        
                        with z.open(target_file) as f:
                            # Read with specific settings for FAERS
                            q_df = pl.read_csv(
                                f, 
                                separator='$', 
                                ignore_errors=True, 
                                encoding="latin-1",
                                quote_char=None, # FAERS files don't use consistent quoting
                                infer_schema_length=10000, # Better schema inference for large datasets
                                truncate_ragged_lines=True,
                                schema_overrides=common_overrides
                            )
                            
                            # Add tracking columns to identify source in the full DF
                            q_df = q_df.with_columns([
                                pl.lit(year).alias("src_year"),
                                pl.lit(quarter).alias("src_quarter")
                            ])
                            
                            frames.append(q_df)
                            print(f"Successfully loaded {len(q_df)} rows.")

                except Exception as e:
                    print(f"Error processing {year} Q{quarter}: {e}")

    if not frames:
        return pl.DataFrame()

    # Vertical concat handles potential column mismatches (nulling out missing cols)
    full_faers_df = pl.concat(frames, how="diagonal")
    return full_faers_df