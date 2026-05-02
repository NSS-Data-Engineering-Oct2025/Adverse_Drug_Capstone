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
