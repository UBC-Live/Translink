import requests
import zipfile
import io
import os
from datetime import datetime
import logging

URL = "https://gtfs-static.translink.ca/gtfs/google_transit.zip"
BASE_OUTPUT_DIR = "data/raw/static"


def init_logging(now):
    """
    Initialize a logger for the static GTFS data download.

    Creates a timestamped log file under `data/runs/` and configures
    it to capture info-level events for the static GTFS download process.

    Args:
        now (str): Timestamp string used to name the log file.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger("Static")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"data/runs/static_{now}.log")
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


def run(timestamp=None):
    """
    Download and extract the GTFS static data feed.

    This function:
        1. Creates an output directory timestamped with the run time.
        2. Downloads the GTFS static ZIP file from TransLink.
        3. Validates the HTTP response.
        4. Unzips the GTFS contents into the output directory.
        5. Logs the progress and errors to a timestamped log file.

    Args:
        timestamp (str | None): Optional override for the timestamp used in
            directory/log naming. If not provided, the current time is used.

    Raises:
        requests.RequestException: If the ZIP file cannot be downloaded.
        zipfile.BadZipFile: If the downloaded file is not a valid ZIP.
        Exception: For any other unexpected errors.
    """
    now = timestamp or datetime.now().strftime("%Y-%m-%dT%H-%M")
    logger = init_logging(now)
    output_dir = os.path.join(BASE_OUTPUT_DIR, f"gtfs_static_{now}")

    try:
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")

        logger.info(f"Fetching ZIP file from {URL}...")
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        logger.info("Download successful!")

        logger.info("Unzipping the file...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            zf.extractall(output_dir)
        logger.info(f"Extraction completed to {output_dir}")

    except requests.RequestException as e:
        logger.error(f"Failed to download the ZIP file: {e}")
        raise
    except zipfile.BadZipFile as e:
        logger.error(f"The downloaded file is not a valid ZIP: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    run()
