import os
import requests
import zipfile
import shutil
from datetime import datetime
from google.cloud import storage
import logging
import tempfile
from .date_utils import get_url_id, is_weekday, get_last_weekday

logger = logging.getLogger(__name__)

def generate_filename(file_type: str, date: datetime) -> str:
    """Generate the filename based on file type and date."""
    return f"{file_type.split('.')[0]}_{date.strftime('%Y%m%d')}.{file_type.split('.')[-1]}"

def download_file_to_temp(url: str, temp_filepath: str) -> bool:
    """Download a file of the SGX website from a URL and save it to a temporary path."""
    try:
        logger.info(f"Starting download from {url} to temp: {temp_filepath}")
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        with open(temp_filepath, 'wb') as f:
            f.write(response.content)
        logger.info(f"Downloaded to temp: {temp_filepath}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading {url}: {e}")
        with open('missed_files.txt', 'a') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d')} - {url}\n")
        return False

def unzip_and_upload_to_gcs(zip_filepath: str, bucket: storage.Bucket, gcs_destination_path: str) -> bool:
    """Unzip the file, upload extracted files to GCS, and clean up."""
    try:
        # Create a temporary directory for extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"Extracting {zip_filepath} to {temp_dir}")
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            logger.info(f"Extracted files to {temp_dir}")

            # Upload each extracted file to GCS
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    # Construct GCS path, preserving relative path
                    relative_path = os.path.relpath(local_path, temp_dir)
                    gcs_filepath = os.path.join(gcs_destination_path, relative_path)
                    
                    logger.info(f"Uploading {local_path} to GCS: {gcs_filepath}")
                    blob = bucket.blob(gcs_filepath)
                    blob.upload_from_filename(local_path, content_type='application/octet-stream')
                    logger.info(f"Uploaded to GCS: {gcs_filepath}")

        # Delete the zip file after successful extraction and upload
        logger.info(f"Deleting zip file: {zip_filepath}")
        os.remove(zip_filepath)
        return True
    except zipfile.BadZipFile as e:
        logger.error(f"Error unzipping {zip_filepath}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error processing {zip_filepath} to GCS: {e}")
        return False

def download_single_file(date: datetime, file_type: str, bucket: storage.Bucket, 
                        base_date: datetime, base_url_id: int, gcs_destination_path: str) -> bool:
    """Download a single ZIP file, unzip it, and upload extracted files to GCS."""
    url_id = get_url_id(date, base_date, base_url_id)
    url = f"https://links.sgx.com/1.0.0/derivatives-historical/{url_id}/{file_type}"
    filename = generate_filename(file_type, date)
    
    # Create a temporary file path for the ZIP
    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_file:
        temp_filepath = temp_file.name
    
    logger.debug(f"Generated URL: {url}")
    logger.debug(f"Temporary file path: {temp_filepath}")
    
    # Download the ZIP file to temp path
    if not download_file_to_temp(url, temp_filepath):
        return False
    
    # Unzip and upload extracted files to GCS
    return unzip_and_upload_to_gcs(temp_filepath, bucket, gcs_destination_path)

def download_files_for_date(date: datetime, file_types: list, bucket: storage.Bucket, 
                            base_date: datetime, base_url_id: int, gcs_destination_path: str) -> bool:
    """Download all specified files for a given date, unzip, and upload to GCS."""
    success = True
    for file_type in file_types:
        if not download_single_file(date, file_type, bucket, base_date, base_url_id, gcs_destination_path):
            success = False
    return success

def download_date_range(start_date: datetime, end_date: datetime, file_types: list, bucket: storage.Bucket, 
                        base_date: datetime, base_url_id: int, gcs_destination_path: str) -> None:
    """Download files for a range of dates, unzip, and upload to GCS, skipping weekends."""
    current_date = start_date
    while current_date <= end_date:
        if is_weekday(current_date):
            download_files_for_date(current_date, file_types, bucket, base_date, base_url_id, gcs_destination_path)
        else:
            logger.info(f"Skipping {current_date.strftime('%Y-%m-%d')} (weekend)")
        current_date += timedelta(days=1)

def download_auto(bucket: storage.Bucket, file_types: list, base_date: datetime, 
                  base_url_id: int, gcs_destination_path: str) -> None:
    """Automatically download files from base date to the last weekday, unzip, and upload to GCS."""
    today = datetime.today()
    last_weekday = get_last_weekday(today)
    
    if last_weekday < base_date:
        logger.info("No data to download: last weekday is before base date.")
        return
    
    logger.info(f"Auto downloading from {base_date.strftime('%Y-%m-%d')} to {last_weekday.strftime('%Y-%m-%d')}")
    download_date_range(base_date, last_weekday, file_types, bucket, base_date, base_url_id, gcs_destination_path)
