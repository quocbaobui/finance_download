import argparse
import sys
from datetime import datetime
from google.cloud import storage
from .config import GCS_PROJECT_ID, GCS_BUCKET_NAME, LOG_FILE, BASE_DATE, BASE_URL_ID, FILE_TYPES, GCS_DESTINATION_PATH
from .logger import setup_logging
from .date_utils import is_weekday, get_last_weekday
from .download_utils import download_files_for_date, download_date_range, download_auto

logger = setup_logging(LOG_FILE)

def setup_gcs_client() -> storage.Bucket:
    """Initialize Google Cloud Storage client."""
    try:
        client = storage.Client(project=GCS_PROJECT_ID)
        bucket = client.get_bucket(GCS_BUCKET_NAME)
        logger.info(f"Connected to GCS bucket: {GCS_BUCKET_NAME}")
        return bucket
    except Exception as e:
        logger.error(f"Failed to connect to GCS: {e}")
        sys.exit(1)

def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Download files from SGX to GCS (latest file is last weekday).")
    parser.add_argument('-d', '--date', help="Specific date (YYYY-MM-DD)")
    parser.add_argument('-r', '--range', nargs=2, metavar=('START', 'END'), 
                        help="Date range (YYYY-MM-DD YYYY-MM-DD)")
    parser.add_argument('-t', '--today', action='store_true', help="Download files for the last weekday")
    parser.add_argument('-a', '--auto', action='store_true', help="Auto download from base date to last weekday")
    parser.add_argument('-c', '--config', help="Config file (not implemented)")
    return parser.parse_args()

def main():
    """Main function to handle download logic based on arguments."""
    args = parse_arguments()
    bucket = setup_gcs_client()
    today = datetime.today()
    last_weekday = get_last_weekday(today)

    if args.date:
        try:
            date = datetime.strptime(args.date, '%Y-%m-%d')
            if date > last_weekday:
                logger.info(f"Date {date.strftime('%Y-%m-%d')} is today or in the future, skipping.")
            elif not is_weekday(date):
                logger.info(f"Date {date.strftime('%Y-%m-%d')} is a weekend, no files available.")
            else:
                download_files_for_date(date, FILE_TYPES, bucket, BASE_DATE, BASE_URL_ID, GCS_DESTINATION_PATH)
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            sys.exit(1)

    elif args.range:
        try:
            start_date = datetime.strptime(args.range[0], '%Y-%m-%d')
            end_date = datetime.strptime(args.range[1], '%Y-%m-%d')
            if start_date > end_date:
                logger.error("Start date must be less than or equal to end date.")
                sys.exit(1)
            if end_date > last_weekday:
                logger.info(f"End date adjusted from {end_date.strftime('%Y-%m-%d')} to {last_weekday.strftime('%Y-%m-%d')}")
                end_date = last_weekday
            download_date_range(start_date, end_date, FILE_TYPES, bucket, BASE_DATE, BASE_URL_ID, GCS_DESTINATION_PATH)
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            sys.exit(1)

    elif args.today:
        logger.info(f"Downloading files for last weekday: {last_weekday.strftime('%Y-%m-%d')}")
        download_files_for_date(last_weekday, FILE_TYPES, bucket, BASE_DATE, BASE_URL_ID, GCS_DESTINATION_PATH)

    elif args.auto or not any(vars(args).values()):
        download_auto(bucket, FILE_TYPES, BASE_DATE, BASE_URL_ID, GCS_DESTINATION_PATH)

    elif args.config:
        logger.info("Config file functionality not implemented.")
        sys.exit(1)

if __name__ == "__main__":
    main()
