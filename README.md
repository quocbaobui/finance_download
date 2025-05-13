# SGX Downloader

A Python project to download WEBPXTICK_DT.zip from SGX, unzip it, compress CSV files with Zstandard (ZSTD), upload to Google Cloud Storage (GCS), and run an ETL pipeline to load data into ClickHouse.

## Overview

This project automates the process of downloading historical derivatives data from SGX, processing it with ZSTD compression, storing it on GCS, and loading it into ClickHouse for analysis. It uses PySpark for efficient data processing and supports large datasets (~130MB per file, ~2M rows).

## Directory Structure

The project is organized as follows:

```
sgx-downloader/
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── date_utils.py
│   ├── download_utils.py
│   ├── etl.py
│   ├── logger.py
│   └── main.py
├── requirements.txt
├── README.md
├── .gitignore
└── logs/                (automatically created directory for log files)
```

- **src/**: Contains all Python modules for the project.
  - **config.py**: Configuration settings (GCS bucket, log file, base date).
  - **date_utils.py**: Utility functions for date calculations.
  - **download_utils.py**: Functions for downloading, unzipping, compressing, and uploading to GCS.
  - **etl.py**: ETL pipeline using PySpark to load data into ClickHouse.
  - **logger.py**: Logging configuration.
  - **main.py**: Main script to run the downloader.
  - **__init__.py**: Marks src as a Python package.
- **requirements.txt**: Lists project dependencies.
- **README.md**: Project documentation.
- **.gitignore**: Excludes temporary files, logs, and sensitive data.
- **logs/**: Automatically created directory for log files (e.g., download_sgx.log).

## Prerequisites

- Python 3.8+
- Google Cloud account with a GCS bucket
- Service account key for GCS authentication
- ClickHouse server running locally (port 8123 for HTTP)
- Java 8 or 11 (for PySpark)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd sgx-downloader
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up Google Cloud authentication:
   - Download your service account key JSON file.
   - Set the environment variable:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
   ```

4. Set up ClickHouse:
   - Install and start ClickHouse server.
   - Create the target table:
   ```sql
   CREATE TABLE sgx_tick_data (
       Comm String,
       Contract_Type String,
       Mth_Code String,
       Year Int32,
       Strike Float32,
       Trade_Date Date,
       Log_Time Int32,
       Price Float32,
       Msg_Code Nullable(String),
       Volume Int32
   ) ENGINE = MergeTree()
   PARTITION BY toYYYYMM(Trade_Date)
   ORDER BY (Trade_Date, Comm, Contract_Type);
   ```

## Configuration

Edit `src/config.py` to set:
- `GCS_BUCKET_NAME`: Your GCS bucket name.
- `GCS_PROJECT_ID`: Your Google Cloud project ID.
- `GCS_DESTINATION_PATH`: Destination folder in the bucket.
- `LOG_FILE`: Log file path (default: logs/download_sgx.log in project directory).

## Usage

### Download and Upload to GCS

Run the downloader script:
```bash
python -m src.main [options]
```

#### Options

- `-d, --date YYYY-MM-DD`: Download and process files for a specific date.
- `-r, --range START END`: Download and process files for a date range (YYYY-MM-DD).
- `-t, --today`: Download and process for the last weekday.
- `-a, --auto`: Auto-download from base date (2025-03-14) to the last weekday.

Currently: Downloads WEBPXTICK_DT.zip, unzips, compresses CSV files to ZSTD, and uploads to GCS.

#### Examples

```bash
# Download and process for a specific date
python -m src.main --date 2025-03-14

# Download and process for a date range
python -m src.main --range 2025-03-14 2025-03-20
```

### ETL Pipeline

Run the ETL script to load data from GCS to ClickHouse:
```bash
python -m src.etl --gcs-path "sgx-data/*.zst" --table "sgx_tick_data" --batch-size 100000
```

#### Options

- `--gcs-path`: GCS path pattern to ZSTD files (default: sgx-data/*.zst).
- `--table`: ClickHouse table name (default: sgx_tick_data).
- `--batch-size`: Number of rows per batch for ClickHouse insertion (default: 100000).

## CSV Schema

The ETL pipeline expects CSV files with the following schema:
- Comm: String
- Contract_Type: String
- Mth_Code: String
- Year: Int32
- Strike: Float32
- Trade_Date: Date
- Log_Time: Int32
- Price: Float32
- Msg_Code: Nullable(String)
- Volume: Int32

## Logging

- Logs are written to logs/download_sgx.log in the project directory.
- Info and error messages are printed to the console.
- Failed downloads are logged to missed_files.txt.

## Gitignore

The .gitignore file ensures that temporary files, logs, and sensitive data are not committed:

- Python cache files (__pycache__, *.pyc, etc.)
- Virtual environments (env/, venv/, etc.)
- Log files (*.log)
- Google Cloud credentials (*.json)
- IDE settings (.vscode/, .idea/)
- OS-specific files (.DS_Store, Thumbs.db)

## Notes

- The downloader skips weekends as SGX data is only available for weekdays.
- Ensure your GCS bucket and service account have `storage.objects.create` permission.
- Ensure ClickHouse server is running locally (port 8123) before running the ETL script.
- The ETL pipeline is optimized for large CSV files (~130MB, 2M rows) with ZSTD compression, batch processing, and Spark partitioning.
