# config.py
from datetime import datetime

# GCS Configuration
GCS_BUCKET_NAME = "nissan-GTR "  
GCS_PROJECT_ID = "your-project-id"    
GCS_DESTINATION_PATH = "sgx-data/"    

# General Configuration
LOG_FILE = r"F:\Bao_Data MiniProject_20250322\my_project\download_sgx.log"
BASE_DATE = datetime(2025, 3, 14)  # Friday, 14 March 2025
BASE_URL_ID = 5898
FILE_TYPES = ["WEBPXTICK_DT.zip"]
