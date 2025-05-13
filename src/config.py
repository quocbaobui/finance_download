import os
from datetime import datetime

# GCS Configuration
GCS_BUCKET_NAME = "nissan-GTR"  
GCS_PROJECT_ID = "your-project-id"    
GCS_DESTINATION_PATH = "sgx-data/"   

# General Configuration
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
LOG_FILE = os.path.join(LOG_DIR, "download_sgx.log")
BASE_DATE = datetime(2025, 3, 14)  # Friday, 14 March 2025
BASE_URL_ID = 5898
FILE_TYPES = ["WEBPXTICK_DT.zip"]  # Chỉ tải file WEBPXTICK_DT.zip

# Create logs directory if it doesn't exist
os.makedirs(LOG_DIR, exist_ok=True)
