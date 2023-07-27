"""Settings"""
from pathlib import Path
import os
import boto3
from datetime import timedelta

#Variables
BUCKET_NAME='airqualitydatastorage'
AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")
PAYS = 'SEN'
API_TOKEN=os.getenv("API_TOKEN")
API_URL = "https://api.waqi.info/feed/here/?token="+API_TOKEN
S3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

DAGS_DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Home directory
HOME_DIR = Path.cwd().parent

# input data directory
DATA_DIR = Path(HOME_DIR, "data")
DATA_DIR_INPUT = Path(DATA_DIR, "input")
WORLD_BANK_DATA = Path(DATA_DIR_INPUT, "wb_data")
WAQI_DATA = Path(DATA_DIR_INPUT, "waqi_data")

# output data directory
DATA_DIR_OUTPUT = Path(DATA_DIR, "output")