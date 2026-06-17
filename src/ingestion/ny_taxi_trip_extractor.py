from datetime import datetime
from datetime import timedelta
from dateutil.rrule import rrule, MONTHLY
from pyspark.sql import functions as F

import requests
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger("ny_taxi_trip_extractor")

class NyTaxiTripExtractor:

    def __init__(self, 
                 dbutils,
                 start_date="2023-01-01", 
                 end_date="2023-05-01", 
                 base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/", 
                 file_format="parquet", 
                 volume_path="/Volumes/ifood_case/default/landing", 
                 trip_type="yellow", 
                 sufix_trip_type_url="_tripdata_"):
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.base_url = base_url
        self.file_format = file_format
        self.volume_path = volume_path
        self.trip_type = trip_type
        self.sufix_trip_type_url = sufix_trip_type_url
        self.dbutils = dbutils

    def _get_trip_data(self, dt: datetime, url: str, landing_path: str):
        resp = requests.head(url, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"Data not found for {dt.strftime('%Y-%m')} (HTTP {resp.status_code}) — skipping.")
            return
        logger.info(f"Downloading {url}")
        self.dbutils.fs.cp(url, landing_path)
    
    def _clear_path_if_exists(self, landing_path: str):
        if os.path.exists(landing_path):
            logger.info(f"Clear {landing_path} to download new data")
            self.dbutils.fs.rm(landing_path, True)
    
    def extract(self):
        for dt in rrule(MONTHLY, dtstart=self.start_date, until=self.end_date):
            logger.info(f"Getting {self.trip_type} trips data from {dt.strftime('%Y-%m')}")
            trip_data_url = f"{self.base_url}{self.trip_type}{self.sufix_trip_type_url}{dt.strftime('%Y-%m')}.{self.file_format}"
            landing_path = f"{self.volume_path}/ny_taxi_trip/{self.trip_type}/year={dt.strftime('%Y')}/month={dt.strftime('%m')}/data.{self.file_format}"
            self._clear_path_if_exists(landing_path)
            self._get_trip_data(dt, trip_data_url, landing_path)
