from datetime import datetime
from dateutil.rrule import rrule, MONTHLY

import requests
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

class NyTaxiTripExtractor:

    def __init__(self, 
                 dbutils):
        self.dbutils = dbutils

    def _get_trip_data(self, dt: datetime, url: str, landing_path: str):
        resp = requests.head(url, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"Arquivo nao encontrado para o periodo {dt.strftime('%Y-%m')} (HTTP {resp.status_code}) — pulando.")
            return
        logger.info(f"Baixando {url}")
        self.dbutils.fs.cp(url, landing_path)
    
    def _clear_path_if_exists(self, landing_path: str):
        if os.path.exists(landing_path):
            logger.info(f"Limpando o path {landing_path} para baixar novo arquivo")
            self.dbutils.fs.rm(landing_path, True)
    
    def extract(self, 
                start_date="2023-01-01", 
                end_date="2023-05-01", 
                base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/", 
                file_format="parquet", 
                volume_path="/Volumes/ifood_case/default/landing", 
                trip_type="yellow", 
                sufix_trip_type_url="_tripdata_"):
        for dt in rrule(MONTHLY, dtstart=datetime.strptime(start_date, '%Y-%m-%d'), until=datetime.strptime(end_date, '%Y-%m-%d')):
            logger.info(f"Obtendo {trip_type} trips do periodo {dt.strftime('%Y-%m')}")
            trip_data_url = f"{base_url}{trip_type}{sufix_trip_type_url}{dt.strftime('%Y-%m')}.{file_format}"
            landing_path = f"{volume_path}/ny_taxi_trip/{trip_type}/year={dt.strftime('%Y')}/month={dt.strftime('%m')}/data.{file_format}"
            self._clear_path_if_exists(landing_path)
            self._get_trip_data(dt, trip_data_url, landing_path)
