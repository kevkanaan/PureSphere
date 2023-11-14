import os

from io import StringIO, BytesIO
from typing import List
from bs4 import BeautifulSoup

import requests
import pandas as pd

LANDING_ZONE_PATH = "/opt/airflow/data/landing/air-quality/"

def download_stations_details():
    """Download a CSV file containing tons of details about every measuring station used to acquire data.

    The CSV file is saved here: data/air-quality/stations_metadata.csv.
    """
    r = requests.get("https://static.data.gouv.fr/resources/donnees-temps-reel-de-mesure-des-concentrations-de-polluants-atmospheriques-reglementes-1/20230808-065455/fr-2023-d-lcsqa-ineris-20230717.xls")
    station_metadata_df = pd.read_excel(BytesIO(r.content))
    station_metadata_df.to_csv(LANDING_ZONE_PATH+"stations_metadata.csv", index=False)

def download_daily_reports(years: List[int]):
    """ Download daily measures data for all measuring stations and given years.

    Keyword arguments:
    years -- list of years for which to retrieve data. You can't download data before 2021 (>=2021)

    Measure reports are saved to Parquet format in data/air-quality/{YEAR}/FR_E2_{YYYY}_{MM}_{DD}. Example: FR_E2_2023_03_15
    """
    for year in years:
        if not os.path.exists(LANDING_ZONE_PATH+f"{year}"):
            os.makedirs(LANDING_ZONE_PATH+f"{year}")
        r = requests.get(f"https://files.data.gouv.fr/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/{year}")
        soup = BeautifulSoup(r.content, features="html.parser")
        for a in soup.select('a[href*=csv]'):
            filename = a['href']
            if not os.path.exists(LANDING_ZONE_PATH+f"{year}/{filename[:-4]}"):
                r = requests.get(f"https://files.data.gouv.fr/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/{year}/{filename}")
                decoded_file = StringIO(r.content.decode("utf-8"))
                report_df = pd.read_csv(decoded_file, sep=";")
                report_df.to_parquet(LANDING_ZONE_PATH+f"{year}/{filename[:-4]}")
