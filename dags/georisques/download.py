"""
This script downloads and extracts the data from Géorisques.

We just retrieve the 2021 data, as it is the most recent one available
and we don't retrieve the previous years, as the air-quality dataset
starts in 2021.
"""

import io
import pathlib
import zipfile
import requests
from georisques.constants import DATA_PATH, RETRIEVE_FROM_YEAR

def download_data() -> None:
    session = requests.Session()

    available_reports = session.get('https://georisques.gouv.fr/webappReport/ws/telechargement/irep').json()

    for year, report in available_reports['annuel'].items():
        if year < RETRIEVE_FROM_YEAR or not report: # report too old or not available (e.g. 2023)
            continue

        if pathlib.Path(DATA_PATH['LANDING_ZONE'] + f'{year}').exists():
            print(f'{year} already downloaded')
            continue

        print(f'Downloading {year} data')
        link = report['lien']

        response = session.get(link)
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            archive.extractall(DATA_PATH['LANDING_ZONE'])


if __name__ == '__main__':
    download_data()
