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

LANDING_ZONE_PATH = '/opt/airflow/data/landing/georisques/'
RETRIEVED_YEAR = '2021'

def download_data() -> None:
    session = requests.Session()

    available_reports = session.get('https://georisques.gouv.fr/webappReport/ws/telechargement/irep').json()
    report = available_reports['annuel'][RETRIEVED_YEAR]

    if pathlib.Path(LANDING_ZONE_PATH+f'{RETRIEVED_YEAR}').exists():
        print(f'{RETRIEVED_YEAR} data already downloaded')
        return

    print(f'Downloading {RETRIEVED_YEAR} data')
    link = report['lien']

    response = session.get(link)
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(LANDING_ZONE_PATH)
