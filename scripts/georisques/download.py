"""
This script downloads and extracts all the available archives from Géorisques.
"""

import io
import pathlib
import requests
import zipfile

session = requests.Session()

available_reports = session.get('https://georisques.gouv.fr/webappReport/ws/telechargement/irep').json()

for year, report in available_reports['annuel'].items():
    if not report: # report not available (e.g. 2022 and 2023)
        continue

    if pathlib.Path(f'data/georisques/{year}').exists():
        print(f'{year} already downloaded')
        continue

    print(f'Downloading {year}')
    link = report['lien']

    response = session.get(link)
    z = zipfile.ZipFile(io.BytesIO(response.content))
    z.extractall('data/georisques')
