"""
This script applies some transformations to the Géorisques data
to move them from the landing zone to the staging zone.
"""

import json
import os
from distutils.dir_util import copy_tree
from airflow.exceptions import AirflowSkipException
import pandas
from georisques.constants import DATA_PATH, KEPT_FILES
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def get_folder_path(step_index: int, step_name: str) -> str:
    """Returns the staging folder path for a given step"""
    return f"{DATA_PATH['STAGING_ZONE']}{step_index}_{step_name}"


def copy_folder(source: str, destination: str) -> None:
    copy_tree(source, destination)

    if os.path.exists(f'{destination}/.gitignore'):
        os.remove(f'{destination}/.gitignore')


def remove_unwanted_files(working_folder: str):
    for sub_folder, _, files in os.walk(working_folder):
        for file in files:
            if file not in KEPT_FILES:
                os.remove(os.path.join(sub_folder, file))


def remove_unwanted_columns(working_folder: str):
    for sub_folder in os.listdir(working_folder):
        folder_path = os.path.join(working_folder, sub_folder)

        etablissements_csv_path = os.path.join(folder_path, 'etablissements.csv')
        etablissements_df = pandas.read_csv(etablissements_csv_path, sep=';', dtype={'code_postal': str})
        etablissements_df = etablissements_df.drop(columns=['numero_siret', 'code_epsg', 'code_ape', 'libelle_ape', 'code_eprtr', 'libelle_eprtr'])
        etablissements_df.to_csv(etablissements_csv_path, index=False)

        emissions_csv_path = os.path.join(folder_path, 'emissions.csv')
        emissions_df = pandas.read_csv(emissions_csv_path, sep=';')
        emissions_df = emissions_df.drop(columns=['annee_emission'])
        emissions_df.to_csv(emissions_csv_path, index=False)

        rejets_csv_path = os.path.join(folder_path, 'rejets.csv')
        rejets_df = pandas.read_csv(rejets_csv_path, sep=';')
        rejets_df = rejets_df.drop(columns=['annee_rejet'])
        rejets_df.to_csv(rejets_csv_path, index=False)


def strip_columns_name(working_folder: str):
    for sub_folder, _, files in os.walk(working_folder):
        for file in files:
            df = pandas.read_csv(os.path.join(sub_folder, file), dtype={'code_postal': str})
            df.columns = df.columns.str.strip()
            df.to_csv(os.path.join(sub_folder, file), index=False)


def strip_data(working_folder: str):
    for sub_folder, _, files in os.walk(working_folder):
        for file in files:
            df = pandas.read_csv(os.path.join(sub_folder, file), dtype={'code_postal': str})
            df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            df.to_csv(os.path.join(sub_folder, file), index=False)


def retrieve_coordinates(working_folder: str):
    geolocator = Nominatim(user_agent='PureSphere')
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    try:
        with open('data.json', encoding='utf-8') as json_file:
            cached_addresses = json.load(json_file)
    except FileNotFoundError:
        cached_addresses = {}

    def find_coordinates(row: pandas.Series) -> pandas.Series:
        keys = ['adresse', 'code_postal', 'commune', 'departement', 'region']

        address = ' '.join([str(row[key]) for key in keys])

        if address in cached_addresses:
            return pandas.Series(cached_addresses[address])

        location = geocode(address)

        while location is None: # the API doesn't find the location, we retry with a less precise address
            keys = keys[1:]
            new_address = ' '.join([str(row[key]) for key in keys])
            location = geocode(new_address)

        coordinates = [location.latitude, location.longitude]
        cached_addresses[address] = coordinates

        return pandas.Series(coordinates)


    for sub_folder in os.listdir(working_folder):
        folder_path = os.path.join(working_folder, sub_folder)
        etablissements_csv_path = os.path.join(folder_path, 'etablissements.csv')
        etablissements_df = pandas.read_csv(etablissements_csv_path, dtype={'code_postal': str})
        etablissements_df[['latitude', 'longitude']] = etablissements_df.apply(
            find_coordinates, axis=1
        )
        etablissements_df.to_csv(etablissements_csv_path, index=False)

    with open('data.json', 'w', encoding='utf-8') as outfile:
        json.dump(cached_addresses, outfile)


STEPS = [
    remove_unwanted_files,
    remove_unwanted_columns,
    strip_columns_name,
    strip_data,
    retrieve_coordinates,
]

def execute_step(step_index: int):
    step = STEPS[step_index - 1]
    step_name = step.__name__
    step_folder = get_folder_path(step_index, step_name)

    # check if the step has already been applied
    if os.path.isdir(step_folder):
        raise AirflowSkipException(f'Step {step_index} has already been applied, please delete the {step_folder} folder to reapply it')

    # create the step folder from the previous step
    if step_index == 1: # if it's the first step, we retrieve the landing zone data
        copy_folder(source=DATA_PATH['LANDING_ZONE'], destination=step_folder)
    else:
        previous_step_folder = get_folder_path(step_index - 1, STEPS[step_index - 2].__name__)
        copy_folder(source=previous_step_folder, destination=step_folder)

    # apply wrangling step
    print(f'Applying step {step_index}...')
    step(working_folder=step_folder)
    print(f'Step {step_index} has been applied')


if __name__ == '__main__':
    for index in range(1, len(STEPS) + 1):
        execute_step(index)
