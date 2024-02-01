"""
This script applies some transformations to the Géorisques data
to move them from the landing zone to the staging zone.
"""

import json
import os
from distutils.dir_util import copy_tree
from airflow.exceptions import AirflowSkipException
import pandas
import sqlalchemy
from sqlalchemy_utils import create_database, database_exists
from georisques.constants import DATA_PATH, KEPT_FILES, STUDIED_YEAR
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


def get_cached_addresses() -> dict:
    try:
        with open(DATA_PATH['STAGING_ZONE'] + 'addresses.json', encoding='utf-8') as json_file:
            return json.load(json_file)
    except FileNotFoundError:
        return {}


def save_cached_addresses(cached_addresses: dict) -> None:
    with open(DATA_PATH['STAGING_ZONE'] + 'addresses.json', 'w', encoding='utf-8') as outfile:
        json.dump(cached_addresses, outfile, ensure_ascii=False)


def retrieve_coordinates(working_folder: str):
    geolocator = Nominatim(user_agent='PureSphere')
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    cached_addresses = get_cached_addresses()

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

    save_cached_addresses(cached_addresses)

def create_georisques_etablissements_studied_year_sql_table(working_folder: str):
    etablissements_studied_year_df = pandas.read_csv(os.path.join(working_folder, STUDIED_YEAR ,'etablissements.csv'), dtype={'code_postal': str})
    engine = sqlalchemy.create_engine("postgresql://airflow:airflow@postgres:5432/staging")
    if not database_exists(engine.url):
        create_database(engine.url)
    table_name = "georisques_etablissements_"+STUDIED_YEAR
    if not sqlalchemy.inspect(engine).has_table(table_name):
        with engine.connect() as conn:
            etablissements_studied_year_df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY(identifiant);")
    else:
        existing_etablissements = pandas.read_sql_table(table_name, engine)
        new_etablissements = pandas.concat([etablissements_studied_year_df, existing_etablissements]).drop_duplicates(keep=False)
        if not new_etablissements.empty:
            with engine.connect() as conn:
                new_etablissements.to_sql(table_name, conn, if_exists="append", index=False)

def create_georisques_emissions_studied_year_sql_table(working_folder: str):
    emissions_studied_year_df = pandas.read_csv(os.path.join(working_folder, STUDIED_YEAR ,'emissions.csv'), dtype={'code_postal': str})

    engine = sqlalchemy.create_engine("postgresql://airflow:airflow@postgres:5432/staging")
    if not database_exists(engine.url):
        create_database(engine.url)
    # Drop the emissions linked to an etablissement that is not in the studied year etablissements table
    result = engine.execute(f"SELECT identifiant FROM georisques_etablissements_{STUDIED_YEAR};").fetchall()
    etablissements_ids = [id[0] for id in result]
    emissions_studied_year_df = emissions_studied_year_df[emissions_studied_year_df['identifiant'].isin(etablissements_ids)]

    table_name = "georisques_emissions_"+STUDIED_YEAR
    if not sqlalchemy.inspect(engine).has_table(table_name):
        with engine.connect() as conn:
            emissions_studied_year_df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY(identifiant, milieu, polluant);")
            conn.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT fk_{table_name} FOREIGN KEY(identifiant) REFERENCES georisques_etablissements_{STUDIED_YEAR}(identifiant);")
    else:
        existing_emissions = pandas.read_sql_table(table_name, engine)
        new_emissions = pandas.concat([emissions_studied_year_df, existing_emissions]).drop_duplicates(keep=False)
        if not new_emissions.empty:
            with engine.connect() as conn:
                new_emissions.to_sql(table_name, conn, if_exists="append", index=False)

def create_georisques_rejets_studied_year_sql_table(working_folder: str):
    rejets_studied_year_df = pandas.read_csv(os.path.join(working_folder, STUDIED_YEAR ,'rejets.csv'), dtype={'code_postal': str})
    engine = sqlalchemy.create_engine("postgresql://airflow:airflow@postgres:5432/staging")
    if not database_exists(engine.url):
        create_database(engine.url)
    # Drop the rejets linked to an etablissement that is not in the studied year etablissements table
    result = engine.execute(f"SELECT identifiant FROM georisques_etablissements_{STUDIED_YEAR};").fetchall()
    etablissements_ids = [id[0] for id in result]
    rejets_studied_year_df = rejets_studied_year_df[rejets_studied_year_df['identifiant'].isin(etablissements_ids)]

    table_name = "georisques_rejets_"+STUDIED_YEAR
    if not sqlalchemy.inspect(engine).has_table(table_name):
        with engine.connect() as conn:
            rejets_studied_year_df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY(identifiant);")
            conn.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT fk_{table_name} FOREIGN KEY(identifiant) REFERENCES georisques_etablissements_{STUDIED_YEAR}(identifiant);")
    else:
        existing_rejets = pandas.read_sql_table(table_name, engine)
        new_rejets = pandas.concat([rejets_studied_year_df, existing_rejets]).drop_duplicates(keep=False)
        if not new_rejets.empty:
            with engine.connect() as conn:
                new_rejets.to_sql(table_name, conn, if_exists="append", index=False)

STEPS = [
    remove_unwanted_files,
    remove_unwanted_columns,
    strip_columns_name,
    strip_data,
    retrieve_coordinates,
    create_georisques_etablissements_studied_year_sql_table,
    create_georisques_emissions_studied_year_sql_table,
    create_georisques_rejets_studied_year_sql_table,
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
