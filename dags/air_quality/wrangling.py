from hmac import new
import os
import glob
from typing import Dict, List
from pyarrow import parquet
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy_utils import create_database, database_exists

STAGING_ZONE_PATH = "/opt/airflow/data/staging/air-quality/"
LANDING_ZONE_PATH = "/opt/airflow/data/landing/air-quality/"

def get_stations_metadata_dataframe():
    return pd.read_csv(LANDING_ZONE_PATH+"stations_metadata.csv")

def get_landing_zone_files():
    files = [file for file in glob.glob(LANDING_ZONE_PATH+r"*") if str(file[-4:]).isdigit()]
    measurement_reports_per_year: Dict[int, List[str]] = {}
    for file in files:
        measurement_reports = glob.glob(file+"/FR_E2_*")
        measurement_reports_per_year[int(file[-4:])] = measurement_reports
    return measurement_reports_per_year

def get_previous_wrangling_step_files(previous_step: str):
    files = list(glob.glob(STAGING_ZONE_PATH+f"{previous_step}/*"))
    measurement_reports_per_year: Dict[int, List[str]] = {}
    for file in files:
        measurement_reports = glob.glob(file+"/FR_E2_*")
        measurement_reports_per_year[int(file[-4:])] = measurement_reports
    return measurement_reports_per_year

def remove_invalid_measurements():
    measurement_reports_per_year = get_landing_zone_files()
    for year, measurement_reports in measurement_reports_per_year.items():
        folder_name = STAGING_ZONE_PATH+f"invalid_measurements_removed/{year}"
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        for measurement_report in measurement_reports:
            report_name = measurement_report[-16:]
            if not os.path.exists("/".join([folder_name, report_name])):
                measurement_report_table = parquet.read_table(measurement_report)
                measurement_report_df = measurement_report_table.to_pandas()
                index_invalid_measurements = measurement_report_df[measurement_report_df["valeur brute"].isna()
                                                                    | ~(measurement_report_df["type d'évaluation"] == "mesures fixes")
                                                                    | ~(measurement_report_df["code qualité"] == "A")
                                                                    | (measurement_report_df["validité"] == -1)].index
                measurement_report_df_without_invalid_measurements = measurement_report_df.drop(index_invalid_measurements)
                measurement_report_df_without_invalid_measurements.to_parquet("/".join([folder_name, report_name]))

def drop_useless_columns():
    measurement_reports_per_year = get_previous_wrangling_step_files("invalid_measurements_removed")
    for year, measurement_reports in measurement_reports_per_year.items():
        folder_name = STAGING_ZONE_PATH+f"useless_columns_dropped/{year}"
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        for measurement_report in measurement_reports:
            report_name = measurement_report[-16:]
            if not os.path.exists("/".join([folder_name, report_name])):
                measurement_report_table = parquet.read_table(measurement_report)
                measurement_report_table_columns_dropped = measurement_report_table.select(["code site", "Polluant", "valeur brute", "unité de mesure"])
                parquet.write_table(measurement_report_table_columns_dropped, "/".join([folder_name, report_name]))

def aggregate_measurement_by_site_code_and_pollutant_type():
    measurement_reports_per_year = get_previous_wrangling_step_files("useless_columns_dropped")
    for year, measurement_reports in measurement_reports_per_year.items():
        folder_name = STAGING_ZONE_PATH+f"aggregated_by_code_site_and_pollutant/{year}"
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        for measurement_report in measurement_reports:
            report_name = measurement_report[-16:]
            if not os.path.exists("/".join([folder_name, report_name])):
                measurement_report_table = parquet.read_table(measurement_report)
                measurement_report_df = measurement_report_table.to_pandas()
                # Aggregate by code site and pollutant
                measurement_report_aggregated_by_station_and_pollutant = measurement_report_df.groupby(["code site", "Polluant"])
                # Get daily average, min, max, std along with measureùent unit
                measurement_report_aggregated_by_station_and_pollutant = measurement_report_aggregated_by_station_and_pollutant.agg({"valeur brute": ["mean", "min", "max", "std", "count"],
                                                                                  "unité de mesure" : lambda x: x.unique()[0].replace("-", "/")}).reset_index()
                # Flatten multi-index columns
                measurement_report_aggregated_by_station_and_pollutant.columns = measurement_report_aggregated_by_station_and_pollutant.columns.map('|'.join).str.strip('|')
                # Rename columns
                measurement_report_aggregated_by_station_and_pollutant.rename({
                    "valeur brute|mean": "mean",
                    "valeur brute|max": "max",
                    "valeur brute|min": "min",
                    "valeur brute|std": "std",
                    "valeur brute|count": "number_of_measurements",
                    "unité de mesure|<lambda>": "measurement_unit",
                }, axis="columns", errors="raise", inplace=True)

                measurement_report_aggregated_by_station_and_pollutant.to_parquet("/".join([folder_name, report_name]))

def create_air_quality_station_sql_table():
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/staging")
    if not database_exists(engine.url):
        create_database(engine.url)
    # Get the stations metadata and clean it
    stations_metadata = get_stations_metadata_dataframe()
    stations_metadata.rename({"GMLID": "gmlid",
                              "LocalId": "local_id",
                              "Namespace": "namespace",
                              "Version": "version",
                              "NatlStationCode": "national_station_code",
                              "Name": "name",
                              "Municipality": "municipality",
                              "EUStationCode": "eu_station_code",
                              "ActivityBegin": "activity_begin",
                              "ActivityEnd": "activity_end",
                              "Latitude": "latitude",
                              "Longitude": "longitude",
                              "SRSName": "srs_name",
                              "Altitude": "altitude",
                              "AltitudeUnit": "altitude_unit",
                              "AreaClassification": "area_classification",
                              "BelongsTo": "belongs_to"}, axis="columns", inplace=True)
    stations_metadata.fillna("NULL", inplace=True)
    stations_metadata.replace(to_replace="'", value="-", inplace=True, regex=True)
    stations_metadata.drop_duplicates(subset=["national_station_code"], inplace=True)
    if not sqlalchemy.inspect(engine).has_table("air_quality_stations"):
        with engine.connect() as conn:
            stations_metadata.to_sql("air_quality_stations", conn, if_exists="replace", index=False)
            conn.execute("ALTER TABLE air_quality_stations ADD CONSTRAINT pk_aq_stations PRIMARY KEY(national_station_code);")
    else:
        existing_stations = pd.read_sql_table("air_quality_stations", engine)
        new_stations = pd.concat([existing_stations, stations_metadata]).drop_duplicates(keep=False)
        if not new_stations.empty:
            with engine.connect() as conn:
                new_stations.to_sql("air_quality_stations", conn, if_exists="append", index=False)

def create_air_quality_measurements_sql_table():
    # Create the staging database if it does not exist
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/staging")
    if not database_exists(engine.url):
        create_database(engine.url)
    # Create the air_quality_measurements table
    measurements_cleaned_data = pd.read_parquet(STAGING_ZONE_PATH+"measurements_file.parquet")
    measurements_cleaned_data.rename({"code site": "national_station_code",
                                      "Polluant": "pollutant",
                                      "moyenne": "mean",
                                      "min": "min",
                                      "max": "max",
                                      "écart-type": "std",
                                      "nombre de mesures": "number_of_measurements",
                                      "unité de mesure": "measurement_unit",
                                      "Date": "date"}, axis="columns", inplace=True)
    # Retrieve the stations codes from the stations table
    result = engine.execute("SELECT national_station_code FROM air_quality_stations;").fetchall()
    stations_code = [code[0] for code in result]
    measurements_cleaned_data = measurements_cleaned_data[measurements_cleaned_data['national_station_code'].isin(stations_code)]
    if not sqlalchemy.inspect(engine).has_table("air_quality_measurements"):
        with engine.connect() as conn:
            measurements_cleaned_data.to_sql("air_quality_measurements", conn, if_exists="replace", index=False)
            conn.execute("ALTER TABLE air_quality_measurements ADD CONSTRAINT pk_aq_measurements PRIMARY KEY(national_station_code, pollutant, date);")
            conn.execute("ALTER TABLE air_quality_measurements ADD CONSTRAINT fk_aq_measurements FOREIGN KEY(national_station_code) REFERENCES air_quality_stations(national_station_code);")
    else:
        existing_measurements = pd.read_sql_table("air_quality_measurements", engine)
        new_measurements = pd.concat([existing_measurements, measurements_cleaned_data]).drop_duplicates(keep=False)
        if not new_measurements.empty:
            with engine.connect() as conn:
                new_measurements.to_sql("air_quality_measurements", conn, if_exists="append", index=False)