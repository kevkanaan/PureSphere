import os
import glob
from typing import Dict, List
from pyarrow import parquet
import pandas as pd

STAGING_ZONE_PATH = "/opt/airflow/data/staging/air-quality/"
LANDING_ZONE_PATH = "/opt/airflow/data/landing/air-quality/"
# STAGING_ZONE_PATH = "data/staging/air-quality/"
# LANDING_ZONE_PATH = "data/landing/air-quality/"

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
                    "valeur brute|mean": "moyenne",
                    "valeur brute|max": "max",
                    "valeur brute|min": "min",
                    "valeur brute|std": "écart-type",
                    "valeur brute|count": "nombre de mesures",
                    "unité de mesure|<lambda>": "unité de mesure",
                }, axis="columns", errors="raise", inplace=True)

                measurement_report_aggregated_by_station_and_pollutant.to_parquet("/".join([folder_name, report_name]))
