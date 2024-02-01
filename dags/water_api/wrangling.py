import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy_utils import create_database, database_exists

LANDING_ZONE_PATH = "/opt/airflow/data/landing/water-quality/"
STAGING_ZONE_PATH = "/opt/airflow/data/staging/water-quality/"
# LANDING_ZONE_PATH = "./data/landing/water-quality/"
# STAGING_ZONE_PATH = "./data/staging/water-quality/"
def get_stations_metadata_dataframe():
    return pd.read_csv(LANDING_ZONE_PATH+"stationpc.csv", sep=";")

def wrangle_file():
    file_path = LANDING_ZONE_PATH + "analysispc_2021"
    table = pq.read_table(file_path)
    df = table.to_pandas()
    return wrangle_data(df)

def wrangle_data(df):
    cols = list(df.columns.values)
    columns_we_want = ["code_station","libelle_station","libelle_fraction","date_prelevement","libelle_parametre","resultat","symbole_unite","mnemo_remarque","limite_detection","limite_quantification","limite_saturation","libelle_qualification","code_laboratoire"]
    df = df.drop(columns=[col for col in cols if col not in columns_we_want])
    df = clean_resultat(df)
    return daily_aggregation(df)

def clean_resultat(df):
    df['resultat'] = df['resultat'].str.replace(r'\D', '')
    df['resultat'] = pd.to_numeric(df['resultat'], errors='coerce')
    df = df.dropna(subset=['resultat'])

    return df

def daily_aggregation(df):
    df['date_prelevement'] = pd.to_datetime(df['date_prelevement'])
    df['daily_mean'] = df.groupby([df['date_prelevement'].dt.date, 'libelle_parametre'])['resultat'].transform(lambda x: round(x.mean(), 3))
    return rename_columns(df)

def rename_columns(df):
    df = df.rename(columns={'code_station':'station_code','libelle_station':'station_name','libelle_fraction':'fraction','date_prelevement':'date','libelle_qualification':'qualification','code_laboratoire':'laboratory_code'})
    df.to_csv(STAGING_ZONE_PATH + "analysispcwrangling.csv", sep=";", index=False, encoding="ISO-8859-1")
    return df

def create_water_quality_station_sql_table():
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/staging")
    if not database_exists(engine.url):
        create_database(engine.url)
    # Get the stations metadata and clean it
    stations_metadata = get_stations_metadata_dataframe()
    stations_metadata.fillna("NULL", inplace=True)
    stations_metadata.replace(to_replace="'", value="-", inplace=True, regex=True)
    stations_metadata.drop_duplicates(subset=["code_station"], inplace=True)
    if not sqlalchemy.inspect(engine).has_table("water_quality_stations"):
        with engine.connect() as conn:
            stations_metadata.to_sql("water_quality_stations", conn, if_exists="replace", index=False)
            conn.execute("ALTER TABLE water_quality_stations ADD CONSTRAINT pk_wq_stations PRIMARY KEY(code_station);")
    else:
        existing_stations = pd.read_sql_table("water_quality_stations", engine)
        new_stations = pd.concat([existing_stations, stations_metadata]).drop_duplicates("code_station", keep=False)
        if not new_stations.empty:
            with engine.connect() as conn:
                new_stations.to_sql("water_quality_stations", conn, if_exists="append", index=False)

def create_water_quality_measurements_sql_table():
    # Create the staging database if it does not exist
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/staging")
    if not database_exists(engine.url):
        create_database(engine.url)
    # Create the air_quality_measurements table
    measurements_cleaned_data = pd.read_csv(STAGING_ZONE_PATH+"analysispcwrangling.csv", sep=";", encoding="utf-8")
    # Retrieve the stations codes from the stations table
    result = engine.execute("SELECT code_station FROM water_quality_stations;").fetchall()
    stations_code = [code[0] for code in result]
    measurements_cleaned_data = measurements_cleaned_data[measurements_cleaned_data['station_code'].isin(stations_code)]
    measurements_cleaned_data.drop_duplicates(subset=["station_code", "date", "libelle_parametre"], inplace=True)
    measurements_cleaned_data.rename(columns={"station_code":"code_station"}, inplace=True)
    measurements_cleaned_data["date"] = measurements_cleaned_data["date"].apply(lambda x: "/".join([x.split("-")[-1], x.split("-")[1], x.split("-")[0]]))
    if not sqlalchemy.inspect(engine).has_table("water_quality_measurements"):
        with engine.connect() as conn:
            measurements_cleaned_data.to_sql("water_quality_measurements", conn, if_exists="replace", index=False)
            conn.execute("ALTER TABLE water_quality_measurements ALTER COLUMN date TYPE DATE USING to_date(date, 'DD/MM/YYYY');")
            conn.execute("ALTER TABLE water_quality_measurements ADD CONSTRAINT pk_wq_measurements PRIMARY KEY(code_station, date, libelle_parametre);")
            conn.execute("ALTER TABLE water_quality_measurements ADD CONSTRAINT fk_wq_measurements FOREIGN KEY(code_station) REFERENCES water_quality_stations(code_station);")
    else:
        existing_measurements = pd.read_sql_table("water_quality_measurements", engine)
        new_measurements = pd.concat([existing_measurements, measurements_cleaned_data]).drop_duplicates(['station_code', 'date', 'libelle_parametre'], keep=False)
        if not new_measurements.empty:
            with engine.connect() as conn:
                new_measurements.to_sql("water_quality_measurements", conn, if_exists="append", index=False)
