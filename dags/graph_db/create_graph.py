import py2neo
import sqlalchemy
import pandas as pd
from geopy import distance
from sqlalchemy_utils import create_database, database_exists
import georisques.constants as constants

RADIUS = 10.0       # Radius to map a monitoring station to an industrial site

def find_air_quality_stations_in_radius(center, radius:float):
    engine = sqlalchemy.create_engine('postgresql://airflow:airflow@postgres:5432/staging')
    conn = engine.connect()
    air_quality_stations_df = pd.read_sql_query(f"""SELECT national_station_code, latitude, longitude
                                            FROM air_quality_stations""", conn)
    air_quality_stations_df["distance"] = air_quality_stations_df.apply(lambda row: distance.distance(center, (row["latitude"], row["longitude"])).km, axis=1)
    air_quality_stations_df = air_quality_stations_df[air_quality_stations_df["distance"] <= radius]
    conn.close()
    engine.dispose()
    return air_quality_stations_df["national_station_code"].tolist()

def find_water_quality_stations_in_radius(center, radius:float):
    engine = sqlalchemy.create_engine('postgresql://airflow:airflow@postgres:5432/staging')
    conn = engine.connect()
    water_quality_stations_df = pd.read_sql_query(f"""SELECT code_station, libelle_station, latitude, longitude
                                                FROM water_quality_stations""", conn)
    water_quality_stations_df["distance"] = water_quality_stations_df.apply(lambda row: distance.distance(center, (row["latitude"], row["longitude"])).km, axis=1)
    water_quality_stations_df = water_quality_stations_df[water_quality_stations_df["distance"] <= radius]
    conn.close()
    engine.dispose()
    return water_quality_stations_df["code_station"].tolist()

def identify_monitoring_stations_for_industrial_site():
    prod_engine = sqlalchemy.create_engine("postgresql://airflow:airflow@postgres:5432/production")
    stag_engine = sqlalchemy.create_engine("postgresql://airflow:airflow@postgres:5432/staging")
    if not database_exists(prod_engine.url):
        create_database(prod_engine.url)
    if not sqlalchemy.inspect(prod_engine).has_table(f"industrial_sites_monitoring_stations_{constants.STUDIED_YEAR}"):
        with stag_engine.connect() as conn:
            industrial_sites_df = pd.read_sql_query(f"""SELECT identifiant, nom_etablissement, latitude, longitude
                                                FROM georisques_etablissements_{constants.STUDIED_YEAR}""", conn)
            industrial_sites_df["air_quality_stations"] = industrial_sites_df.apply(lambda row: find_air_quality_stations_in_radius((row["latitude"], row["longitude"]), 10.0), axis=1)
            industrial_sites_df["water_quality_stations"] = industrial_sites_df.apply(lambda row: find_water_quality_stations_in_radius((row["latitude"], row["longitude"]), 10.0), axis=1)
            industrial_sites_df.to_sql(f"industrial_sites_monitoring_stations_{constants.STUDIED_YEAR}", conn, if_exists="replace", index=False)

def create_graph_db():
    engine = sqlalchemy.create_engine('postgresql://airflow:airflow@postgres:5432/staging')
    industrial_sites = engine.execute(f"""SELECT identifiant, nom_etablissement, latitude, longitude
                                        FROM georisques_etablissements_{constants.STUDIED_YEAR}""").fetchall()
    
    water_quality_stations = engine.execute(f"""SELECT code_station, libelle_station, latitude, longitude
                                                FROM water_quality_stations""").fetchall()
    
    air_quality_stations = engine.execute(f"""SELECT national_station_code, name, latitude, longitude
                                            FROM air_quality_stations""").fetchall()
    
    industrial_sites_label = ["IndustrialSite"]
    water_quality_stations_label = ["Station", "WaterQuality"]
    air_quality_stations_label = ["Station", "AirQuality"]

    graph_nodes = []

    for industrial_site in industrial_sites:
        graph_nodes.append(py2neo.Node(*industrial_sites_label, **dict(industrial_site)))

    for water_quality_station in water_quality_stations:
        graph_nodes.append(py2neo.Node(*water_quality_stations_label, **dict(water_quality_station)))
    
    for air_quality_station in air_quality_stations:
        graph_nodes.append(py2neo.Node(*air_quality_stations_label, **dict(air_quality_station)))

    graph = py2neo.Graph("bolt://neo:7687")
    graph.delete_all()
    tx = graph.begin()
    for node in graph_nodes:
        tx.create(node)
    tx.commit()
