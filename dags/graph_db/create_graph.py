import os
import py2neo
import sqlalchemy
import pandas as pd
from geopy import distance
from sqlalchemy_utils import create_database, database_exists
from georisques import constants

PRODUCTION_ZONE = '/opt/airflow/data/production/'
RADIUS = 10.0       # Radius to map a monitoring station to an industrial site

def find_air_quality_stations_in_radius(center, radius:float):
    engine = sqlalchemy.create_engine('postgresql://airflow:airflow@postgres:5432/staging')
    conn = engine.connect()
    air_quality_stations_df = pd.read_sql_query("""SELECT national_station_code, latitude, longitude
                                            FROM air_quality_stations""", conn)
    air_quality_stations_df["distance"] = air_quality_stations_df.apply(lambda row: distance.distance(center, (row["latitude"], row["longitude"])).km, axis=1)
    air_quality_stations_df = air_quality_stations_df[air_quality_stations_df["distance"] <= radius]
    conn.close()
    engine.dispose()
    return air_quality_stations_df["national_station_code"].tolist()

def find_water_quality_stations_in_radius(center, radius:float):
    engine = sqlalchemy.create_engine('postgresql://airflow:airflow@postgres:5432/staging')
    conn = engine.connect()
    water_quality_stations_df = pd.read_sql_query("""SELECT code_station, libelle_station, latitude, longitude
                                                FROM water_quality_stations""", conn)
    water_quality_stations_df["distance"] = water_quality_stations_df.apply(lambda row: distance.distance(center, (row["latitude"], row["longitude"])).km, axis=1)
    water_quality_stations_df = water_quality_stations_df[water_quality_stations_df["distance"] <= radius]
    conn.close()
    engine.dispose()
    return water_quality_stations_df["code_station"].tolist()

def create_monitoring_stations_sql_table():
    prod_engine = sqlalchemy.create_engine("postgresql://airflow:airflow@postgres:5432/production")
    stag_engine = sqlalchemy.create_engine("postgresql://airflow:airflow@postgres:5432/staging")

    # Read stations table from staging database and prepare them to be concatened using Pandas
    water_quality_stations_df = pd.read_sql_query("""SELECT code_station AS identifiant, libelle_station AS name, latitude, longitude
                                                FROM water_quality_stations""", stag_engine)
    air_quality_stations_df = pd.read_sql_query("""SELECT national_station_code AS identifiant, name, latitude, longitude, activity_begin, activity_end
                                            FROM air_quality_stations""", stag_engine)
    water_quality_stations_df["type"] = "water_quality"
    air_quality_stations_df["type"] = "air_quality"
    # Filter out air quality stations that were not working in studied year
    air_quality_stations_df['activity_begin'] = pd.to_datetime(air_quality_stations_df['activity_begin'], format="ISO8601", errors="coerce")
    air_quality_stations_df['activity_end'] = pd.to_datetime(air_quality_stations_df['activity_end'], format="ISO8601", errors="coerce")

    # Define the start and end of studied year
    start_date = pd.to_datetime(f'{constants.STUDIED_YEAR}-01-01T00:00:00+01:00', format="ISO8601", errors="ignore")
    end_date = pd.to_datetime(f'{constants.STUDIED_YEAR}-12-31T23:59:59+01:00', format="ISO8601", errors="ignore")

    # Drop the rows where the entity wasn't working in studied year
    air_quality_stations_df = air_quality_stations_df[((air_quality_stations_df['activity_begin'] <= end_date) & ((air_quality_stations_df['activity_end'] >= start_date) | (air_quality_stations_df['activity_end'].isna())))]
    air_quality_stations_df.drop(["activity_begin", "activity_end"], axis=1, inplace=True)

    monitoring_stations_df = pd.concat([air_quality_stations_df, water_quality_stations_df])

    if not database_exists(prod_engine.url):
        create_database(prod_engine.url)
    table_name = f"monitoring_stations_{constants.STUDIED_YEAR}"
    if not sqlalchemy.inspect(prod_engine).has_table(table_name):
        with prod_engine.connect() as conn:
            monitoring_stations_df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY(identifiant, type);")
    else:
        existing_stations_df = pd.read_sql_table(table_name, prod_engine)
        new_stations = pd.concat([monitoring_stations_df, existing_stations_df]).drop_duplicates(keep=False)
        if not new_stations.empty:
            with prod_engine.connect() as conn:
                new_stations.to_sql(table_name, conn, if_exists="append", index=False)
    prod_engine.dispose()
    stag_engine.dispose()

def identify_monitoring_stations_for_industrial_site():
    prod_engine = sqlalchemy.create_engine("postgresql://airflow:airflow@postgres:5432/production")
    stag_engine = sqlalchemy.create_engine("postgresql://airflow:airflow@postgres:5432/staging")
    if not database_exists(prod_engine.url):
        create_database(prod_engine.url)
    if not sqlalchemy.inspect(prod_engine).has_table(f"industrial_sites_monitoring_stations_{constants.STUDIED_YEAR}"):
        with stag_engine.connect() as conn:
            if os.path.isfile(PRODUCTION_ZONE+'industrial_sites_monitoring_stations_2021.csv'):
                industrial_sites_df = pd.read_csv(PRODUCTION_ZONE+f'industrial_sites_monitoring_stations_{constants.STUDIED_YEAR}.csv', sep=",", encoding="utf-8")
                industrial_sites_df.columns = ["identifiant", "nom_etablissement", "latitude", "longitude", "air_quality_stations", "water_quality_stations"]
                industrial_sites_df.to_sql(f"industrial_sites_monitoring_stations_{constants.STUDIED_YEAR}", conn, if_exists="replace", index=False)
            else:
                industrial_sites_df = pd.read_sql_query(f"""SELECT identifiant, nom_etablissement, latitude, longitude
                                                    FROM georisques_etablissements_{constants.STUDIED_YEAR}""", conn)
                industrial_sites_df["air_quality_stations"] = industrial_sites_df.apply(lambda row: find_air_quality_stations_in_radius((row["latitude"], row["longitude"]), 10.0), axis=1)
                industrial_sites_df["water_quality_stations"] = industrial_sites_df.apply(lambda row: find_water_quality_stations_in_radius((row["latitude"], row["longitude"]), 10.0), axis=1)
                industrial_sites_df.to_csv(PRODUCTION_ZONE+f'industrial_sites_monitoring_stations_{constants.STUDIED_YEAR}.csv', sep=",", encoding="utf-8")
                industrial_sites_df.to_sql(f"industrial_sites_monitoring_stations_{constants.STUDIED_YEAR}", conn, if_exists="replace", index=False)
    else:
        already_mapped_industrial_sites = pd.read_sql_table(f"industrial_sites_monitoring_stations_{constants.STUDIED_YEAR}", stag_engine)
        industrial_sites_df = pd.read_sql_query(f"""SELECT identifiant, nom_etablissement, latitude, longitude
                                                    FROM georisques_etablissements_{constants.STUDIED_YEAR}""", stag_engine)
        industrial_sites_df_to_be_mapped = industrial_sites_df[~industrial_sites_df["identifiant"].isin(already_mapped_industrial_sites["identifiant"])]
        if not industrial_sites_df_to_be_mapped.empty:
            industrial_sites_df_to_be_mapped["air_quality_stations"] = industrial_sites_df_to_be_mapped.apply(lambda row: find_air_quality_stations_in_radius((row["latitude"], row["longitude"]), 10.0), axis=1)
            industrial_sites_df_to_be_mapped["water_quality_stations"] = industrial_sites_df_to_be_mapped.apply(lambda row: find_water_quality_stations_in_radius((row["latitude"], row["longitude"]), 10.0), axis=1)
            with stag_engine.connect() as conn:
                industrial_sites_df_to_be_mapped.to_sql(f"industrial_sites_monitoring_stations_{constants.STUDIED_YEAR}", conn, if_exists="append", index=False)
    prod_engine.dispose()
    stag_engine.dispose()

def create_graph_db():
    stag_engine = sqlalchemy.create_engine('postgresql://airflow:airflow@postgres:5432/staging')
    prod_engine = sqlalchemy.create_engine('postgresql://airflow:airflow@postgres:5432/production')

    industrial_sites = stag_engine.execute(f"""SELECT identifiant, nom_etablissement, latitude, longitude
                                        FROM georisques_etablissements_{constants.STUDIED_YEAR}""").fetchall()

    water_quality_stations = prod_engine.execute(f"""SELECT identifiant, name, latitude, longitude
                                                FROM monitoring_stations_{constants.STUDIED_YEAR}
                                                WHERE type = 'water_quality'""").fetchall()

    air_quality_stations = prod_engine.execute(f"""SELECT identifiant, name, latitude, longitude
                                                FROM monitoring_stations_{constants.STUDIED_YEAR}
                                                WHERE type = 'air_quality'""").fetchall()

    monitoring_stations_raw_mapping = stag_engine.execute(f"SELECT * FROM industrial_sites_monitoring_stations_{constants.STUDIED_YEAR}").fetchall()

    industrial_sites_monitoring_map = {}

    for monitoring_station_mapping in monitoring_stations_raw_mapping:
        industrial_site_id = monitoring_station_mapping[0]
        monitoring_aq_station = monitoring_station_mapping[4]
        monitoring_wq_station = monitoring_station_mapping[5]
        if monitoring_aq_station != "{}":
            aq_stations = monitoring_aq_station[1:-1].split(",")
            try:
                industrial_sites_monitoring_map[industrial_site_id]["air_quality_stations"] = aq_stations
            except KeyError:
                industrial_sites_monitoring_map[industrial_site_id] = {}
                industrial_sites_monitoring_map[industrial_site_id]["air_quality_stations"] = aq_stations
        if monitoring_wq_station != "{}":
            wq_stations = monitoring_wq_station[1:-1].split(",")
            try:
                industrial_sites_monitoring_map[industrial_site_id]["water_quality_stations"] = wq_stations
            except KeyError:
                industrial_sites_monitoring_map[industrial_site_id] = {}
                industrial_sites_monitoring_map[industrial_site_id]["water_quality_stations"] = wq_stations

    industrial_sites_label = ["IndustrialSite"]
    water_quality_stations_label = ["Station", "WaterQuality"]
    air_quality_stations_label = ["Station", "AirQuality"]

    industrial_nodes = {}
    aq_station_nodes = {}
    wq_station_nodes = {}

    for industrial_site in industrial_sites:
        industrial_nodes[industrial_site[0]]= py2neo.Node(*industrial_sites_label, **dict(industrial_site))

    for water_quality_station in water_quality_stations:
        wq_station_nodes[water_quality_station[0]] = py2neo.Node(*water_quality_stations_label, **dict(water_quality_station))

    for air_quality_station in air_quality_stations:
        aq_station_nodes[air_quality_station[0]] = py2neo.Node(*air_quality_stations_label, **dict(air_quality_station))

    monitored_by_relationships = []

    for identifiant, industrial_node in industrial_nodes.items():
        if identifiant in industrial_sites_monitoring_map:
            stations_mapping = industrial_sites_monitoring_map[identifiant]
            if "air_quality_stations" in stations_mapping:
                for air_quality_station_code in stations_mapping["air_quality_stations"]:
                    try:
                        aq_node = aq_station_nodes[air_quality_station_code]
                        monitored_by_relationships.append(py2neo.Relationship(aq_node, "MONITOR", industrial_node))
                    except KeyError:
                        pass
            if "water_quality_stations" in stations_mapping:
                for water_quality_station_code in stations_mapping["water_quality_stations"]:
                    try:
                        wq_node = wq_station_nodes[water_quality_station_code]
                        monitored_by_relationships.append(py2neo.Relationship(wq_node, "MONITOR", industrial_node))
                    except KeyError:
                        pass

    graph = py2neo.Graph("bolt://neo:7687")
    graph.delete_all()
    tx = graph.begin()
    for node in industrial_nodes.values():
        tx.create(node)
    for node in wq_station_nodes.values():
        tx.create(node)
    for node in aq_station_nodes.values():
        tx.create(node)
    for relationship in monitored_by_relationships[:1000]:
        # We limit the number of relationship for the sake of accelerating the pipeline, otherwise building the graph takes hours
        # You can get rid of [:1000] if you want to build the entire graph.
        tx.create(relationship)
    tx.commit()
