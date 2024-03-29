# PureSphere

Project made by 3 INSA Lyon students for the OT7-Data Engineering course of [Ricardo TOMMASSINI](https://www.riccardotommasini.com/) :
- Kevin KANAAN
- Tom DELAPORTE
- Jorick PEPIN

## Initialization

> **Note**
> First time only.

Create the necessary folders:

```bash
mkdir -p ./dags ./logs ./plugins ./config
```

Setting the right Airflow user:

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Initialize the database:

```bash
docker compose up airflow-init
```

## Run Airflow

```bash
docker compose up
```

The webserver is available at: http://localhost:8080. The default account has the login `airflow` and the password `airflow`. To connect to Postgres database using pgAdmin, the username is `airflow`.

If you want to run the pipeline offline, make sure:
- the folder [data/landing/air-quality/2021](data/landing/air-quality/2021/) contains all the daily measurement files. 
- the folder [data/landing/georisques/2021](data/landing/georisques/2021/) contains its 8 CSV files
-  the folder [data/landing/water-quality](data/landing/water-quality/) contains both `analysispc2021` and `stationpc.csv`

Then, simply skip the ingestion pipeline as it is not made to run offline and starts directly from the wrangling pipeline.

## Set up Spark connection
Within Airflow webserver, create a new connection to Spark. To do so:
- Go into Admin > Connections
- Click on **+** to configure a new connection
- Create the Spark connection with the following parameters:
    - *Connection Id*: ```spark-conn```
    - *Connection Type*: ```Spark```
    - *Host*: ```spark://spark-master```
    - *Port*: ```7077```
- Save this new connection and Airflow is properly connected to Spark!

## Run commands

You can run CLI commands, but you have to do it in one of the defined `airflow-*` services, ex:

```bash
docker compose run airflow-worker airflow info
```

## Dependencies

We can add Python dependencies through the `requirements.txt` file. Keep in mind that when a new dependency is added, you have to rebuild the image. See the [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#special-case-adding-dependencies-via-requirements-txt-file) for more information.

## Quick overview of the project

The goal of the project is to implement a full stack data pipeline to answer 2-3 questions formulated in natural language.

We chose the following questions, focusing on France in 2021:
- What are the zones for which we have information about the air quality and the water quality?
- What is the proportion of industrial sites having a monitoring station in a 10km radius zone ?

To answer them, we use 3 datasets: Géorisques, Hub'eau and Géod'air

For more information about a dataset, you can look at its README in the `/dags/<dataset>` folder.

To read the whole project report, [it's here](docs/Rapport.md)
### Data

The retrieved data are stored in the `/data` folder. The data are stored in 3 "zones":
- the **landing** zone: the raw data as they are retrieved from the sources
- the **staging** zone: the date as they are after each stage of data cleansing
- the **production** zone: the final data, completely cleaned, and ready to be used

### Pipelines

The pipelines are defined in the `/dags` folder:
1. `ingest.py`: responsible to bring raw data to the landing zone. It takes approximately 15min to run.
2. `wrangle.py`: responsible to migrate raw data from the landing zone and move them into the staging area (cleaning, wrangling, transformation, etc.). Again, it takes approximately 15min to run.
3. `production.py`: responsible to move the data from the staging zone into the production zone, and trigger the update of data marts (views). The data mart consist of a SQL table located in Postgres `production` db and a toy graph database stored in Neo4J. Build the whole graph requires hours, so we have restricted it to 1000 relations for the sake of illustration.

### Useful links
| Service      |       URL      |
|--------------|:--------------:|
| airflow      | [localhost:8080](http://localhost:8080) |
| spark-master | [localhost:9090](http://localhost:9090) |
| Neo4J Browser       | [localhost:7474](http://localhost:7474) |
| Neo4J DB      | [localhost:7687](http://localhost:7687) |