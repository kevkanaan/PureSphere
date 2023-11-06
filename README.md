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

The webserver is available at: http://localhost:8080. The default account has the login `airflow` and the password `airflow`.

## Run commands

You can run CLI commands, but you have to do it in one of the defined `airflow-*` services, ex:

```bash
docker compose run airflow-worker airflow info
```

## The project

The goal of the project is to implement a full stack data pipeline to answer 2-3 questions formulated in natural language.

We chose the following questions, focusing on France:
- What are the zones for which we have information about the air quality and the water quality?
- Can we see the impact of industrial sites on their surrounding area in terms of air and water quality?
- ...

To answer them, we use 3 datasets:
- Géorisques: the list of industrial facilities releasing pollutants
- Water API: information about water quality
- OpenAQ: information about air quality

For more information about a dataset, you can look at its README in the `/script` folder.

### Data

The retrieved data are stored in the `/data` folder. The data are stored in 3 "zones":
- the **landing** zone: the raw data as they are retrieved from the sources
- the **staging** zone: the date as they are after each stage of data cleansing
- the **production** zone: the final data, completely cleaned, and ready to be used
