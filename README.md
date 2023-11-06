# PureSphere

Project made by 3 INSA Lyon students for the OT7-Data Engineering course of [Ricardo TOMMASSINI](https://www.riccardotommasini.com/) :
- Kevin KANAAN
- Tom DELAPORTE
- Jorick PEPIN

## Initialization

> **Note**
> First time only.

### Create the necessary folders

```bash
mkdir -p ./dags ./logs ./plugins ./config
```

### Setting the right Airflow user

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### Initialize the database

```bash
docker compose up airflow-init
```

## Run Airflow

```bash
docker compose up
```

The webserver is available at: http://localhost:8080. The default account has the login `airflow` and the password `airflow`.

## Run commands

You can run CLI commands, but you have to do it in one of the defined airflow-* services, ex:

```bash
docker compose run airflow-worker airflow info
```

## The project

The goal of the project is to implement a full stack data pipeline to answer 2-3 questions formulated in natural language.

We choose the following questions:
- 
- 
- 

To answer them, we use 4 datasets:
- Géorisques: the list of industrial facilities releasing pollutants
- ARIA: the list of industrial accidents
- Water API: information about water quality
- OpenAQ: information about air quality

For more information about a dataset, you can look at its README in the `/script` folder.
