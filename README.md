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

The project has the following structure:

`/dags` - contains the DAG files.\
`/logs` - contains logs from task execution and scheduler.\
`/config` - you can add custom log parser or add airflow_local_settings.py to configure cluster policy.\
`/plugins` - you can put your custom plugins here.

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
