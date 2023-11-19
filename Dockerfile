FROM apache/airflow:2.7.2

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
