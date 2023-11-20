from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, split, element_at, to_date, date_format

STAGING_ZONE_PATH = "/opt/airflow/data/staging/air-quality/"
LANDING_ZONE_PATH = "/opt/airflow/data/landing/air-quality/"

spark = SparkSession.builder.appName('PureSphere').config("spark.executor.memory", "4g").getOrCreate()
measurements_df = spark.read.parquet(STAGING_ZONE_PATH+"aggregated_by_code_site_and_pollutant/**/FR_E2_*")
measurements_df = measurements_df.withColumn("Date", date_format(to_date(element_at(split(element_at(split(input_file_name(), "/"), -1), "_"), -1), "yyyy-MM-dd"), "dd/MM/yyyy"))
measurements_pd_df = measurements_df.toPandas()
measurements_pd_df.to_parquet(STAGING_ZONE_PATH+"measurements_file.parquet")
spark.stop()
