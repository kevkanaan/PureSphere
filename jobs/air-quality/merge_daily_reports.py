import glob
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import input_file_name, split, element_at, to_date, date_format

STAGING_ZONE_PATH = "/opt/airflow/data/staging/air-quality/"
LANDING_ZONE_PATH = "/opt/airflow/data/landing/air-quality/"

spark = SparkSession.builder.appName('PureSphere').getOrCreate()

files = list(glob.glob(STAGING_ZONE_PATH+"aggregated_by_code_site_and_pollutant/*"))
measurement_reports_per_year = {}
for file in files:
    measurement_reports = glob.glob(file+"/FR_E2_*")
    measurement_reports_per_year[int(file[-4:])] = measurement_reports

measurements_df: DataFrame = None
for measurement_reports in measurement_reports_per_year.values():
    for measurement_report in measurement_reports:
        measurement_report_df = spark.read.format("parquet").load(measurement_report)
        measurement_report_df = measurement_report_df.withColumn("Date", date_format(to_date(element_at(split(element_at(split(input_file_name(), "/"), -1), "_"), -1), "yyyy-MM-dd"), "dd/MM/yyyy"))
        if measurements_df is None:
            measurements_df = measurement_report_df
        else:
            measurements_df = measurements_df.unionByName(measurement_report_df)
measurements_df.write.mode("overwrite").parquet(STAGING_ZONE_PATH+"measurements_file.parquet")
spark.stop()
