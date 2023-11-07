from pyspark.sql import SparkSession
import logging
import pyspark.sql.functions as f
from pyspark.sql.types import StructField, StringType, StructType, BooleanType, FloatType, IntegerType, DateType
from dotenv import load_dotenv
import os
import yaml
import datetime

load_dotenv()
raw_data_path = os.getenv("Raw_data_path")

log_folder = os.getenv("log_folder_path")

if not os.path.exists(log_folder):
    os.mkdir(log_folder)

current_datetime = datetime.datetime.now()
log_filename = current_datetime.strftime("%Y-%m-%d.log")

log_file_path = os.path.join(log_folder,log_filename)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.FileHandler(log_file_path, mode = 'w')
ch.setFormatter(formatter)
logger.addHandler(ch)

jar_file_path = os.getenv("postgres_jar_file_path")
spark = SparkSession.builder.appName("CSVtoPostgres")\
                            .config('spark.driver.extraClassPath',jar_file_path)\
                            .getOrCreate()


logger.info("Reading Raw Data")
try:
    uncleaned_data = spark.read.csv(raw_data_path, header=True, inferSchema=True)
    logger.info("CSV File successfully read.")

except Exception as e:
    logger.error("Coundnot Find the CSV File: %s", str(e))
# uncleaned_data.show()

# spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# # data = uncleaned_data.withColumn("Last Updated", regexp_replace(col("Last Updated"), "Jan", "January"))  # Replace abbreviated month names
# data = uncleaned_data.withColumn("Last Updated", f.regexp_replace(f.col("Last Updated"), ",", ""))  # Remove commas
# data = data.withColumn("Last Updated", f.to_date(f.col("Last Updated"), "MMMM d yyyy"))



user = os.getenv("postgres_username")
password = os.getenv("postgres_password")

jdbc_url = "jdbc:postgresql://localhost:5432/Apps_Database"

logger.info("Dumping the raw data to postgres database using custome schema.")
try:
    uncleaned_data.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'google_playstore_apps_raw_data_table', user=user,password=password).mode('overwrite').save()
    logger.info("Raw Data successfully loaded to DB")
except Exception as e:
    logger.error("Couldnot load data to postgres: %s",str(e))





