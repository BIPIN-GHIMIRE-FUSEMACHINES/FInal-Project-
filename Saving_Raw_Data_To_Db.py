from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

spark = SparkSession.builder.appName("CSVtoPostgres")\
                            .config('spark.driver.extraClassPath','/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.6.0.jar')\
                            .getOrCreate()

try:
    uncleaned_data = spark.read.csv("/home/bipin/FInal-Project-/Data/Raw_Data/googleplaystoree.csv", header=True, inferSchema=True)
    logger.info("************************")
    logger.info("CSV File successfully read.")
    logger.info("*************************")

except Exception as e:
    logger.error("Coundnot Find the CSV File: %s", str(e))
# uncleaned_data.show()

from dotenv import load_dotenv
import os

load_dotenv()

user = os.getenv("user")
password = os.getenv("password")

jdbc_url = "jdbc:postgresql://localhost:5432/Apps_Database"

try:
    uncleaned_data.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'apps_raw_data_table', user=user,password=password).mode('overwrite').save()
    logger.info("Raw Data successfully loaded to DB")
except Exception as e:
    logger.error("Couldnot load data to postgres: %s",str(e))





