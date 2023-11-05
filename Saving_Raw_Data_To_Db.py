from pyspark.sql import SparkSession
import logging
import pyspark.sql.functions as f
from pyspark.sql.types import StructField, StringType, StructType, BooleanType, FloatType, IntegerType, DateType
from dotenv import load_dotenv
import os
import yaml

yaml_file_path = '/home/bipin/FInal-Project-/config.yaml'

# Read the YAML file and parse it into a Python dictionary
with open(yaml_file_path, 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.FileHandler("/home/bipin/FInal-Project-/test.log")
ch.setFormatter(formatter)
logger.addHandler(ch)

spark = SparkSession.builder.appName("CSVtoPostgres")\
                            .config('spark.driver.extraClassPath','/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.6.0.jar')\
                            .getOrCreate()

custom_schema = StructType([
    StructField("App", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Rating", FloatType(), True),
    StructField("Reviews", IntegerType(), True),
    StructField("Size", StringType(), True),
    StructField("Installs", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("Content Rating", StringType(), True),
    StructField("Genres", StringType(), True),
    StructField("Last Updated", StringType(), True),
    StructField("Current Ver", StringType(), True),
    StructField("Android Ver", StringType(), True),
])

try:
    uncleaned_data = spark.read.csv(config["Raw_data_path"], header=True, inferSchema=True)
    logger.info("************************")
    logger.info("CSV File successfully read.")
    logger.info("*************************")

except Exception as e:
    logger.error("Coundnot Find the CSV File: %s", str(e))
# uncleaned_data.show()

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# data = uncleaned_data.withColumn("Last Updated", regexp_replace(col("Last Updated"), "Jan", "January"))  # Replace abbreviated month names
data = uncleaned_data.withColumn("Last Updated", f.regexp_replace(f.col("Last Updated"), ",", ""))  # Remove commas
data = data.withColumn("Last Updated", f.to_date(f.col("Last Updated"), "MMMM d yyyy"))

load_dotenv()

user = os.getenv("user")
password = os.getenv("password")

jdbc_url = "jdbc:postgresql://localhost:5432/Apps_Database"

try:
    data.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'apps_raw_data_table', user=user,password=password).mode('overwrite').save()
    logger.info("Raw Data successfully loaded to DB")
except Exception as e:
    logger.error("Couldnot load data to postgres: %s",str(e))





