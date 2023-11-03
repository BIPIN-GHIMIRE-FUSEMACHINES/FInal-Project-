from pyspark.sql import SparkSession
import logging
import yaml


yaml_file_path = '/home/rojesh/Documents/FInal-Project-/configuration.yaml'

# Read the YAML file and parse it into a Python dictionary
with open(yaml_file_path, 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)


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
    uncleaned_data = spark.read.csv(config["Raw_Data_Path"], header=True, inferSchema=True)
    logger.info("************************")
    logger.info("CSV File successfully read.")
    logger.info("*************************")

except Exception as e:
    logger.error("Coundnot Find the CSV File: %s", str(e))
# uncleaned_data.show()


user = config["pg_user"]
password = config["pg_password"]

jdbc_url = "jdbc:postgresql://localhost:5432/Apps_Database"

try:
    uncleaned_data.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'apps_raw_data_table', user=user,password=password).mode('overwrite').save()
    logger.info("Raw Data successfully loaded to DB")
except Exception as e:
    logger.error("Couldnot load data to postgres: %s",str(e))





