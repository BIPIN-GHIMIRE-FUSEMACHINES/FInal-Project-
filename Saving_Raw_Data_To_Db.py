
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSVtoPostgres")\
                            .config('spark.driver.extraClassPath','/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.6.0.jar')\
                            .getOrCreate()

uncleaned_data = spark.read.csv("./Data/Raw_Data/googleplaystore.csv", header=True, inferSchema=True)

# uncleaned_data.show()

from dotenv import load_dotenv
import os

load_dotenv()

user = os.getenv("user")
password = os.getenv("password")

jdbc_url = "jdbc:postgresql://localhost:5432/Apps_Database"

uncleaned_data.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'apps_raw_data_table', user=user,password=password).mode('overwrite').save()






