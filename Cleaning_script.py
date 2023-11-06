import pyspark.sql.functions as f
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
import logging
import yaml
import datetime

load_dotenv()

log_folder = os.getenv("log_folder_path")
current_datetime = datetime.datetime.now()
log_filename = current_datetime.strftime("%Y-%m-%d.log")
log_file_path = os.path.join(log_folder,log_filename)

# #Initializing Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.FileHandler(log_file_path, mode = 'a')
ch.setFormatter(formatter)
logger.addHandler(ch)


# Create a Spark session
logger.info("Creating Spark Session")

jar_file_path = os.getenv("postgres_jar_file_path")
spark = SparkSession.builder.appName("CleaningData")\
    .config('spark.driver.extraClassPath',jar_file_path)\
    .getOrCreate()

logger.info("Spark Session Successfully Created")


logger.info("Loading the environment variables.")

user = os.getenv("postgres_username")
password = os.getenv("postgres_password")
logger.info("Environment Variables successfully Loaded.")


jdbc_url = "jdbc:postgresql://localhost:5432/Apps_Database"

connection_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

logger.info("Reading the Data from postgres Database.")
try:
    #Reading the Database from Postgres
    uncleaned_data = spark.read.jdbc(url=jdbc_url, table="apps_raw_data_table", properties=connection_properties)
    logger.info("Connection to postgres for reading data successfull.")
except Exception as e:
    logger.error("Unable to connect to postgres for reading data.")
    
logger.info("Postgres Data Successfully Read")

# Replacing NaN (null value) present in the Rating Column with 0.0
uncleaned_data = uncleaned_data.withColumn("Rating", f.when(f.col("Rating") == "NaN", "0.0").otherwise(f.col("Rating")))

"""
Replacing the value, Varies with device in Current Ver, Size and Android Ver as None
And
Adding three new columns:
Device_Current_Version_Dependency: True if the current version of the device varies with device i.e true when Current Ver = Null
Device_Android_Version_Dependency: True if the Android version of the device varies with device i.e true when Android Ver = Null
Device_Size_Dependency: True if the Size of the device varies with device i.e true when Size = Null
"""

cleaning_data = uncleaned_data.withColumn("Current Ver", f.when(f.col("Current Ver") == "Varies with device", None).otherwise(f.col("Current Ver")))

cleaning_data = cleaning_data.withColumn("Device_Current_Version_Dependency", f.when(f.col("Current Ver").isNull(), True).otherwise(False))

cleaning_data = cleaning_data.withColumn("Size", f.when(f.col("Size") == "Varies with device", None).otherwise(f.col("Size")))

cleaning_data = cleaning_data.withColumn("Device_Size_Dependency", f.when(f.col("Size").isNull(), True).otherwise(False))

cleaning_data = cleaning_data.withColumn("Android Ver", f.when(f.col("Android Ver") == "Varies with device", None).otherwise(f.col("Android Ver")))

cleaning_data = cleaning_data.withColumn("Device_Android_Version_Dependency", f.when(f.col("Android Ver").isNull(), True).otherwise(False))


# Converting the values in Size column to Mb and creating a new column Size_ib_Megabytes.
cleaning_data_test = cleaning_data.withColumn(
    "Size_in_Megabytes",
    f.when(cleaning_data["Size"].contains("M"), 
         f.regexp_replace(cleaning_data["Size"], "M", "").cast("float"))
    .when(cleaning_data["Size"].contains("k"), 
          (f.regexp_replace(cleaning_data["Size"], "k", "").cast("float") / 1024))
    .otherwise(cleaning_data["Size"])
)



# Removing additional character "and up" from the Android Ver Column
cleaning_data = cleaning_data_test.withColumn("Android Ver", f.regexp_replace(cleaning_data_test["Android Ver"], " and up", ""))

# Typecasting Price Column to Float
cleaning_data = cleaning_data.withColumn("Price", f.regexp_replace(f.col("Price"), "\\$", "").cast("float"))

# Removing numeric values from the Type column
values_to_filter = ['0', '102248', 'NaN', '2509']
filtered_rows = cleaning_data.filter(~cleaning_data['Type'].isin(values_to_filter))

# TypeCasting Rating Colun to Float, Rerviews to Integer and Size_in_Megabytes to float
df = filtered_rows.withColumn("Rating", f.col("Rating").cast("float"))
df = df.withColumn("Reviews", f.col("Reviews").cast("integer"))
df = df.withColumn("Size_in_Megabytes", f.col("Size_in_Megabytes").cast("float"))


logger.info("Loading the Cleaned Data to postgres database.")
# Dumping the cleaned dataframe to postgres.
try:
    df.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'Cleaned_google_playstore_data', user=user, password=password).mode('overwrite').save()
    logger.info("Cleaned Data Successfully Loaded to Db")

except Exception as e:
    logger.error("Cleaned Data To DB failed.")



