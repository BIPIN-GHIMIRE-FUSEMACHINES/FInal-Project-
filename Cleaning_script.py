import pyspark.sql.functions as f
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
import logging

#Initializing Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
# Create a Spark session
spark = SparkSession.builder.appName("CleaningData")\
    .config('spark.driver.extraClassPath','/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.6.0.jar')\
    .getOrCreate()



load_dotenv()

user = os.getenv("user")
password = os.getenv("password")

jdbc_url = "jdbc:postgresql://localhost:5432/Apps_Database"

connection_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

uncleaned_data = spark.read.jdbc(url=jdbc_url, table="apps_raw_data_table", properties=connection_properties)

uncleaned_data = uncleaned_data.withColumn("Rating", f.when(f.col("Rating") == "NaN", "0.0").otherwise(f.col("Rating")))

cleaning_data = uncleaned_data.withColumn("Current Ver", f.when(f.col("Current Ver") == "Varies with device", None).otherwise(f.col("Current Ver")))

# cleaning_data.show()

cleaning_data = cleaning_data.withColumn("Device_Current_Version_Dependency", f.when(f.col("Current Ver").isNull(), True).otherwise(False))

cleaning_data = cleaning_data.withColumn("Size", f.when(f.col("Size") == "Varies with device", None).otherwise(f.col("Size")))

cleaning_data = cleaning_data.withColumn("Device_Size_Dependency", f.when(f.col("Size").isNull(), True).otherwise(False))
cleaning_data = cleaning_data.withColumn("Android Ver", f.when(f.col("Android Ver") == "Varies with device", None).otherwise(f.col("Android Ver")))

cleaning_data = cleaning_data.withColumn("Device_Android_Version_Dependency", f.when(f.col("Android Ver").isNull(), True).otherwise(False))

# cleaning_data.show()

cleaning_data_test = cleaning_data.withColumn(
    "Size_in_Megabytes",
    f.when(cleaning_data["Size"].contains("M"), 
         f.regexp_replace(cleaning_data["Size"], "M", "").cast("float"))
    .when(cleaning_data["Size"].contains("k"), 
          (f.regexp_replace(cleaning_data["Size"], "k", "").cast("float") / 1024))
    .otherwise(cleaning_data["Size"])
)


# cleaning_data_test.show()

cleaning_data = cleaning_data_test.withColumn("Android Ver", f.regexp_replace(cleaning_data_test["Android Ver"], " and up", ""))

cleaning_data = cleaning_data.withColumn("Price", f.regexp_replace(f.col("Price"), "\\$", "").cast("float"))

values_to_filter = ['0', '102248', 'NaN', '2509']

# Filter the rows with the specified values
filtered_rows = cleaning_data.filter(~cleaning_data['Type'].isin(values_to_filter))

# Show the filtered rows
# filtered_rows.show()
df = filtered_rows.withColumn("Rating", f.col("Rating").cast("float"))
df = df.withColumn("Reviews", f.col("Reviews").cast("integer"))
df = df.withColumn("Size_in_Megabytes", f.col("Size_in_Megabytes").cast("float"))

try:
    df.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'stagged_google_playstore_data', user='postgres',password='1234').mode('overwrite').save()
    logger.info("Cleaned Data Successfully Loaded to Db")

except Exception as e:
    logger.error("Cleaned Data To DB failed.")



