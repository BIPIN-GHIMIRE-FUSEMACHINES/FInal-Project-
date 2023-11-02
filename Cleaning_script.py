import pyspark.sql.functions as f
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CleaningData")\
    .config('spark.driver.extraClassPath','/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.6.0.jar')\
    .getOrCreate()



load_dotenv()

user = os.getenv("user")
password = os.getenv("password")

jdbc_url = "jdbc:postgresql://localhost:5432/air"

connection_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

uncleaned_data = spark.read.jdbc(url=jdbc_url, table="test_table_create_testing_final", properties=connection_properties)

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


# csv_path = "./Data/"
# cleaning_data.write.csv(csv_path, header=True, mode = "overwrite")



# # %%
# parquet_path = "./Data/Parquet_Data/"
# cleaning_data.write.parquet(parquet_path, mode="overwrite")

cleaning_data.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'stagged_google_playstore_data', user='postgres',password='1234').mode('overwrite').save()




