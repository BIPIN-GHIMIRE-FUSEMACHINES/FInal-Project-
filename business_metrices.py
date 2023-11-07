from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from dotenv import load_dotenv
import os

# Create a Spark session
spark = SparkSession.builder.appName("MetricCreation")\
    .config('spark.driver.extraClassPath','/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.6.0.jar')\
    .getOrCreate()


load_dotenv()

user = os.getenv("postgres_username")
password = os.getenv("postgres_password")


jdbc_url = "jdbc:postgresql://localhost:5432/Apps_Database"

connection_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table="cleaned_google_playstore_data", properties=connection_properties)

# Highest and lowest rated apps categories.
avg_rating_by_category = df.groupBy("Category").agg(f.avg("Rating").alias("AverageRating"))

top_five_avg_rating_by_category = avg_rating_by_category.orderBy(f.col("AverageRating").desc()).limit(10)
bottom_five_avg_rating_by_category = avg_rating_by_category.orderBy(f.col("AverageRating").asc()).limit(10)

top_five_avg_rating_by_category.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'Top_ten_highest_rated_categories', user=user,password=password).mode('overwrite').save()
bottom_five_avg_rating_by_category.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'Top_ten_lowest_rated_categories', user=user,password=password).mode('overwrite').save()


# Number of apps updated in each year.
app_updates_time_series = df.withColumn("UpdateYear", f.year("Last Updated")) \
    .groupBy("UpdateYear") \
    .agg(f.count("App").alias("NumberOfUpdates")) \
    .orderBy(f.desc("UpdateYear"))

app_updates_time_series.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'Number_of_updates_in_each_year', user=user,password=password).mode('overwrite').save()


# Number of Paid Apps in top 10 categories
paid_apps_df = df.filter(f.col("Type") == "Paid")
category_grouped = paid_apps_df.groupBy("Category")
category_installs = category_grouped.agg(f.sum("Installs").alias("TotalInstalls"))
sorted_category_installs = category_installs.orderBy(f.col("TotalInstalls").desc())
top_categories_paid = sorted_category_installs.limit(10)

top_categories_paid.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'Top_ten_highest_installed_paid_apps', user=user,password=password).mode('overwrite').save()


# Number of Free Apps in top 10 categories.
paid_apps_df = df.filter(f.col("Type") == "Free")
category_grouped = paid_apps_df.groupBy("Category")
category_installs = category_grouped.agg(f.sum("Installs").alias("TotalInstalls"))
sorted_category_installs = category_installs.orderBy(f.col("TotalInstalls").desc())
top_categories_free = sorted_category_installs.limit(10)

top_categories_free.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'Top_ten_highest_installed_free_apps', user=user,password=password).mode('overwrite').save()


# Counts of free and paids app in top 5 categories
category_grouped = df.groupBy("Category")
category_metrics = category_grouped.agg(
    f.count(f.when(f.col("Type") == "Paid", True)).alias("PaidAppsCount"),
    f.count(f.when(f.col("Type") == "Free", True)).alias("FreeAppsCount"),
    f.count("*").alias("TotalAppCount"))
sorted_category_metrics = category_metrics.orderBy(f.col("TotalAppCount").desc())

sorted_category_metrics.write.format('jdbc').options(url=jdbc_url,driver = 'org.postgresql.Driver', dbtable = 'Top_five_apps_count', user=user,password=password).mode('overwrite').save()


spark.stop()