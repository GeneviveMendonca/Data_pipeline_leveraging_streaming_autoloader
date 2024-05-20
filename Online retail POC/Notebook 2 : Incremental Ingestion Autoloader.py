# Databricks notebook source
# MAGIC %md
# MAGIC Step 1: Create Bronze Table
# MAGIC
# MAGIC

# COMMAND ----------

from delta.tables import *

bronze_path = "/delta/brnz/genny"

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS bronze_g
  USING delta
  LOCATION '{bronze_path}'
""")



# COMMAND ----------

input_file = "dbfs:/FileStore/Genevive/auto_loader/"

# COMMAND ----------

# MAGIC %md
# MAGIC Ingesting csv data by autoloader

# COMMAND ----------

# Set up auto-loader
# Increase the maximum number of files per trigger in the auto-loader
spark.conf.set("spark.databricks.delta.auto.loader.maxFilesPerTrigger", "2000")

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import current_timestamp

# Define the schema for the CSV files
schema = StructType([
    StructField("Invoice", StringType()),
    StructField("StockCode", StringType()),
    StructField("Description", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("InvoiceDate", TimestampType()),
    StructField("Price", DoubleType()),
    StructField("CustomerID", DoubleType()),
    StructField("Country", StringType()),
    StructField("IngestionTime", TimestampType(), nullable=False)
])

# Create the initial streaming query
query = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .option("cloudFiles.schemaLocation", f"{bronze_path}/_schema_location") \
    .option("recursiveFileLookup", "true") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .schema(schema) \
    .load(input_file + "/*.csv") \
    .selectExpr(
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "Price",
        "CustomerID",
        "Country",
        "cast(null as timestamp) as IngestionTime"
    )

# Add ingestion time column
query = query.withColumn("IngestionTime", current_timestamp())

# Start writing the stream to Delta Lake
streaming_query = query.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{bronze_path}/_checkpoint") \
    .option("mergeSchema", "true") \
    .trigger(once=True) \
    .start(bronze_path)

# Wait for the stream to finish
streaming_query.awaitTermination()


# COMMAND ----------

# import time

# time.sleep(10)

# query.stop()

# COMMAND ----------

# %fs rm -r  /delta/brnz/genny/_checkpoint

# COMMAND ----------

# %sql
# TRUNCATE TABLE bronze_g

# COMMAND ----------


