# Databricks notebook source
# MAGIC %md
# MAGIC - Data Refinement and Structuring

# COMMAND ----------

# Read data from Bronze Table with Streaming
bronze_stream_df = spark.readStream.format("delta") \
    .option("inferSchema", "true") \
    .load("dbfs:/delta/brnz/genny")
    
bronze_stream_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC data cleansing and Data Quality Checks:

# COMMAND ----------

# Remove rows with missing values in relevant columns
silver_t_data = bronze_stream_df.dropna(subset=["Invoice", "StockCode", "Description", "InvoiceDate", "Country"])


# COMMAND ----------

# MAGIC %md
# MAGIC feature Engineering

# COMMAND ----------

# Edit the code to add imports for col function
from pyspark.sql.functions import col

# Create a new feature for total amount
silver_t_data = silver_t_data.withColumn("TotalAmount", col("Quantity") * col("Price"))

# COMMAND ----------

# MAGIC %md
# MAGIC Data Extraction

# COMMAND ----------

from pyspark.sql.functions import year, date_format

# Extract additional date-related features with year, month, and day names
silver_t_data = silver_t_data.withColumn("Year", year("InvoiceDate")) \
                             .withColumn("Month", date_format("InvoiceDate", "MMMM")) \
                             .withColumn("DayOfMonth", date_format("InvoiceDate", "dd"))



# COMMAND ----------

from pyspark.sql.functions import when
# Create a binary column indicating if the Quantity is greater than 10
silver_t_data = silver_t_data.withColumn('HighQuantity', when(silver_t_data['Quantity'] > 10, 1).otherwise(0))


# COMMAND ----------

# MAGIC %md
# MAGIC Data Filtering:

# COMMAND ----------

# Filter out canceled orders
silver_t_data = silver_t_data.filter(col("Invoice").startswith("C") == False)

# COMMAND ----------

# MAGIC %md
# MAGIC Add ingestion current timestamp time

# COMMAND ----------

from delta.tables import *

silver_path = "/delta/silver_table/gen"

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS silver_g
  USING delta
  LOCATION '{silver_path}'
""")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Add IngestionTimeSilver column to the DataFrame
silver_t_data_with_column = silver_t_data.withColumn("IngestionTimeSilver", current_timestamp())

# Start the streaming query without foreachBatch
silver_stream_query = silver_t_data_with_column.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{silver_path}/_checkpoint") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("mergeSchema", "true") \
    .trigger(once=True) \
    .option("maxFilesPerTrigger", 5000) \
    .start(silver_path)

# gold_stream_query.awaitTermination()

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp

# # Start the streaming query without foreachBatch
# silver_stream_query = silver_t_data.writeStream.format("delta") \
#     .withColumn("IngestionTimeSilver", current_timestamp()) \
#     .outputMode("append") \
#     .option("checkpointLocation", f"{silver_path}/_checkpoint") \
#     .option("cloudFiles.inferColumnTypes", "true") \
#     .option("mergeSchema", "true") \
#     .trigger(once=True) \
#     .option("maxFilesPerTrigger", 5000) \
#     .start(silver_path)

# # gold_stream_query.awaitTermination()

# COMMAND ----------

silver_stream_query.id

# COMMAND ----------

silver_stream_query.status

# COMMAND ----------

silver_stream_query.lastProgress

# COMMAND ----------

# import time

# time.sleep(10)

# silver_stream_query.stop()

# COMMAND ----------

# %fs rm -r  /delta/silver_table/gen/_checkpoint

# COMMAND ----------

# %sql
# TRUNCATE TABLE silver_g

# COMMAND ----------


