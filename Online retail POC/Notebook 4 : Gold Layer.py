# Databricks notebook source
# Read data from silver Table with Streaming
gold_stream_df = spark.readStream.format("delta") \
    .option("inferSchema", "true") \
    .load("dbfs:/delta/silver_table/gen")
    
gold_stream_df.printSchema()

# COMMAND ----------

print("Is the dataframe streaming:", gold_stream_df.isStreaming)
gold_stream_df.printSchema()

# COMMAND ----------

from delta.tables import *

gold_path = "/delta/gold_table/gen"

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS gold_g
  USING delta
  LOCATION '{gold_path}'
""")

# COMMAND ----------

gold_path = "/delta/gold_table/gen"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Add IngestionTimeSilver column to the DataFrame
gold_t_data_with_column = gold_stream_df.withColumn("IngestionTimeGold", current_timestamp())

# Start the streaming query without foreachBatch
gold_stream_query = gold_t_data_with_column.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{gold_path}/_checkpoint") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("mergeSchema", "true") \
    .trigger(once=True) \
    .option("maxFilesPerTrigger", 5000) \
    .start(gold_path)

# gold_stream_query.awaitTermination()

# COMMAND ----------

gold_stream_query.id

# COMMAND ----------

gold_stream_query.status

# COMMAND ----------

gold_stream_query.lastProgress

# COMMAND ----------

# %sql
# select * from gold_g

# COMMAND ----------

# %sql
# TRUNCATE TABLE gold_g

# COMMAND ----------

# %fs rm -r  /delta/gold_table/gen/_checkpoint

# COMMAND ----------

import pyspark.sql.functions as F
import matplotlib.pyplot as plt

# Read data from Silver Table with schema inference
gold_df = spark.read.format("delta") \
    .option("inferSchema", "true") \
    .load("dbfs:/delta/gold_table/gen")


# COMMAND ----------

# Calculate monthly totals
monthly_totals = gold_df.groupBy("Month").agg(F.sum("TotalAmount").alias("MonthlyTotal"))

# Convert the DataFrame to Pandas for plotting
monthly_totals_pandas = monthly_totals.toPandas()

# Plot the monthly totals
plt.bar(monthly_totals_pandas["Month"], monthly_totals_pandas["MonthlyTotal"])
plt.xlabel("Month")
plt.ylabel("Total Amount")
plt.title("Total Amount by Month")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC total sales for each country in descending order

# COMMAND ----------

# Example: Aggregations
total_sales = gold_df.groupBy("Country").sum("TotalAmount").orderBy("sum(TotalAmount)", ascending=False)
total_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC total quantity sold for each country in descending order

# COMMAND ----------

# Example: GroupBy
country_quantity = gold_df.groupBy("Country").agg({"Quantity": "sum"}).orderBy("sum(Quantity)", ascending=False)
country_quantity.show()

# COMMAND ----------

# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number, sum, avg

# window_spec = Window.partitionBy("Country").orderBy("InvoiceDate")
# ranked_df = silver_df.withColumn("rank", row_number().over(window_spec))
