# Databricks notebook source
# MAGIC %md
# MAGIC Online Retail 

# COMMAND ----------

# MAGIC                                %pip install openpyxl

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Excel Sheets to Spark DataFrames") \
    .getOrCreate()

# Define the path to the Excel file
# excel_path = "/dbfs/FileStore/Genevive/online_retail_II.xlsx"
excel_path = "/dbfs/FileStore/Genevive/new_xl.xlsx"



# COMMAND ----------

# Read the Excel file into pandas DataFrames for each sheet
# df1_pandas = pd.read_excel(excel_path, sheet_name='Year 2009-2010')
df1_pandas = pd.read_excel(excel_path, sheet_name='Sheet1')

# COMMAND ----------

df2_pandas = pd.read_excel(excel_path, sheet_name='Year 2010-2011')

# COMMAND ----------

# Convert pandas DataFrames to Spark DataFrames
df1_spark = spark.createDataFrame(df1_pandas)

# COMMAND ----------

df1_spark.show()

# COMMAND ----------

df2_spark = spark.createDataFrame(df2_pandas)

# Show the first few rows of each Spark DataFrame
print("First few rows of Sheet2:")
df2_spark.show()

# COMMAND ----------

# dbfs:/FileStore/Genevive/auto_loader/year2009.csv

# COMMAND ----------

# output_path1 = "/FileStore/Genevive/auto_loader/year2009.csv"
output_path3 = "/FileStore/Genevive/auto_loader/yeartest.csv"

# COMMAND ----------

# Save the first DataFrame to a CSV file with inferred schema
# df1_spark.write.csv(output_path1, header=True, mode="overwrite", inferSchema=True)
df1_spark.write.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .mode("overwrite") \
    .save(output_path3)

# Save the second DataFrame to a CSV file
# df2_spark.write.csv(output_path2, header=True, mode="overwrite")

# Display the paths of the saved CSV files
print(f"CSV file saved to: {output_path3}")
# print(f"CSV file saved to: {output_path2}")

# COMMAND ----------

# sample_df = spark.read.option("header", "true").option("inferSchema", "true").csv(output_path1).limit(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_g where StockCode like '%Geeny%'

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS bronze_genevive")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze_g SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history silver_g

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_g

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE silver_g SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE gold_g SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# from pyspark.ml.feature import StandardScaler
# from pyspark.ml.feature import VectorAssembler
# from pyspark.sql.functions import col, to_timestamp
# from pyspark.sql.types import IntegerType

# # Cast columns to the correct data types
# silver_t_data = silver_t_data.withColumn("Invoice", col("Invoice").cast(IntegerType()))
# silver_t_data = silver_t_data.withColumn("StockCode", col("StockCode").cast(IntegerType()))
# silver_t_data = silver_t_data.withColumn("Description", col("Description").cast("string"))
# silver_t_data = silver_t_data.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate")))
# silver_t_data = silver_t_data.withColumn("Country", col("Country").cast("string"))
# # silver_t_data = silver_t_data.withColumn("Month", col("Month").cast(IntegerType()))
# silver_t_data = silver_t_data.withColumn("DayOfMonth", col("DayOfMonth").cast(IntegerType()))

# # Replace null values with default value (0 in this case)
# silver_t_data1 = silver_t_data.fillna(0)

# # Create a vector assembler to combine the relevant columns into "features"
# assembler = VectorAssembler(
#     inputCols=["Invoice", "StockCode", "Quantity", "Price", "Month", "DayOfMonth"],
#     outputCol="features")

# # Apply the vector assembler to create "features" column
# silver_t_data_with_features = assembler.transform(silver_t_data1)

# # Perform feature scaling with error handling
# try:
#     scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
#     scaled_data = scaler.fit(silver_t_data_with_features).transform(silver_t_data_with_features)
# except Exception as e:
#     print("An error occurred during scaling:", e)
#     scaled_data = None

# COMMAND ----------

# Display the "features" column
# scaled_data.select("features").show()

# Display the "scaled_features" column
# scaled_data.select("scaled_features").show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df = spark.read.format("csv").load('/databricks-datasets/timeseries/Fires')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls '/databricks-datasets/sms_spam_collection'

# COMMAND ----------

# MAGIC %md
# MAGIC
