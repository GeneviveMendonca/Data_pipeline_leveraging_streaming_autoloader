# Databricks notebook source
# MAGIC %sql
# MAGIC select count(*) from gold_g --1047877

# COMMAND ----------

import zipfile
 
zip_path = '/dbfs/FileStore/Genevive/PARTICIPANT_ATTRIBUTES.zip'
extract_path = '/dbfs/FileStore/Genevive/'
 
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

# COMMAND ----------


