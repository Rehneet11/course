# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Describe Schema

# COMMAND ----------

constructor_schema="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply Schema

# COMMAND ----------

df=spark.read.schema(constructor_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DROP the url column

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp
dropped_df=df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the column

# COMMAND ----------

renamed_df=dropped_df.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("constructorRef","constructor_ref")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))                

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add Ingestion Date Column

# COMMAND ----------

final_df=add_ingestion_date(renamed_df)
final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")
