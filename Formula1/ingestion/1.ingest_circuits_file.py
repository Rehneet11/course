# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest circuits.csv File

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Applying Custom Schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
circuit_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                                  StructField("circuitRef",StringType(),True),StructField("name",StringType(),True),StructField("location",StringType(),True),StructField("country",StringType(),True),StructField("lat",DoubleType(),True),StructField("lng",DoubleType(),True),StructField("alt",IntegerType(),True),StructField("url",StringType(),True)
                                  ]
                          )


# COMMAND ----------

df=spark.read.options(header=True).schema(circuit_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Selecting columns

# COMMAND ----------

from pyspark.sql.functions import col,lit
selected_df=df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))
selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the column

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW DATABASES

# COMMAND ----------

renamed_df=selected_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))
renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add Ingestion Date as a new Column

# COMMAND ----------

final_df=add_ingestion_date(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data in DL using parquet format

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")
