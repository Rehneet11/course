# Databricks notebook source
raw_folder_path="abfss://raw@dbvsarthi.dfs.core.windows.net"
processed_folder_path="abfss://processed@dbvsarthi.dfs.core.windows.net"
presentation_folder_path="abfss://presentation@dbvsarthi.dfs.core.windows.net"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df=input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_partition_column(input_df,partition_column):
    column_list=[]
    for column_name in input_df.schema.names:
        if(column_name!=partition_column):
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df=input_df.select(column_list)
    return output_df



# COMMAND ----------

def incremental_load(db_name,table_name,df,partition_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if(spark.catalog.tableExists(f"{db_name}.{table_name}")):
        df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        df.write.partitionBy(partition_column).mode("append").format("parquet").saveAsTable(f"{db_name}.{table_name}")
