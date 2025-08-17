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

def df_col_to_list(input_df,input_column):
    df_list=[]
    for record in input_df.select(input_column).collect():
        df_list.append(record[input_column])

    return df_list

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

from delta.tables import DeltaTable
def merge_delta_data(db_name,table_name,df,partition_column,merge_condition):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    if(spark.catalog.tableExists(f"{db_name}.{table_name}")):
        deltaTable=DeltaTable.forName(spark,f"{db_name}.{table_name}")
        deltaTable.alias("tgt").merge(
            df.alias("src"),
            merge_condition) \
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        df.write.mode("overwrite").partitionBy(f"{partition_column}").format("delta").saveAsTable(f"{db_name}.{table_name}")
