import dlt
from pyspark.sql.functions import col

@dlt.view(
    name="sales_enriched_view"
)
def sales_enriched_view():
    df=spark.readStream.table("append_sales_staged")
    df=df.withColumn("total_amount",col('quantity')*col('amount'))
    return df

dlt.create_streaming_table(
    name="transform_sales"
)

dlt.create_auto_cdc_flow(
    target="transform_sales",
    source="sales_enriched_view",
    keys=["sales_id"],
    sequence_by="sale_timestamp",
    stored_as_scd_type="1",
    ignore_null_updates=False
)