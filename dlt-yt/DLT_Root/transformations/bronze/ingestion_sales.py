import dlt 


rules={
    "rule1" : "sales_id IS NOT NULL",
    "rule2" : "customer_id IS NOT NULL",
    "rule3" : "product_id IS NOT NULL",
    "rule4" : "quantity > 0"
}

dlt.create_streaming_table(
    name = "append_sales_staged",
    expect_all_or_drop = rules
)

@dlt.append_flow(target="append_sales_staged")
def east_sales():
    df = spark.readStream.table("vsarthidlt.dwh.sales_east")
    return df
    
@dlt.append_flow(target="append_sales_staged")
def west_sales():
    df = spark.readStream.table("vsarthidlt.dwh.sales_west")
    return df