import dlt

rules = {
    "rule1" : "customer_id IS NOT NULL",
    "rule2" : "customer_name IS NOT NULL",
    "rule3" : "region IS NOT NULL"
}

@dlt.table(
    name="customers_staged"
)
@dlt.expect_all_or_drop(rules)

def customers_staged():
    df=spark.readStream.table("vsarthidlt.dwh.customers")
    return df