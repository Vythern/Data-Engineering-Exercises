# 15. End-to-End ETL Challenge (Excellent 60-minute Interview)

# Input:

# Orders:

# order_id
# customer_id
# date
# amount

# Customers:

# customer_id
# city
# segment

# Requirements:

# Clean missing values
# Remove duplicates
# Join datasets
# Calculate:
# total sales by city
# average order value by segment
# top 3 customers by spending
# Write the result partitioned by city


from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.master("local[2]").appName("Exercise15").getOrCreate()

order_data = [
    # order_id, customer_id, date, amount
    (1, 101, "2026-01-01", 100.0),
    (2, 101, "2026-01-05", 150.0),
    (3, 102, "2026-01-03", 200.0),
    (4, 103, "2026-01-04", 50.0),
    (5, 103, "2026-01-10", 75.0),
    (6, 104, "2026-01-07", 300.0),
    (7, 105, "2026-01-08", 125.0),

    # duplicate order
    (7, 105, "2026-01-08", 125.0),

    # missing amount
    (8, 102, "2026-01-12", None),

    # missing customer_id
    (9, None, "2026-01-15", 90.0),

    # another order for ranking customers
    (10, 104, "2026-01-20", 250.0),
    (11, 104, "2026-01-25", 100.0),
]

customer_data = [
    # customer_id, city, segment
    (101, "New York", "Premium"),
    (102, "Chicago", "Standard"),
    (103, "Chicago", "Standard"),
    (104, "Los Angeles", "Premium"),
    (105, "Boston", "Standard"),

    # customer with no orders
    (106, "Miami", "Premium"),

    # duplicate customer record
    (105, "Boston", "Standard"),
]

order_df = spark.createDataFrame(
    order_data,
    ["order_id", "customer_id", "date", "amount"]
)

customer_df = spark.createDataFrame(
    customer_data,
    ["customer_id", "city", "segment"]
)



# Clean missing values from orders
order_df = order_df.fillna(value=25, subset=["amount"]) #missing amount
#order_df = order_df.fillna(value=5, subset=["customer_id"]) #missing customer id.  Implies customer 5 is the default customer I guess.  
#This fill is most likely harmful in a real data setting.  For the exercise though, it is "educational"
# So let's just drop it instead.  
order_df = order_df.dropna(subset=["customer_id"])



# Remove duplicate orders and customers
order_df = order_df.drop_duplicates(["order_id"])
customer_df = customer_df.drop_duplicates(["customer_id"])

# Join datasets.  
joined = customer_df.join(order_df, customer_df.customer_id == order_df.customer_id, "inner")
joined = joined.drop(order_df.customer_id)


joined.cache() # We will use this for multiple things, so save it in memory / disk (default cache behaviour)

joined.show()

# Calculate:
# total sales by city
count_by_city = joined.groupBy(F.col("city")).count()

count_by_city.show()

# average order value by segment.  Use agg on the grouped object type.  
order_value_by_segment = joined.groupBy(F.col("segment")).agg(F.avg(F.col("amount")).alias("AVG Amount by segment"))

order_value_by_segment.show()

# top 3 customers by spending
# group by customer id, sort by sum of amounts descending, limit 3
top_three_by_spending = joined.groupBy(
    F.col("customer_id")) \
    .agg(F.sum(F.col("amount")).alias("total_spent")) \
    .sort(F.col("total_spent") \
    .desc()
).limit(3)

top_three_by_spending.show()

# Write the result partitioned by city
# joined.write \ # I don't want to actually generate the file but it'd look like this:  
#     .mode("overwrite") \
#     .partitionBy("city") \
#     .parquet("./output/customer_orders")



spark.stop()