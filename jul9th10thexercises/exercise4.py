# 4. Joins

# Customers

# id	name
# 1	Alice
# 2	Bob

# Orders

# customer_id	amount
# 1	100
# 1	50
# 3	25

# Questions:

# Inner join
# Left join
# Customers with no orders
# Total spend per customer

# Tests:

# joins
# null handling
# aggregations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("Exercise4").getOrCreate()

customers = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "Diana"),
    (5, "Evan"),
]

orders = [
    (1, 100),
    (1, 50),
    (1, 75),

    (2, 200),
    (2, 150),

    (3, 25),
    (3, 300),

    (4, 80),
]

customers = spark.createDataFrame(customers, ["id", "name"])
orders = spark.createDataFrame(orders, ["customer_id", "amount"])

# Inner join
# Left join
# Customers with no orders
# Total spend per customer
inner_joined = customers.join(orders, customers.id == orders.customer_id, "inner")
inner_joined = inner_joined.cache()

inner_joined.show()

left_joined = customers.join(orders, customers.id == orders.customer_id, "left")

left_joined.show()

# Customers with no orders is a left anti join.  
left_anti = customers.join(orders, customers.id == orders.customer_id, "left_anti")

left_anti.show()

# get inner join so we only have customers with values
# group by customer
# sum amount column
# 

total_spent = inner_joined.groupBy(F.col("customer_id")).sum("amount").alias("Total Spent")

total_spent.show()

spark.stop()