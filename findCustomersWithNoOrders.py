from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

spark = SparkSession.builder.appName("FindCustomersWithNoOrders").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# customers.csv:  customer_id | first_name | last_name | email | city


# orders.csv:   order_id | customer_id | order_date | amount

customers_df = spark.read.csv("./customers.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("./orders.csv", header=True, inferSchema=True)

# SELECT (customers.customer_id), (customers.first_name), (customers.last_name) FROM customers INNER JOIN orders ON customers.customer_id = orders.customer_id
# We want everything that ISN'T in this query

# Left anti-join.  Get all customers where orders don't exist for that customer.  
# SELECT c.customer_id, c.first_name, c.last_name
# FROM customers c
# LEFT JOIN orders o
# ON c.customer_id = o.customer_id
# WHERE o.customer_id IS NULL;

customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")

results = spark.sql("""
    SELECT c.customer_id, c.first_name, c.last_name
    FROM customers c
    LEFT JOIN orders o
    ON c.customer_id = o.customer_id
    WHERE o.customer_id IS NULL
""")

print(results.collect())

spark.stop()

# expected output:

# | customer_id | first_name | last_name |
# | ----------- | ---------- | --------- |
# | 1002        | Bob        | Smith     |
# | 1004        | David      | Wilson    |
# | 1007        | Grace      | Moore     |
# | 1009        | Ivy        | Anderson  |
# | 1013        | Mia        | Martin    |
# | 1015        | Olivia     | Garcia    |