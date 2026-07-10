# 2. Group By and Aggregation

# Sales data:

# customer, product, price
# A	Phone	500
# A	Laptop	1000
# B	Phone	700

# Questions:

# Total spend per customer
# Average purchase amount
# Highest spending customer

# Skills:

# groupBy
# agg
# sum
# avg
# orderBy


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("Exercise2").getOrCreate()

dataset = [
    ("A", "Phone", 500),
    ("A", "Laptop", 1000),
    ("B", "Phone", 700),
]

df = spark.createDataFrame(dataset, ["customer", "product", "price"])

totalSpent = df.groupBy("customer").agg(F.sum("price").alias("spendings"))

totalSpent.show()

averageSpent = df.groupBy("customer").avg("price")

averageSpent.show()

mostSpent = totalSpent.orderBy(F.col("spendings").desc()).limit(1)

mostSpent.show()

spark.stop()