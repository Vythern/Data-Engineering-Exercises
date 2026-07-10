# 6. Window Function Challenge

# Transactions

# user	date	amount
# A	Jan1	10
# A	Jan2	20
# A	Jan3	15

# Questions:

# Running total
# Previous transaction
# Difference from previous transaction
# Rolling 7-day average

# Tests:

# lag
# lead
# cumulative sums
# window frames

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession.builder.master("local[2]").appName("Exercise6").getOrCreate()

    
    
dataset = [
    ("A", "Jan1", 10),
    ("A", "Jan2", 20),
    ("A", "Jan3", 15),
]

df = spark.createDataFrame(dataset, ["user", "date", "amount"])

#we can use a window to create partitions based on the user, then get the sum at each date
#include all preceding, up to current row is default behaviour
window = Window.partitionBy("user").orderBy("date").rangeBetween(Window.unboundedPreceding, Window.currentRow)

running_totals = df.withColumn("running_total", F.sum("amount").over(window))

running_totals.show()

lagWindow = Window.partitionBy("user").orderBy("date")

previous = df.withColumn(
    "previous_amount",
    F.lag("amount").over(lagWindow)
)

previous.show()


spark.stop()