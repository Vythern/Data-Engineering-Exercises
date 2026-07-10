# Input

# Month	Product	Sales
# Jan	A	10
# Jan	B	15
# Feb	A	12

# Output

# Month	A	B
# Jan	10	15
# Feb	12	null

# Tests:

# pivot

from pyspark.sql import SparkSession
from pyspark.sql import functions as F



spark = SparkSession.builder.master("local[2]").appName("Exercise8").getOrCreate()

dataset = [
    ("Jan", "A", 10),
    ("Jan", "B", 15),
    ("Jan", "C", 8),

    ("Feb", "A", 12),
    ("Feb", "C", 20),

    ("Mar", "B", 18),
    ("Mar", "C", 25),

    ("Apr", "A", 14),
    ("Apr", "B", 16),

    ("May", "A", 11),
    ("May", "B", 13),
    ("May", "C", 17),

    ("Jun", "B", 19),
]

df = spark.createDataFrame(dataset, ["month", "product", "amount"])

# We don't technically sum anything, since sales are already in the form of a sum, but
# spark requires it to get it back into a dataframe object instead of a group by object.  
df = df.groupBy(F.col("month")).pivot("product").sum("amount")

# Yeah it might have been unwise to generate so much data.  Now I've got to sort it.  
df = df.withColumn(
    "month_num",
    F.when(F.col("month") == "Jan", 1)
     .when(F.col("month") == "Feb", 2)
     .when(F.col("month") == "Mar", 3)
     .when(F.col("month") == "Apr", 4)
     .when(F.col("month") == "May", 5)
     .when(F.col("month") == "Jun", 6)
)

df = df.orderBy("month_num").drop("month_num")

df.show()

spark.stop()