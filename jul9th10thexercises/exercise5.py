# 5. Find Duplicate Records

# Dataset:

# email
# -----
# a@test.com
# b@test.com
# a@test.com
# c@test.com

# Tasks:

# Find duplicate emails
# Count occurrences
# Keep only unique rows

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[2]").appName("Exercise2").getOrCreate()

data = [
    ("a@test.com",),
    ("b@test.com",),
    ("a@test.com",),
    ("c@test.com",),
]

df = spark.createDataFrame(data, ["email"])

# Find duplicate emails
# Count occurrences
# Keep only unique rows

dupes = df.groupBy("email").count().filter(F.col("count") > 1)

dupes.show()



uniques = df.dropDuplicates(["email"])

uniques.show()

spark.stop()