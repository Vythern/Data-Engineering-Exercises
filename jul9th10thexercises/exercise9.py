# Input:

# [
#   ("Alice", ["Math", "Science"]),
#   ("Bob", ["History"])
# ]

# Output:

# name	subject
# Alice	Math
# Alice	Science
# Bob	History

# Tests:

# arrays
# explode


from pyspark.sql import SparkSession
from pyspark.sql import functions as F



spark = SparkSession.builder.master("local[2]").appName("Exercise9").getOrCreate()


dataset = [
    ("Alice", ["Math", "Science", "English"]),
    ("Bob", ["History"]),
    ("Charlie", ["Math", "Art"]),
    ("Diana", ["Science", "History", "Music"]),
    ("Ethan", ["Computer Science", "Math"]),
    ("Fiona", ["Biology"]),
    ("George", ["Physics", "Chemistry", "Math"]),
    ("Hannah", ["English", "History"]),
]

df = spark.createDataFrame(dataset, ["name", "subjects"])

df = df.select("name", F.explode("subjects"))

df.show()

spark.stop()