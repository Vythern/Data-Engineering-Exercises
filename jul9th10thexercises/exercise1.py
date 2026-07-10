from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("Exercise1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

dataset = [
    (1, "Alice", 25, "NY"),
    (2, "Bob", None, "LA"),
    (2, "Bob", None, "LA"),
    (3, None, 0, "NY")
]

df = spark.createDataFrame(dataset, ["id", "name", "age", "state"])



df = df.dropDuplicates(["id"])

# avg already skips null, no need to filter for null again.  
averageAge = df.select(F.avg("age")).collect()[0][0]

# I don't know what to do.  Every resource I have says that this code should work, but it just doesn't run on my device.  fillna is broken idk

df = df.fillna(value=averageAge, subset=["age"])
df = df.fillna(value="Unknown", subset=["name"])

df = df.filter(F.col("age") > 30)


df.show()

spark.stop()