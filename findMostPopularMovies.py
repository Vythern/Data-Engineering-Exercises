from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType
from pyspark .sql import functions as f

spark = SparkSession.builder.appName("FindMostPopularMovies").getOrCreate()

# user id | item id | rating | timestamp. 

schema = StructType([
    StructField("id", IntegerType()),
    StructField("itemID", IntegerType()),
    StructField("rating", FloatType()),
    StructField("timestamp", IntegerType())
])


# Use tabs as separators
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")

# We need to get rid of the user id and timestamp of the rating

moviesDF = moviesDF.select("itemID", "rating")

# Group movies by id, sorted by highest average rating, descending.  

moviesDF.printSchema()

print(moviesDF.groupBy("itemID") \
    .agg(f.avg("rating").alias("Average Rating")) \
    .withColumnRenamed("itemID", "Movie ID") \
    .sort("Average Rating", ascending=False) \
    .show(10))

spark.stop()