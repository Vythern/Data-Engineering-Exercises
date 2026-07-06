from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType
from pyspark.sql.functions import udf



# Type hint -> return a dictionary of int, str.  
def loadMovieNames() -> dict[int, str]:
    movieNames = {}

    with open('./ml-100k/u.item', 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames

spark = SparkSession.builder.appName("MovieRatingsWithNames").master("local").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#Broadcast the name of all movies.  
nameDict = spark.sparkContext.broadcast(loadMovieNames())


schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("movie_id", IntegerType()),
    StructField("rating", LongType()),
    StructField("timestamp", LongType())
])

movie_rating_DF = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")

movieRatingCounts = movie_rating_DF.groupby('movie_id').count()

@udf
def lookupName(movieId: int) -> str:
    return nameDict.value.get(movieId, "Unknown") #Default value if movieId not found.  Return unknown.

movieCountsWithNames = movieRatingCounts.withColumn(
    "movieTitle",
    lookupName(f.col("movie_id"))
)

sortedMovies = movieCountsWithNames.orderBy(f.desc('count'))

sortedMovies.show(10, False) # False stops truncation here.  

spark.stop()