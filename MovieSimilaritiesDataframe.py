import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def loadMovieNames(spark, path):
    lines = spark.read.text(path)

    # We want id and name.  read.text returns a df that looks like:  
    # value:  
    # 1::Toy Story (1995)::Animation|Children's|Comedy
    # 2::Jumanji (1995)::Adventure|Children's|Fantasy
    
    # We select the value column, set a separator, then split the values column based on that separator, call it parts, and then:  
    #return a column called parts whose values are arrays of strings, and put it into a dataframe
    splitDF = lines.select(
        F.split(F.col("value"), "::").alias("parts")
    )

    #Now we need to get only the first two columns, id and name.  

    #parts looks kinda like this:  
    #value n:  [str, str, str, str]

    relevant_df = splitDF.select(F.col("parts")[0].cast("int").alias("movieID"), F.col("parts")[1].alias("movieName"))
    # The relevant df is the column parts' array index 0 as an int, and index 1 as a string

    #Convert to dictionary now, and return it.  
    rows = relevant_df.collect()

    movie_dictionary = {
        row.movieID: row.movieName for row in rows
    }

    return movie_dictionary


spark = SparkSession.builder.appName("MovieSimilaritiesWithDFs").getOrCreate()
# What should the master be?  

# We need to make it save to parquet, too.  Probably at a different path, as well?  
MOVIES_PATH = "s3a://revature-training-389009238812-us-east-2-an/ml-1m/movies.dat"
RATINGS_PATH = "s3a://revature-training-389009238812-us-east-2-an/ml-1m/ratings.dat"

# How to do a broadcast with sparksession instead of spark context?  
print("Loading movie names from S3...")
nameDict = spark.sparkContext.broadcast(loadMovieNames(spark, MOVIES_PATH))

print("Loading ratings from S3...")
data = spark.read.text(RATINGS_PATH)

# We do more or less the same thing as our loadMovieNames, except this dataframe stays distributed instead of broadcasted.  
# We do not need to send the movie ratings to all nodes.  
split_df = data.select(F.split(F.col("value"), "::").alias("parts"))
relevant_df = split_df.select(
    F.col("parts")[0].cast("int").alias("userID"), \
    F.col("parts")[1].cast("int").alias("movieID"), \
    F.col("parts")[2].cast("float").alias("rating")
)

partitioned_ratings = relevant_df.repartition(100, "userID")

r1 = partitioned_ratings.alias("r1")
r2 = partitioned_ratings.alias("r2")

joined_ratings = r1.join(r2, F.col("r1.userID") == F.col("r2.userID"), "inner")

unique_joined_ratings = joined_ratings.filter(F.col("r1.movieID") < F.col("r2.movieID"))

movie_pairs = unique_joined_ratings.select(
    F.col("r1.movieID").alias("movie1"),
    F.col("r2.movieID").alias("movie2"),
    F.col("r1.rating").alias("rating1"),
    F.col("r2.rating").alias("rating2")
)

movie_pair_similarities = movie_pairs.groupBy(
    "movie1",
    "movie2"
).agg(
    F.sum(F.col("rating1") * F.col("rating2")).alias("sum_xy"),
    F.sum(F.col("rating1") * F.col("rating1")).alias("sum_xx"),
    F.sum(F.col("rating2") * F.col("rating2")).alias("sum_yy"),
    F.count("*").alias("numPairs")
)

movie_pair_similarities = movie_pair_similarities.withColumn(
    "similarity",
    F.when(
        (F.sqrt("sum_xx") * F.sqrt("sum_yy")) != 0,
        F.col("sum_xy") / (F.sqrt("sum_xx") * F.sqrt("sum_yy"))
    ).otherwise(0.0)
)

movie_pair_similarities = movie_pair_similarities.persist()

movie_pair_similarities.write.mode("overwrite").parquet("s3a://revature-training-389009238812-us-east-2-an/output/movie-sims-parquet")

# Only run if spark-submit received a valid input basically.  
if len(sys.argv) > 1:

    # the id we are looking for
    movieID = int(sys.argv[1])

    scoreThreshold = 0.97
    coOccurrenceThreshold = 50

    filteredResults = movie_pair_similarities.filter(
        (
            (F.col("movie1") == movieID) |
            (F.col("movie2") == movieID)
        )
        &
        (F.col("similarity") > scoreThreshold)
        &
        (F.col("numPairs") > coOccurrenceThreshold)
    )

    results = filteredResults.orderBy(F.col("similarity").desc()) \
        .limit(10) \
        .collect()

    print("\nTop 10 similar movies for:",
          nameDict.value[movieID])

    for row in results:
        similarMovieID = (
            row.movie2
            if row.movie1 == movieID
            else row.movie1
        )

        print(
            nameDict.value[similarMovieID],
            "\tscore:", row.similarity,
            "\tstrength:", row.numPairs
        )

spark.stop()