from pyspark.sql import SparkSession, functions as Func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName('Superheroes').getOrCreate()


schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])

names = spark.read.schema(schema).option('sep', ' ').csv('./MarvelNames.txt')
lines = spark.read.text('./MarvelGraph.txt')

connections = lines.withColumn('id', Func.split(Func.col('value'), ' ')[0]) \
.withColumn('connections', Func.size(Func.split(Func.col('value'), ' ')) -1) \
.groupBy('id').agg(Func.sum('connections').alias('connections'))

minConnections = connections.agg(Func.min("connections")).first()[0]

leastPopular = connections.orderBy(Func.col("connections").asc())

leastPopular = connections.filter(
    Func.col("connections") == minConnections
)


leastPopularWithNames = leastPopular.join(names, "id")


leastPopularWithNames.show()





spark.stop()