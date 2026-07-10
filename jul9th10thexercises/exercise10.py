# 10. JSON Parsing

# Input:

# {
#   "customer":{
#       "id":1,
#       "city":"NY"
#   }
# }

# Tasks:

# Parse JSON
# Flatten nested columns
# Select nested fields

# Tests:

# from_json
# schemas
# structs

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, StringType




spark = SparkSession.builder.master("local[2]").appName("Exercise10").getOrCreate()

schema = StructType([
    StructField("customer", StructType([
        StructField("id", IntegerType()),
        StructField("city", StringType())
    ]))
])

dataset = [
    ('{"customer":{"id":1,"city":"NY"}}',),
    ('{"customer":{"id":2,"city":"LA"}}',)
]

# Just a single column called json data right now.  It has the customer values, which themselves are schema, containing two things
df = spark.createDataFrame(dataset, ["json_data"])

# df.show()

#functions has a from_json method for reading things into the schema
df = df.withColumn("customer_entries", F.from_json("json_data", schema))

# df.show()

df = df.select(
    F.col("customer_entries.customer.id"),
    F.col("customer_entries.customer.city")
)


df.show()

spark.stop()