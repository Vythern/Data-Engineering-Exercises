from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Decided to try to practice dataframe syntax by writing some code from memory
# I was struggling to see why udf annotations / etc were necessary.  
# Had to do some googling here and there but I'm feeling a lot better about things now.  
# Not sure if I'd be able to reproduce this 100% perfect from scratch, but I'd get 95% of it.  
# The immports are tricky, and F.col() kinda confuses me.  
# f.col(something) refers to a column by "name" that is present in the dataframe where the wrapping(function) was called from
# Then UDFs are kinda special- they apply an operation to every row.  That's why returning a "str" doesn't break everything.  
# Because a UDF does it for all the rows.  
# If we sent f.col(name), age, etc to a non-udf of the same kind, it wouldn't know what to do.  
# But f.col and our udf both kinda work under the hood like a for row in column:  do(the udf)


data = [
    ("Alice", "Johnson", 29, "Engineering"),
    ("Bob", "Smith", 41, "Finance"),
    ("Charlie", "Brown", 22, "Engineering"),
    ("Diana", "Miller", 35, "Marketing"),
    ("Edward", "Davis", 31, "Finance"),
    ("Fiona", "Wilson", 27, "Engineering"),
]


@F.udf
def generate_username(firstName: str, lastName: str, age: int) -> str:
    return ( firstName[0] + lastName + str(age)[-2])





schema = StructType([
    StructField("firstName", StringType()), \
    StructField("lastName", StringType()), \
    StructField("age", IntegerType()), \
    StructField("department", StringType())
])





spark = SparkSession.builder.appName("PracticingSparkStuff").master("local").getOrCreate()

df = spark.createDataFrame(data, schema=schema)

employees_with_usernames = df.withColumn("Username", generate_username(F.col("firstName"), F.col("lastName"), F.col("age")))

employees_with_usernames.show()

spark.stop()