from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType
from pyspark.sql.functions import regexp_extract, col
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("GetLogs").getOrCreate()

# Read access_lot.txt file.  

unfiltered_logs_df = spark.read.text("./access_log.txt")


# What's the easiest way to filter this access log?  massive regex?  
pattern = r'^(\S+).*?"\S+\s(\S+)\sHTTP/\d\.\d"\s(\d{3})'

logs = unfiltered_logs_df.select(
    f.regexp_extract("value", pattern, 1).alias("ip"),
    f.regexp_extract("value", pattern, 2).alias("endpoint"),
    f.regexp_extract("value", pattern, 3).alias("status")
)

# logs.printSchema()

# logs.show(10)



# Make a df that gets the logs grouped by ip, and gets the count of them
# Make a df that gets the logs grouped by endpoint, and gets the count of them
# Make a df that gets the logs grouped by status, and gets the count of them


ip_requests = logs.groupBy("ip").count().orderBy("count", ascending=False)

endpoint_requests = logs.groupBy("endpoint").count().orderBy("count", ascending=False)

status_codes = logs.groupBy("status").count().orderBy("count", ascending=False)


ip_requests.show()

endpoint_requests.show()

status_codes.show()

spark.stop()