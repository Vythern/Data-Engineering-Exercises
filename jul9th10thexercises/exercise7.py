# Given clickstream events:

# user
# timestamp

# Create sessions where a new session starts if the gap exceeds 30 minutes.

# Tests:

# window functions
# timestamp arithmetic
# cumulative IDs


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession.builder.master("local[2]").appName("Exercise7").getOrCreate()


dataset = [
    # User A - 2 sessions
    ("A", "2026-01-01 09:00:00"),
    ("A", "2026-01-01 09:10:00"),
    ("A", "2026-01-01 09:25:00"),   # 15 min gap

    ("A", "2026-01-01 10:05:00"),   # 40 min gap -> new session
    ("A", "2026-01-01 10:20:00"),

    # User B - 3 sessions
    ("B", "2026-01-01 08:00:00"),
    ("B", "2026-01-01 08:20:00"),

    ("B", "2026-01-01 09:00:00"),   # 40 min gap -> new session
    ("B", "2026-01-01 09:15:00"),

    ("B", "2026-01-01 11:00:00"),   # 1h45 gap -> new session

    # User C - one continuous session
    ("C", "2026-01-01 12:00:00"),
    ("C", "2026-01-01 12:05:00"),
    ("C", "2026-01-01 12:25:00"),
    ("C", "2026-01-01 12:45:00"),
]

df = spark.createDataFrame(dataset, ["user", "timestamp"])

df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

window = Window.partitionBy(F.col("user")).orderBy(F.col("timestamp"))

df = df.withColumn("previous_timestamp", F.lag("timestamp").over(window))

df = df.withColumn(
    "gap", (
        F.col("timestamp").cast("long") - F.col("previous_timestamp").cast("long")
    ) / 60
)

df = df.withColumn("new_session", F.when(F.col("previous_timestamp").isNull() | (F.col("gap") > 30), 1).otherwise(0))

window2 = Window.partitionBy("user").orderBy("timestamp")

df = df.withColumn("session_id", F.sum(F.col("new_session")).over(window2))

df = df.select("user", "timestamp", "session_id")

df.show()

spark.stop()