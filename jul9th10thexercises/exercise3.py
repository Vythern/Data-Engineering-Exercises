# 3. Top N per Group

# Example:

# department	employee	salary
# IT	John	100
# IT	Mary	200
# IT	Alex	150

# Return the highest-paid employee in each department.

# Expected solution:

# Use a Window function.

# Tests:

# Window.partitionBy
# row_number
# rank
# dense_rank


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession.builder.master("local[2]").appName("Exercise2").getOrCreate()

dataset = [
    ("IT", "John", 100),
    ("IT", "Mary", 200),
    ("IT", "Alex", 150),
    ("IT", "Sarah", 200),
    ("IT", "Mike", 120),

    ("HR", "David", 130),
    ("HR", "Emily", 180),
    ("HR", "Chris", 180),
    ("HR", "Anna", 110),
    ("HR", "James", 160),

    ("Finance", "Robert", 250),
    ("Finance", "Lisa", 300),
    ("Finance", "Mark", 220),
    ("Finance", "Jennifer", 300),
    ("Finance", "Tom", 200),

    ("Sales", "Brian", 170),
    ("Sales", "Karen", 210),
    ("Sales", "Steve", 190),
    ("Sales", "Rachel", 210),
    ("Sales", "Paul", 150),
]

df = spark.createDataFrame(dataset, ["department", "employee", "salary"])


# windows operate on partitions.  So when we order by salary descending, it first partitions them into separate 
# groups based on the partition column.  
window = Window.partitionBy("department").orderBy(F.col("salary").desc())

# So with this we add a column called rank, and assign the value of the index in the partition to each of the rows
ranked = df.withColumn("rank", F.row_number().over(window))

filtered = ranked.filter(F.col("rank") == 1)

filtered.show()


spark.stop()