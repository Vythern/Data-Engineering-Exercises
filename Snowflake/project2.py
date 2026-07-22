from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, BooleanType
from pyspark.sql import functions as F



#------------------------------ #
# Build spark session / setup:  
# ----------------------------- #

spark = (
    SparkSession.builder

    # Set a name for the Spark application (shows up in Spark UI/logs).
    .appName("Project2SparkSnowflake")

    # Enable Apache Iceberg SQL extensions so Spark understands
    # Iceberg-specific SQL commands and table operations.
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Register a catalog named "glue_catalog".
    # Spark will use this catalog whenever tables are referenced with
    # the prefix "glue_catalog".
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # Tell Spark that this catalog should use AWS Glue
    # as the metadata store for Iceberg tables.
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    # Specify the S3 warehouse location where Iceberg table data
    # and metadata files will be stored.
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://revature-training-389009238812-us-east-2-an/iceberg/"
    )

    # Configure Iceberg to use the S3FileIO implementation
    # for reading and writing data in Amazon S3.
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    # Create the Spark session with all of the above settings.
    .getOrCreate()
)


#---------------- #
# Setup Schemas:  
# --------------- #

# Made many of the fields string type.  Values can be lost if they are read and discovered as invalid for their type.  
# Eg, remove dollar signs and commas and convert to StringType() so we can clean things like "$14.99" and "399,99"

orders_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("order_date", StringType()),
    StructField("ship_date", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", StringType()),
    StructField("discount_pct", StringType()),
    StructField("total_amount", StringType()),
    StructField("payment_method", StringType()),
    StructField("order_status", StringType())
])

products_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("price", StringType()),
    StructField("cost", StringType()),
    StructField("stock_quantity", IntegerType()),
    StructField("weight_kg", StringType()),
    StructField("created_date", StringType()),
    StructField("is_active", StringType())
])

customers_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("signup_date", StringType()),
    StructField("country", StringType()),
    StructField("state", StringType()),
    StructField("postal_code", StringType()),
    StructField("is_active", StringType()),
    StructField("loyalty_points", IntegerType())
])

#------------------------------------------------- #
# Read the csv data from our S3, ensure it works:  
# ------------------------------------------------ #
orders_df = spark.read.options(header=True, schema=orders_schema).csv("s3://revature-training-389009238812-us-east-2-an/orders.csv") # Need to move these to the s3 bucket.  

products_df = spark.read.options(header=True, schema=products_schema).csv("s3://revature-training-389009238812-us-east-2-an/products.csv") # Need to move these to the s3 bucket.  

customers_df = spark.read.options(header=True, schema=customers_schema).csv("s3://revature-training-389009238812-us-east-2-an/customers.csv") # Need to move these to the s3 bucket.  

# Create an Iceberg db if it isn't already there
spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_catalog_db
""")



# Verify the data was loaded correctly.
orders_df.printSchema()
products_df.printSchema()
customers_df.printSchema()




# Remove any duplicate ids
cleaned_customers = customers_df.dropDuplicates(["customer_id"])

# for each string field, remove trailing and heading spaces.  
for field in cleaned_customers.schema.fields:
    if isinstance(field.dataType, StringType):
        trimmed_value = F.trim(F.col(field.name))

        cleaned_customers = cleaned_customers.withColumn(
            field.name,
            F.when(
                trimmed_value == "",
                None
            ).otherwise(trimmed_value)
        )

# Normalize boolean values
cleaned_customers = cleaned_customers.withColumn(
    "is_active",
    F.when(
        F.upper(F.col("is_active")).isin("TRUE", "YES", "Y"),
        F.lit(True)
    )
    .when(
        F.upper(F.col("is_active")).isin("FALSE", "NO", "N"),
        F.lit(False)
    )
)

# Normalize state casing
cleaned_customers = cleaned_customers.withColumn(
    "state",
    F.upper(F.col("state"))
)

# Since we have a small dataset, we can hard code this (and potentially all other states),
# but realistically, cleaning this data requires mapping for each state, and probably checks for typos.  
cleaned_customers = cleaned_customers.withColumn(
    "state",
    F.when(
        F.upper(F.col("state")) == "MINNESOTA",
        "MN"
    )
    .otherwise(F.upper(F.col("state")))
)

# Normalize country values
cleaned_customers = cleaned_customers.withColumn(
    "country",
    F.when(
        F.upper(F.col("country")).isin(
            "USA",
            "U.S.A.",
            "UNITED STATES"
        ),
        "USA"
    )
    .when(
        F.upper(F.col("country")) == "CANADA",
        "Canada"
    )
    .otherwise(F.col("country"))
)

cleaned_customers = cleaned_customers.withColumn(
    "signup_date",
    F.coalesce(
        F.expr("try_to_timestamp(signup_date, 'yyyy-MM-dd')").cast("date"),
        F.expr("try_to_timestamp(signup_date, 'yyyy/MM/dd')").cast("date"),
        F.expr("try_to_timestamp(signup_date, 'MM-dd-yyyy')").cast("date")
    )
)
# Regex to validate emails
email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

# append fixed pattern to our customers df
cleaned_customers = cleaned_customers.withColumn(
    "email",
    F.when(
        F.col("email").rlike(email_pattern),
        F.lower(F.col("email"))
    )
)

# Integers only in the phone number
cleaned_customers = cleaned_customers.withColumn(
    "phone",
    F.regexp_replace(F.col("phone"), r"\D", "")
)

# Ensure valid number length.  
cleaned_customers = cleaned_customers.withColumn(
    "phone",
    F.when(
        F.length(F.col("phone")).between(10, 15),
        F.col("phone")
    )
)

# No negative or null loyalty points
cleaned_customers = cleaned_customers.withColumn(
    "loyalty_points",
    F.when(
        F.col("loyalty_points").isNull(),
        F.lit(0)
    ).when(
        F.col("loyalty_points") < 0,
        F.lit(0)
    ).otherwise(F.col("loyalty_points"))
)

# If a customer has no id, name, email, or signup date, we drop the record.  
cleaned_customers = cleaned_customers.dropna(
    subset=[
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "signup_date"
    ]
)






#-------------------- #
# Clean orders next:  
# ------------------- #

# drop duplicates
# Remove duplicate orders
cleaned_orders = orders_df.dropDuplicates(["order_id"])

# Trim all string columns and replace empty strings with null
for field in cleaned_orders.schema.fields:
    if isinstance(field.dataType, StringType):
        trimmed = F.trim(F.col(field.name))

        cleaned_orders = cleaned_orders.withColumn(
            field.name,
            F.when(trimmed == "", None).otherwise(trimmed)
        )

null_values = ["NULL", "null", "N/A", "NA", ""]

for col in cleaned_orders.columns:
    cleaned_orders = cleaned_orders.withColumn(
        col,
        F.when(
            F.col(col).isin(null_values),
            None
        ).otherwise(F.col(col))
    )

# Validate product IDs with a bit of regex
product_pattern = r"^P\d+$"

cleaned_orders = cleaned_orders.withColumn(
    "product_id",
    F.when(
        F.col("product_id").rlike(product_pattern),
        F.col("product_id")
    )
)

# Parse ship and order dates in every format
cleaned_orders = cleaned_orders.withColumn(
    "order_date",
    F.coalesce(
        F.expr("try_to_timestamp(order_date, 'yyyy-MM-dd')").cast("date"),
        F.expr("try_to_timestamp(order_date, 'yyyy/MM/dd')").cast("date"),
        F.expr("try_to_timestamp(order_date, 'MM-dd-yyyy')").cast("date")
    )
)

cleaned_orders = cleaned_orders.withColumn(
    "ship_date",
    F.coalesce(
        F.expr("try_to_timestamp(ship_date, 'yyyy-MM-dd')").cast("date"),
        F.expr("try_to_timestamp(ship_date, 'yyyy/MM/dd')").cast("date"),
        F.expr("try_to_timestamp(ship_date, 'MM-dd-yyyy')").cast("date")
    )
)

# Remove the $ sign and convert to float
cleaned_orders = cleaned_orders.withColumn(
    "unit_price",
    F.regexp_replace("unit_price", r"\$", "")
)

# Keep only valid numeric unit prices
cleaned_orders = cleaned_orders.filter(
    F.col("unit_price").rlike(r"^\d+(\.\d+)?$")
)

# Convert unit_price to float
cleaned_orders = cleaned_orders.withColumn(
    "unit_price",
    F.col("unit_price").cast("float")
)


# Validate discount_pct before converting
cleaned_orders = cleaned_orders.filter(
    F.col("discount_pct").rlike(r"^\d+(\.\d+)?$")
)

# Convert discount_pct to float
cleaned_orders = cleaned_orders.withColumn(
    "discount_pct",
    F.col("discount_pct").cast("float")
)


# Remove the $ sign from total_amount
cleaned_orders = cleaned_orders.withColumn(
    "total_amount",
    F.regexp_replace("total_amount", r"\$", "")
)

# Keep only valid numeric total amounts
cleaned_orders = cleaned_orders.filter(
    F.col("total_amount").rlike(r"^\d+(\.\d+)?$")
)

# Convert total_amount to float
cleaned_orders = cleaned_orders.withColumn(
    "total_amount",
    F.col("total_amount").cast("float")
)

# Ensure ship date is after order date
cleaned_orders = cleaned_orders.withColumn(
    "ship_date",
    F.when(
        F.col("ship_date") >= F.col("order_date"),
        F.col("ship_date")
    )
)

# Quantity and unit price must both
cleaned_orders = cleaned_orders.filter(
    F.col("quantity") > 0
)
cleaned_orders = cleaned_orders.filter(
    F.col("unit_price") > 0
)

# Fix discount between 0 and 100%
cleaned_orders = cleaned_orders.withColumn(
    "discount_pct",
    F.when(
        F.col("discount_pct").between(0, 100),
        F.col("discount_pct")
    ).otherwise(F.lit(0.0))
)

# Normalize payment types
cleaned_orders = cleaned_orders.withColumn(
    "payment_method",
    F.when(
        F.col("payment_method").isin("Visa", "MasterCard"),
        "Credit Card"
    )
    .when(
        F.col("payment_method") == "Debit",
        "Debit Card"
    )
    .otherwise(F.col("payment_method"))
)

# Keep only valid statuses
valid_statuses = [
    "Delivered",
    "Pending",
    "Cancelled",
    "Returned",
    "Shipped"
]


cleaned_orders = cleaned_orders.withColumn(
    "order_status",
    F.when(
        F.col("order_status").isin(valid_statuses),
        F.col("order_status")
    )
)

# Recalculate totals with discount percent
cleaned_orders = cleaned_orders.withColumn(
    "total_amount",
    F.round(
        F.col("quantity") *
        F.col("unit_price") *
        (1 - F.col("discount_pct") / 100),
        2
    )
)

# If an order has no id, customer, product, date, quantity, or price, drop it.  
cleaned_orders = cleaned_orders.dropna(
    subset=[
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price"
    ]
)





#-------------------- #
# Finally, products
# ------------------- #

# Remove duplicate products by id
cleaned_products = products_df.dropDuplicates(["product_id"])

# Remove leading and trailing spaces
for field in cleaned_products.schema.fields:
    if isinstance(field.dataType, StringType):
        trimmed = F.trim(F.col(field.name))

        cleaned_products = cleaned_products.withColumn(
            field.name,
            F.when(trimmed == "", None).otherwise(trimmed)
        )

# Validate ids with regex
product_pattern = r"^P\d+$"

cleaned_products = cleaned_products.withColumn(
    "product_id",
    F.when(
        F.col("product_id").rlike(product_pattern),
        F.col("product_id")
    )
)

# Remove dollar signs and commas
cleaned_products = cleaned_products.withColumn(
    "price",
    F.regexp_replace("price", r"\$", "")
)

# Replace comma decimals if needed
cleaned_products = cleaned_products.withColumn(
    "price",
    F.regexp_replace("price", ",", ".")
)

# Keep only valid numeric prices
cleaned_products = cleaned_products.filter(
    F.col("price").rlike(r"^\d+(\.\d+)?$")
)

# Convert price to float
cleaned_products = cleaned_products.withColumn(
    "price",
    F.col("price").cast("float")
)


# Clean and convert cost to float
cleaned_products = cleaned_products.withColumn(
    "cost",
    F.regexp_replace("cost", r"\$", "")
)

# Keep only valid numeric costs
cleaned_products = cleaned_products.filter(
    F.col("cost").rlike(r"^\d+(\.\d+)?$")
)

# Convert cost to float
cleaned_products = cleaned_products.withColumn(
    "cost",
    F.col("cost").cast("float")
)


# Clean and convert weight to float
# Keep only valid numeric weights
cleaned_products = cleaned_products.filter(
    F.col("weight_kg").rlike(r"^\d+(\.\d+)?$")
)

# Convert weight to float
cleaned_products = cleaned_products.withColumn(
    "weight_kg",
    F.col("weight_kg").cast("float")
)

# Parse dates in every format
cleaned_products = cleaned_products.withColumn(
    "created_date",
    F.coalesce(
        F.expr("try_to_timestamp(created_date, 'yyyy-MM-dd')").cast("date"),
        F.expr("try_to_timestamp(created_date, 'yyyy/MM/dd')").cast("date"),
        F.expr("try_to_timestamp(created_date, 'MM-dd-yyyy')").cast("date")
    )
)


# Normalize boolean values.
cleaned_products = cleaned_products.withColumn(
    "is_active",
    F.when(
        F.upper(F.trim(F.col("is_active"))).isin("TRUE", "YES", "Y"),
        F.lit(True)
    )
    .when(
        F.upper(F.trim(F.col("is_active"))).isin("FALSE", "NO", "N"),
        F.lit(False)
    )
)

# Price must be positive, quantity, weight, and costs must be at least 0
cleaned_products = cleaned_products.filter(
    F.col("price") > 0
)
cleaned_products = cleaned_products.filter(
    F.col("cost") >= 0
)
cleaned_products = cleaned_products.withColumn(
    "stock_quantity",
    F.when(
        F.col("stock_quantity") < 0,
        F.lit(0)
    ).otherwise(F.col("stock_quantity"))
)
cleaned_products = cleaned_products.withColumn(
    "weight_kg",
    F.when(
        F.col("weight_kg") >= 0,
        F.col("weight_kg")
    ).otherwise(None)
)

# Capitalize brand
cleaned_products = cleaned_products.withColumn(
    "brand",
    F.initcap(F.col("brand"))
)

# Finally, drop any rows with missing fields.  
cleaned_products = cleaned_products.dropna(
    subset=[
        "product_id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "created_date",
        "is_active"
    ]
)

# We use an AWS Glue Catalog to create / register an Iceberg table so that Spark / Snowflake- 
# and other tools can discover and access it.
cleaned_customers.writeTo("glue_catalog.iceberg_catalog_db.customers").using("iceberg").createOrReplace()
cleaned_orders.writeTo("glue_catalog.iceberg_catalog_db.orders").using("iceberg").createOrReplace()
cleaned_products.writeTo("glue_catalog.iceberg_catalog_db.products").using("iceberg").createOrReplace()

# Check whether or not the data can be successfully found
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.customers
LIMIT 10
""").show()

spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.orders
LIMIT 10
""").show()

spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.products
LIMIT 10
""").show()


cleaned_customers = cleaned_customers.withColumnRenamed(
    "is_active",
    "customer_is_active"
)

cleaned_products = cleaned_products.withColumnRenamed(
    "is_active",
    "product_is_active"
)



order_details = (
    cleaned_orders
    .join(cleaned_customers, "customer_id", "inner")
    .join(cleaned_products, "product_id", "inner")
)

order_details.writeTo(
    "glue_catalog.iceberg_catalog_db.order_details"
).using("iceberg").createOrReplace()


spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.order_details
LIMIT 10
""").show()


# Stop the Spark session and release cluster resources.
spark.stop()



#SETUP:  
#create a general purpose s3 bucket (defaults), and uploaded the project folder's files into it.  
#clone a cluster.  name it generatingIcebergTable_project_2
#enable aws glue data catalog settings.  
# all other default "should be fine for us".  
#automated close after 1hr inactivity.  
#classificaiton iceberg, properties:  iceberg.enabled: true
#ec2 key pair for ssh:  spark?  Same as last time.  
#default emr role, choose an existing service role.  
#same instance profile.  

#powershell:  ssh -i ~/spark.pem hadoop@ec2 [something].  copy from the node SSH when up and running.  

#go to the priamry instance id, ec2 > instances > ID.  
#check inbound rules, security group, ec2 > sec groups > string of text > edit inbound:  set custom IP in there.  

#connect using ssh.  hadroop@ec2 and after.  


