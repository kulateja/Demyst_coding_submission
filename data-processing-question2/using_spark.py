from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import expr,col

# Define schema
schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("date_of_birth", DateType(), True)
])

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Optimized Spark Job without DOB Anonymization") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Read CSV files with proper partitioning
df1 = spark.read.csv("large_dataset.csv", header=True, schema=schema)



# Anonymize name and address using Spark SQL expressions
anonymized_df = df1 \
    .withColumn("first_name", expr("concat('Anon_', monotonically_increasing_id())")) \
    .withColumn("last_name", expr("concat('Anon_', monotonically_increasing_id())")) \
    .withColumn("address", expr("concat('Anon_Address_', monotonically_increasing_id())")) \
    .withColumn("date_of_birth", col("date_of_birth"))  # Keeping the DOB as it is

# Write output to CSV with fewer partitions
anonymized_df.coalesce(1).write.csv(
    "anonymized_combined_dataset_no_dob",
    header=True,
    mode="overwrite"
)

# Stop Spark Session
spark.stop()






#Demonstration for larger datasets

# Description 

# To adapt the solution for large datasets, several optimizations were implemented to ensure efficient distributed processing. 
# The Spark session was configured with increased memory limits for both the driver and executors, each allocated 8 GB, to handle the
# computational demands of larger data volumes. The number of shuffle partitions was set to 100, enabling better data distribution across 
# the cluster and reducing potential bottlenecks during shuffling operations.The output format was switched from CSV to Parquet, 
# a columnar storage format that provides better performance for large datasets due to its smaller size, faster I/O operations, 
# and compatibility with distributed frameworks. The anonymization logic remains intact, using Spark functions like 
# `monotonically_increasing_id` to generate unique and distributed-friendly identifiers. Additionally, logging was added to track the 
# number of partitions and ensure that the output is generated correctly.
# These modifications enhance the solution’s scalability, making it capable of efficiently processing millions of rows while optimizing 
# resource utilization in a distributed computing environment. This aligns with best practices for big data processing using Apache Spark.





# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, DateType
# from pyspark.sql.functions import expr, col, monotonically_increasing_id

# # Define schema
# schema = StructType([
#     StructField("first_name", StringType(), True),
#     StructField("last_name", StringType(), True),
#     StructField("address", StringType(), True),
#     StructField("date_of_birth", DateType(), True)
# ])

# # Initialize Spark Session with optimized settings
# spark = SparkSession.builder \
#     .appName("Optimized Spark Job with Large Dataset") \
#     .config("spark.executor.memory", "8g") \
#     .config("spark.driver.memory", "8g") \
#     .config("spark.sql.shuffle.partitions", "100") \
#     .getOrCreate()

# # Read CSV files with proper partitioning
# df1 = spark.read.csv(
#     "large_dataset.csv",
#     header=True,
#     schema=schema
# )

# # Log the number of partitions in the dataset
# print(f"Initial number of partitions: {df1.rdd.getNumPartitions()}")

# # Anonymize name and address using Spark SQL expressions
# anonymized_df = df1 \
#     .withColumn("first_name", expr("concat('Anon_', monotonically_increasing_id())")) \
#     .withColumn("last_name", expr("concat('Anon_', monotonically_increasing_id())")) \
#     .withColumn("address", expr("concat('Anon_Address_', monotonically_increasing_id())")) \
#     .withColumn("date_of_birth", col("date_of_birth"))  # Keeping the DOB as it is

# # Optimize write operation for large datasets
# # Write to Parquet format for better performance on larger datasets
# output_path = "anonymized_combined_dataset_no_dob"
# anonymized_df.write.parquet(
#     output_path,
#     mode="overwrite"
# )

# # Log success
# print(f"Anonymized dataset written to {output_path}")

# # Stop Spark Session
# spark.stop()
