import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col


class TestSparkJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("Test Optimized Spark Job without DOB Anonymization") \
            .master("local[*]") \
            .getOrCreate()

        # Define schema for input data
        cls.schema = StructType([
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("date_of_birth", DateType(), True)
        ])

    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()

    def test_combined_dataframe(self):
        # Create test input data
        data1 = [
            ("John", "Doe", "123 Main St", "1980-01-01"),
            ("Jane", "Smith", "456 Elm St", "1990-02-02")
        ]
        data2 = [
            ("Alice", "Brown", "789 Oak St", "1975-03-03"),
            ("Bob", "White", "321 Pine St", "1985-04-04")
        ]

        # Create DataFrames from test data
        df1 = self.spark.createDataFrame(data1, schema=self.schema)
        df2 = self.spark.createDataFrame(data2, schema=self.schema)

        # Combine the datasets
        combined_df = df1.union(df2)

        # Assert combined row count
        self.assertEqual(combined_df.count(), 4)

    def test_anonymization_logic(self):
        # Create test input data
        data = [
            ("John", "Doe", "123 Main St", "1980-01-01"),
            ("Jane", "Smith", "456 Elm St", "1990-02-02")
        ]

        # Create DataFrame from test data
        input_df = self.spark.createDataFrame(data, schema=self.schema)

        # Anonymize data
        anonymized_df = input_df \
            .withColumn("first_name", expr("concat('Anon_', monotonically_increasing_id())")) \
            .withColumn("last_name", expr("concat('Anon_', monotonically_increasing_id())")) \
            .withColumn("address", expr("concat('Anon_Address_', monotonically_increasing_id())")) \
            .withColumn("date_of_birth", col("date_of_birth"))

        # Collect results
        anonymized_data = anonymized_df.collect()

        # Validate anonymization
        for row in anonymized_data:
            self.assertTrue(row["first_name"].startswith("Anon_"))
            self.assertTrue(row["last_name"].startswith("Anon_"))
            self.assertTrue(row["address"].startswith("Anon_Address_"))

        # Ensure date_of_birth is unchanged
        self.assertEqual(input_df.select("date_of_birth").collect(),
                         anonymized_df.select("date_of_birth").collect())

    def test_output_partitioning(self):
        # Create test input data
        data = [
            ("John", "Doe", "123 Main St", "1980-01-01"),
            ("Jane", "Smith", "456 Elm St", "1990-02-02")
        ]

        # Create DataFrame from test data
        input_df = self.spark.createDataFrame(data, schema=self.schema)

        # Write output to a temporary location with fewer partitions
        output_path = "test_output"
        input_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

        # Read output back
        output_df = self.spark.read.csv(output_path, header=True)

        # Assert row count matches input
        self.assertEqual(input_df.count(), output_df.count())


if _name_ == "_main_":
    unittest.main()