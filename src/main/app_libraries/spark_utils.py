from pyspark.sql import SparkSession
import logging
import time
from pyspark.sql import functions as F
import json

# Data Loading and Saving Functions
def load_data(spark, file_path, format="csv"):
    """ Load data from a file into a Spark DataFrame. """
    return spark.read.format(format).load(file_path)

def save_data(df, file_path, format="csv",mode='overwrite'):
    """ Save a Spark DataFrame to a file. """
    df.write.format(format).save(file_path,mode=mode)

# Data Transformation Functions
def standardize_column_names(df):
    """ Standardize column names in a DataFrame (lowercase, replace spaces with underscores). """
    return df.toDF(*[c.lower().replace(' ', '_') for c in df.columns])

# Fill missing values
def fill_missing_values(df, value=0):
    """ Fill missing values in a DataFrame. """
    return df.fillna(value)

# Spark Context/Session Management
def create_spark_session(app_name="MyApp", master="local[*]"):
    """ Create and return a Spark session. """
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()

# Logging Utilities
def get_logger(name):
    """ Get a logger for logging information. """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger

# Metrics and Monitoring Functions
def timeit(method):
    """ Decorator to measure the execution time of a method. """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print(f'{method.__name__} took: {te - ts} sec')
        return result
    return timed

# Data Quality Checks
def check_duplicates(df):
    """ Check for duplicate rows in a DataFrame. """
    return df.count() > df.distinct().count()

# Schema Validation
def validate_schema(df, expected_schema):
    """ Validate the schema of a DataFrame against an expected schema. """
    return df.schema == expected_schema

# Date and Time Utilities
def parse_date(df, date_column):
    """ Parse a date column in a DataFrame to a standard format. """
    return df.withColumn(date_column, F.to_date(df[date_column]))

# Data Aggregation Helpers
def custom_sum(df, column_name):
    """ Custom sum aggregation for a DataFrame. """
    return df.agg(F.sum(column_name))

# Error Handling and Exceptions
class DataValidationError(Exception):
    """ Custom Exception for data validation errors. """
    pass

def validate_data(df, condition):
    """ Validate data based on a condition, raise an error if the validation fails. """
    if not condition:
        raise DataValidationError("Data validation failed")

# Testing Utilities
def assert_dataframe_equals(df1, df2):
    """ Asserts that two DataFrames are equal. """
    return df1.subtract(df2).count() == 0 and df2.subtract(df1).count() == 0

# Configuration and Parameter Management
def load_config(config_file):
    """ Load configuration from a JSON file. """
    with open(config_file, 'r') as file:
        return json.load(file)


