import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession

# Get arguments (AWS Glue passes parameters)
args = getResolvedOptions(sys.argv, ["JOB_NAME", "RDS_HOST", "RDS_USER", "RDS_PASSWORD", "S3_BUCKET", "RDS_DB"])

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Extract parameters
s3_client = boto3.client("s3")
RDS_HOST = args["RDS_HOST"]
RDS_USER = args["RDS_USER"]
RDS_PASSWORD = args["RDS_PASSWORD"]
RDS_DB = args["RDS_DB"]
S3_BUCKET = args["S3_BUCKET"]

# RDS connection properties
# jdbc_url = f"jdbc:mysql://{RDS_HOST}:3306/{RDS_DB}"
jdbc_url = f"jdbc:{RDS_HOST}:3306/{RDS_DB}?SSL=false"

# Function to extract data and load to S3
def fetch_and_upload_s3(table_name):
    """
    Extracts data from a specified MySQL table and uploads it to an S3 bucket as a CSV file.

    Args:
        table_name (str): The name of the table to process.

    This function connects to a MySQL database using AWS Glue, extracts data from the specified
    table, converts it to a Spark DataFrame, and writes it to the specified S3 bucket in CSV format.
    """

    print(f"Processing table: {table_name}")

    connection_options = {
    "url": jdbc_url,
    # "dbtable": "",
    "user": RDS_USER,
    "password": RDS_PASSWORD,
    "customJdbcDriverS3Path": "s3://aws-glue-jdbc-drivers/mysql/mysql-connector-java-8.0.23.jar",
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"
    }

    connection_options["dbtable"] = table_name
    
    # Read from MySQL using Glue
    data_source = glueContext.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options=connection_options
    )

    # if data_source.count() > 0:
    #     print("Successfully connected to RDS!")
    # else:
    #     print("Failed to connect to RDS. Check credentials or networking.")
    #     print(f"Schema for {table_name}: {data_source.schema()}")

    # Check if data exists before proceeding
    if data_source.count() == 0:
        print(f"Skipping {table_name} - No data found.")
        return

    print(f"Successfully fetched {data_source.count()} rows from {table_name}")
    
    # # Convert to DataFrame and Save as CSV
    # df = data_source.toDF()

    # # Check if DataFrame is empty
    # if df.rdd.isEmpty():
    #     print(f"Skipping {table_name} - No data found.")
    #     return

    # df.write.mode("overwrite").csv(f"s3://{S3_BUCKET}/raw-data/{table_name}/", header=True)

    s3_path = f's3://{S3_BUCKET}/raw-data/{table_name}/'
    glueContext.write_dynamic_frame.from_options(
        frame=data_source,
        connection_type='s3',
        connection_options={'path': s3_path, 'overwrite': True, 'header': True},
        format='csv'
    )
    
    print(f"{table_name} uploaded to S3")

# Extract all tables
try:
    tables = ["apartment_attributes", "apartments", "bookings", "user_viewing"]
    for table in tables:
        # s3_prefix = f"raw-data/{table}/"  # Defining the folder to delete
        # delete_s3_folder(s3_prefix)
        fetch_and_upload_s3(table)

except Exception as e:
    print(f"An error occurred: {str(e)}")

print("Glue Job Completed Successfully! Data written to S3.")

# Commit Job
job.commit()
