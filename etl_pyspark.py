#########################################
### IMPORT LIBRARIES AND SET VARIABLES
#########################################

# taken from IS459 movie-aggregate-csv.py

# Import Python modules
import sys
from datetime import datetime

# Import pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f

# Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Parameters
glue_db = "yelp_database"
glue_tbl = "read" # data catalog table
s3_write_path = "s3://is459-assignment2-bda/write/"


#########################################
### EXTRACT (READ DATA)
#########################################
dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(
    database = glue_db,
    table_name = glue_tbl
)

# Convert dynamic frame to data frame to use standard pyspark functions
data_frame = dynamic_frame_read.toDF()


#########################################
### TRANSFORM (MODIFY DATA)
#########################################
data_frame_aggregated = data_frame.groupBy("year").agg(
    f.count('rating').alias('num_ratings'),
    f.mean('rating').alias('avg_rating')
)

data_frame_aggregated = data_frame_aggregated.orderBy(
    f.desc('year')
)


#########################################
### LOAD (WRITE DATA)
#########################################

# Create just 1 partition, because the output data is very small
data_frame_aggregated = data_frame_aggregated.repartition(1)

# Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(
    data_frame_aggregated,
    glue_context,
    "dynamic_frame_write"
)

# Write data back to S3
glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path
    },
    format = "csv"
)

job.commit()