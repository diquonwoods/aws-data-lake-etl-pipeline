import sys

from awsglue.transforms import Join, DropFields
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame


# Initialize AWS Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# Basic data quality validation
DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""


# Load trusted accelerometer data
accelerometer_trusted = glue_context.create_dynamic_frame.from_catalog(
    database="d609",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted"
)


# Load trusted customer data
customer_trusted = glue_context.create_dynamic_frame.from_catalog(
    database="d609",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)


# Join trusted customers with accelerometer records
customer_accelerometer_join = Join.apply(
    frame1=customer_trusted,
    frame2=accelerometer_trusted,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="customer_accelerometer_join"
)


# Remove accelerometer fields from the customer dataset
customer_records = DropFields.apply(
    frame=customer_accelerometer_join,
    paths=[
        "z",
        "user",
        "y",
        "x",
        "timestamp"
    ],
    transformation_ctx="customer_records"
)


# Remove duplicate customer records created by the sensor-data join
customer_curated = DynamicFrame.fromDF(
    customer_records.toDF().dropDuplicates(),
    glue_context,
    "customer_curated"
)


# Validate the curated customer dataset
EvaluateDataQuality().process_rows(
    frame=customer_curated,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "customer_curated_quality",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)


# Write curated customer data to Amazon S3
curated_sink = glue_context.getSink(
    path="s3://your-data-lake-bucket/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_sink"
)

curated_sink.setCatalogInfo(
    catalogDatabase="d609",
    catalogTableName="customer_curated"
)

curated_sink.setFormat("json")
curated_sink.writeFrame(customer_curated)

job.commit()