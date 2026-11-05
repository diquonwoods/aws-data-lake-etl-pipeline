import sys

from awsglue.transforms import Join, DropFields
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame


def spark_sql_query(glue_context, query, mapping, transformation_ctx):
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)

    result = spark.sql(query)

    return DynamicFrame.fromDF(
        result,
        glue_context,
        transformation_ctx
    )


# Initialize AWS Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# Basic data quality validation
DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""


# Load trusted customer data from the AWS Glue Data Catalog
customer_trusted = glue_context.create_dynamic_frame.from_catalog(
    database="d609",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)


# Load raw accelerometer data from the S3 landing layer
accelerometer_landing = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={"multiLine": "false"},
    connection_options={
        "paths": [
            "s3://your-data-lake-bucket/accelerometer/landing/"
        ],
        "recurse": True
    },
    transformation_ctx="accelerometer_landing"
)


# Join accelerometer records to trusted customers
accelerometer_customer_join = Join.apply(
    frame1=accelerometer_landing,
    frame2=customer_trusted,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="accelerometer_customer_join"
)


# Keep accelerometer records for customers who consented to research
research_records_query = """
SELECT *
FROM joined_data
WHERE shareWithResearchAsOfDate IS NOT NULL
"""

research_records = spark_sql_query(
    glue_context,
    query=research_records_query,
    mapping={
        "joined_data": accelerometer_customer_join
    },
    transformation_ctx="research_records"
)


# Remove customer attributes from the trusted sensor dataset
accelerometer_trusted = DropFields.apply(
    frame=research_records,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate"
    ],
    transformation_ctx="accelerometer_trusted"
)


# Validate the trusted accelerometer dataset
EvaluateDataQuality().process_rows(
    frame=accelerometer_trusted,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "accelerometer_trusted_quality",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)


# Write trusted accelerometer data to Amazon S3
trusted_sink = glue_context.getSink(
    path="s3://your-data-lake-bucket/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="trusted_accelerometer_sink"
)

trusted_sink.setCatalogInfo(
    catalogDatabase="d609",
    catalogTableName="accelerometer_trusted"
)

trusted_sink.setFormat("json")
trusted_sink.writeFrame(accelerometer_trusted)

job.commit()