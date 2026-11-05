import sys

from awsglue.transforms import RenameField, DropFields
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame


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


# Load raw Step Trainer data from the S3 landing layer
step_trainer_landing = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={"multiLine": "false"},
    connection_options={
        "paths": [
            "s3://your-data-lake-bucket/step_trainer/landing/"
        ],
        "recurse": True
    },
    transformation_ctx="step_trainer_landing"
)


# Load curated customer data
customer_curated = glue_context.create_dynamic_frame.from_catalog(
    database="d609",
    table_name="customer_curated",
    transformation_ctx="customer_curated"
)


# Remove duplicate customer records
customer_deduplicated = DynamicFrame.fromDF(
    customer_curated.toDF().dropDuplicates(),
    glue_context,
    "customer_deduplicated"
)


# Rename customer serial number to avoid ambiguity during the join
customer_deduplicated = RenameField.apply(
    frame=customer_deduplicated,
    old_name="serialnumber",
    new_name="customerSerialNumber",
    transformation_ctx="rename_customer_serial_number"
)


# Match Step Trainer devices to curated customers
step_trainer_query = """
SELECT *
FROM step_trainer AS st
INNER JOIN customers AS c
    ON st.serialNumber = c.customerSerialNumber
"""

step_trainer_customer_join = spark_sql_query(
    glue_context,
    query=step_trainer_query,
    mapping={
        "step_trainer": step_trainer_landing,
        "customers": customer_deduplicated
    },
    transformation_ctx="step_trainer_customer_join"
)


# Remove customer attributes that are not needed in the trusted sensor layer
step_trainer_trusted = DropFields.apply(
    frame=step_trainer_customer_join,
    paths=[
        "phone",
        "lastUpdateDate",
        "email",
        "shareWithFriendsAsOfDate",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "birthDay"
    ],
    transformation_ctx="step_trainer_trusted"
)


# Validate the trusted Step Trainer dataset
EvaluateDataQuality().process_rows(
    frame=step_trainer_trusted,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "step_trainer_trusted_quality",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)


# Write trusted Step Trainer data to Amazon S3
trusted_sink = glue_context.getSink(
    path="s3://your-data-lake-bucket/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_sink"
)

trusted_sink.setCatalogInfo(
    catalogDatabase="d609",
    catalogTableName="step_trainer_trusted"
)

trusted_sink.setFormat("json")
trusted_sink.writeFrame(step_trainer_trusted)

job.commit()