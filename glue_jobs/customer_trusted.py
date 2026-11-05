import sys

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


# Load raw customer data from the S3 landing layer
customer_landing = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={"multiLine": "false"},
    connection_options={
        "paths": [
            "s3://your-data-lake-bucket/customer/landing/"
        ],
        "recurse": True
    },
    transformation_ctx="customer_landing"
)


# Keep customers who consented to sharing data for research
research_consent_query = """
SELECT *
FROM customer_data
WHERE sharewithresearchasofdate IS NOT NULL
"""

customer_trusted = spark_sql_query(
    glue_context,
    query=research_consent_query,
    mapping={
        "customer_data": customer_landing
    },
    transformation_ctx="customer_trusted"
)


# Validate the trusted customer dataset
EvaluateDataQuality().process_rows(
    frame=customer_trusted,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "customer_trusted_quality",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)


# Write trusted customer data to Amazon S3
trusted_sink = glue_context.getSink(
    path="s3://your-data-lake-bucket/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_sink"
)

trusted_sink.setCatalogInfo(
    catalogDatabase="d609",
    catalogTableName="customer_trusted"
)

trusted_sink.setFormat("json")
trusted_sink.writeFrame(customer_trusted)

job.commit()