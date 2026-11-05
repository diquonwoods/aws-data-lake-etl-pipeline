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


# Load trusted Step Trainer data
step_trainer_trusted = glue_context.create_dynamic_frame.from_catalog(
    database="d609",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted"
)


# Load trusted accelerometer data
accelerometer_trusted = glue_context.create_dynamic_frame.from_catalog(
    database="d609",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted"
)


# Join sensor datasets using their corresponding reading timestamps
machine_learning_query = """
SELECT *
FROM step_trainer AS st
INNER JOIN accelerometer AS a
    ON st.sensorReadingTime = a.timestamp
"""

machine_learning_curated = spark_sql_query(
    glue_context,
    query=machine_learning_query,
    mapping={
        "step_trainer": step_trainer_trusted,
        "accelerometer": accelerometer_trusted
    },
    transformation_ctx="machine_learning_curated"
)


# Validate the curated machine-learning dataset
EvaluateDataQuality().process_rows(
    frame=machine_learning_curated,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "machine_learning_curated_quality",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)


# Write ML-ready dataset to the S3 curated layer
curated_sink = glue_context.getSink(
    path="s3://your-data-lake-bucket/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_sink"
)

curated_sink.setCatalogInfo(
    catalogDatabase="d609",
    catalogTableName="machine_learning_curated"
)

curated_sink.setFormat("json")
curated_sink.writeFrame(machine_learning_curated)

job.commit()