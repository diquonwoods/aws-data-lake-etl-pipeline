import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1769614036676 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1769614036676")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1769614012449 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1769614012449")

# Script generated for node SQL Query
SqlQuery1145 = '''
select * from myDataSource2 as a
inner join myDataSource as s 
 on a.sensorReadingTime = s.timestamp

'''
SQLQuery_node1769614063650 = sparkSqlQuery(glueContext, query = SqlQuery1145, mapping = {"myDataSource":AWSGlueDataCatalog_node1769614012449, "MyDataSource2":AWSGlueDataCatalog_node1769614036676}, transformation_ctx = "SQLQuery_node1769614063650")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769614063650, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769611821619", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1769614288546 = glueContext.getSink(path="s3://woods1/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1769614288546")
AmazonS3_node1769614288546.setCatalogInfo(catalogDatabase="d609",catalogTableName="machine_learning_curated")
AmazonS3_node1769614288546.setFormat("json")
AmazonS3_node1769614288546.writeFrame(SQLQuery_node1769614063650)
job.commit()