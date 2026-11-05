import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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
AWSGlueDataCatalog_node1769572703889 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1769572703889")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1769572722189 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="customer_trusted ", transformation_ctx="AWSGlueDataCatalog_node1769572722189")

# Script generated for node Join
Join_node1769575153932 = Join.apply(frame1=AWSGlueDataCatalog_node1769572722189, frame2=AWSGlueDataCatalog_node1769572703889, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1769575153932")

# Script generated for node Drop Fields
DropFields_node1769572998441 = DropFields.apply(frame=Join_node1769575153932, paths=["z", "user", "y", "x", "timestamp"], transformation_ctx="DropFields_node1769572998441")

# Script generated for node Drop Duplicates
DropDuplicates_node1769575456408 =  DynamicFrame.fromDF(DropFields_node1769572998441.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1769575456408")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1769575456408, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769571637207", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1769573029011 = glueContext.getSink(path="s3://woods1/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1769573029011")
AmazonS3_node1769573029011.setCatalogInfo(catalogDatabase="d609",catalogTableName="customer_curated")
AmazonS3_node1769573029011.setFormat("json")
AmazonS3_node1769573029011.writeFrame(DropDuplicates_node1769575456408)
job.commit()