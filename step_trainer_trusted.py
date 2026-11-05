import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Amazon S3
AmazonS3_node1769611279837 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://woods1/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1769611279837")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1769611250257 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1769611250257")

# Script generated for node Drop Duplicates
DropDuplicates_node1769611351838 =  DynamicFrame.fromDF(AWSGlueDataCatalog_node1769611250257.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1769611351838")

# Script generated for node Rename Field
RenameField_node1769611380244 = RenameField.apply(frame=DropDuplicates_node1769611351838, old_name="serialnumber", new_name="Customserialnumber", transformation_ctx="RenameField_node1769611380244")

# Script generated for node SQL Query
SqlQuery1038 = '''
SELECT *
FROM mydatasource AS m
JOIN sql2 AS s
    ON m.serialNumber = s.serialNumber
'''
SQLQuery_node1769612598025 = sparkSqlQuery(glueContext, query = SqlQuery1038, mapping = {"myDataSource":AmazonS3_node1769611279837, "sql2":RenameField_node1769611380244}, transformation_ctx = "SQLQuery_node1769612598025")

# Script generated for node Drop Fields
DropFields_node1769611912039 = DropFields.apply(frame=SQLQuery_node1769612598025, paths=["phone", "lastUpdateDate", "email", "shareWithFriendsAsOfDate", "customerName", "registrationDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "birthDay"], transformation_ctx="DropFields_node1769611912039")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1769611912039, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769611821619", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1769611936861 = glueContext.getSink(path="s3://woods1/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1769611936861")
AmazonS3_node1769611936861.setCatalogInfo(catalogDatabase="d609",catalogTableName="step_trainer_trusted")
AmazonS3_node1769611936861.setFormat("json")
AmazonS3_node1769611936861.writeFrame(DropFields_node1769611912039)
job.commit()