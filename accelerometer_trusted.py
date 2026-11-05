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
AWSGlueDataCatalog_node1769564625196 = glueContext.create_dynamic_frame.from_catalog(database="d609", table_name="customer_trusted ", transformation_ctx="AWSGlueDataCatalog_node1769564625196")

# Script generated for node Amazon S3
AmazonS3_node1769564675664 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://woods1/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1769564675664")

# Script generated for node Join
Join_node1769564723198 = Join.apply(frame1=AmazonS3_node1769564675664, frame2=AWSGlueDataCatalog_node1769564625196, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1769564723198")

# Script generated for node SQL Query
SqlQuery889 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1769564805591 = sparkSqlQuery(glueContext, query = SqlQuery889, mapping = {"myDataSource":Join_node1769564723198}, transformation_ctx = "SQLQuery_node1769564805591")

# Script generated for node Drop Fields
DropFields_node1769564984260 = DropFields.apply(frame=SQLQuery_node1769564805591, paths=["customername", "email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate", "serialNumber", "birthDay", "shareWithPublicAsOfDate", "shareWithResearchAsOfDate", "registrationDate", "customerName", "shareWithFriendsAsOfDate", "lastUpdateDate"], transformation_ctx="DropFields_node1769564984260")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1769564984260, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769564548392", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1769564892655 = glueContext.getSink(path="s3://woods1/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1769564892655")
AmazonS3_node1769564892655.setCatalogInfo(catalogDatabase="d609",catalogTableName="accelerometer_trusted")
AmazonS3_node1769564892655.setFormat("json")
AmazonS3_node1769564892655.writeFrame(DropFields_node1769564984260)
job.commit()