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

# Script generated for node customer_trusted
customer_trusted_node1779700036014 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="customer_trusted", transformation_ctx="customer_trusted_node1779700036014")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1779700059885 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1779700059885")

# Script generated for node SQL Query
SqlQuery744 = '''
select distinct c.*
from customer_trusted c
inner join accelerometer_trusted a 
on c.email = a.user
'''
SQLQuery_node1779700088985 = sparkSqlQuery(glueContext, query = SqlQuery744, mapping = {"customer_trusted":customer_trusted_node1779700036014, "accelerometer_trusted":accelerometer_trusted_node1779700059885}, transformation_ctx = "SQLQuery_node1779700088985")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779700088985, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779697267359", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779700216223 = glueContext.getSink(path="s3://stedi-human-ivleen/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779700216223")
AmazonS3_node1779700216223.setCatalogInfo(catalogDatabase="stedi-human-balance",catalogTableName="customer_curated")
AmazonS3_node1779700216223.setFormat("json")
AmazonS3_node1779700216223.writeFrame(SQLQuery_node1779700088985)
job.commit()