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
customer_trusted_node1779699481671 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="customer_trusted", transformation_ctx="customer_trusted_node1779699481671")

# Script generated for node accelerometer_landing
accelerometer_landing_node1779699440687 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1779699440687")

# Script generated for node SQL Query
SqlQuery703 = '''
select a.*
from accelerometer_landing a 
inner join customer_trusted c
on a.user = c.email
'''
SQLQuery_node1779699504142 = sparkSqlQuery(glueContext, query = SqlQuery703, mapping = {"accelerometer_landing":accelerometer_landing_node1779699440687, "customer_trusted":customer_trusted_node1779699481671}, transformation_ctx = "SQLQuery_node1779699504142")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779699504142, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779697267359", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779699595374 = glueContext.getSink(path="s3://stedi-human-ivleen/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779699595374")
AmazonS3_node1779699595374.setCatalogInfo(catalogDatabase="stedi-human-balance",catalogTableName="accelerometer_trusted")
AmazonS3_node1779699595374.setFormat("json")
AmazonS3_node1779699595374.writeFrame(SQLQuery_node1779699504142)
job.commit()