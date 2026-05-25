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
AWSGlueDataCatalog_node1779698856662 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1779698856662")

# Script generated for node SQL Query
SqlQuery767 = '''
select * from myDataSource
WHERE sharewithresearchasofdate IS NOT NULL
AND sharewithresearchasofdate <> ''
'''
SQLQuery_node1779698867110 = sparkSqlQuery(glueContext, query = SqlQuery767, mapping = {"myDataSource":AWSGlueDataCatalog_node1779698856662}, transformation_ctx = "SQLQuery_node1779698867110")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779698867110, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779697267359", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779698883784 = glueContext.getSink(path="s3://stedi-human-ivleen/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779698883784")
AmazonS3_node1779698883784.setCatalogInfo(catalogDatabase="stedi-human-balance",catalogTableName="customer_trusted")
AmazonS3_node1779698883784.setFormat("json")
AmazonS3_node1779698883784.writeFrame(SQLQuery_node1779698867110)
job.commit()