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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1779701149589 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1779701149589")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1779701100263 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1779701100263")

# Script generated for node SQL Query
SqlQuery786 = '''
select 
a.user,
a.timestamp,
a.x,
a.y,
a.z,
t.sensorreadingtime,
t.serialnumber,
t.distancefromobject
from accelerometer_trusted a 
inner join step_trainer_trusted t 
on a.timestamp = t.sensorreadingtime
'''
SQLQuery_node1779701171877 = sparkSqlQuery(glueContext, query = SqlQuery786, mapping = {"step_trainer_trusted":step_trainer_trusted_node1779701149589, "accelerometer_trusted":accelerometer_trusted_node1779701100263}, transformation_ctx = "SQLQuery_node1779701171877")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779701171877, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779697267359", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779701363814 = glueContext.getSink(path="s3://stedi-human-ivleen/step_trainer/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779701363814")
AmazonS3_node1779701363814.setCatalogInfo(catalogDatabase="stedi-human-balance",catalogTableName="machine_learning_curated")
AmazonS3_node1779701363814.setFormat("json")
AmazonS3_node1779701363814.writeFrame(SQLQuery_node1779701171877)
job.commit()