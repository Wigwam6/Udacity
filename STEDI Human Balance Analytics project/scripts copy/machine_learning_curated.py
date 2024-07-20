import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node AWS Glue Data Catalog-accelerometer_trusted
AWSGlueDataCatalogaccelerometer_trusted_node1715282869017 = glueContext.create_dynamic_frame.from_catalog(database="mpetr", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalogaccelerometer_trusted_node1715282869017")

# Script generated for node AWS Glue Data Catalog-step_trainer_trusted
AWSGlueDataCatalogstep_trainer_trusted_node1715282869531 = glueContext.create_dynamic_frame.from_catalog(database="mpetr", table_name="step_trainer_trusted_2", transformation_ctx="AWSGlueDataCatalogstep_trainer_trusted_node1715282869531")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct * 

from step
INNER join accel ON step.sensorreadingtime = accel.timestamp

'''
SQLQuery_node1715282975018 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accel":AWSGlueDataCatalogaccelerometer_trusted_node1715282869017, "step":AWSGlueDataCatalogstep_trainer_trusted_node1715282869531}, transformation_ctx = "SQLQuery_node1715282975018")

# Script generated for node SQL Query - remove PII data
SqlQuery1 = '''
select 
serialnumber, 
sensorreadingtime,
distancefromobject,
x,
y,
z,
timestamp


from myDataSource
'''
SQLQueryremovePIIdata_node1715283155579 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":SQLQuery_node1715282975018}, transformation_ctx = "SQLQueryremovePIIdata_node1715283155579")

# Script generated for node machine_learning_curated
machine_learning_curated_node1715283334359 = glueContext.getSink(path="s3://mpetrus-bucket/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1715283334359")
machine_learning_curated_node1715283334359.setCatalogInfo(catalogDatabase="mpetr",catalogTableName="machine_learning_curated")
machine_learning_curated_node1715283334359.setFormat("json")
machine_learning_curated_node1715283334359.writeFrame(SQLQueryremovePIIdata_node1715283155579)
job.commit()
