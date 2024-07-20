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

# Script generated for node AWS Glue Data Catalog-customer_curated
AWSGlueDataCatalogcustomer_curated_node1715201896318 = glueContext.create_dynamic_frame.from_catalog(database="mpetr", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalogcustomer_curated_node1715201896318")

# Script generated for node AWS Glue Data Catalog-step_trainer_landing
AWSGlueDataCatalogstep_trainer_landing_node1715201896803 = glueContext.create_dynamic_frame.from_catalog(database="mpetr", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalogstep_trainer_landing_node1715201896803")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct 
step.serialnumber,
step.sensorreadingtime,
step.distancefromobject

from step
INNER join  cust ON cust.serialnumber = step.serialnumber
'''
SQLQuery_node1715201935766 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step":AWSGlueDataCatalogstep_trainer_landing_node1715201896803, "cust":AWSGlueDataCatalogcustomer_curated_node1715201896318}, transformation_ctx = "SQLQuery_node1715201935766")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1715202142594 = glueContext.getSink(path="s3://mpetrus-bucket/step_trainer/trusted2/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1715202142594")
step_trainer_trusted_node1715202142594.setCatalogInfo(catalogDatabase="mpetr",catalogTableName="step_trainer_trusted_2")
step_trainer_trusted_node1715202142594.setFormat("json")
step_trainer_trusted_node1715202142594.writeFrame(SQLQuery_node1715201935766)
job.commit()
