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

# Script generated for node AWS Glue Data Catalog-accelerometer_landing
AWSGlueDataCatalogaccelerometer_landing_node1715117978275 = glueContext.create_dynamic_frame.from_catalog(database="mpetr", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalogaccelerometer_landing_node1715117978275")

# Script generated for node AWS Glue Data Catalog-customer_trusted
AWSGlueDataCatalogcustomer_trusted_node1715117982879 = glueContext.create_dynamic_frame.from_catalog(database="mpetr", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalogcustomer_trusted_node1715117982879")

# Script generated for node Join
Join_node1715118007817 = Join.apply(frame1=AWSGlueDataCatalogaccelerometer_landing_node1715117978275, frame2=AWSGlueDataCatalogcustomer_trusted_node1715117982879, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1715118007817")

# Script generated for node SQL Query
SqlQuery0 = '''
select x,y,z,user,timestamp from myDataSource;
'''
SQLQuery_node1715119264186 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1715118007817}, transformation_ctx = "SQLQuery_node1715119264186")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1715118370405 = glueContext.getSink(path="s3://mpetrus-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1715118370405")
accelerometer_trusted_node1715118370405.setCatalogInfo(catalogDatabase="mpetr",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1715118370405.setFormat("json")
accelerometer_trusted_node1715118370405.writeFrame(SQLQuery_node1715119264186)
job.commit()
