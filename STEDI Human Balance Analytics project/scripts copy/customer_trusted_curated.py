import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node AWS Glue Data Catalog-accelerometer_landing
AWSGlueDataCatalogaccelerometer_landing_node1715163209175 = glueContext.create_dynamic_frame.from_catalog(database="mpetr", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalogaccelerometer_landing_node1715163209175")

# Script generated for node AWS Glue Data Catalog-customer_trusted
AWSGlueDataCatalogcustomer_trusted_node1715163206548 = glueContext.create_dynamic_frame.from_catalog(database="mpetr", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalogcustomer_trusted_node1715163206548")

# Script generated for node Join
Join_node1715163950051 = Join.apply(frame1=AWSGlueDataCatalogaccelerometer_landing_node1715163209175, frame2=AWSGlueDataCatalogcustomer_trusted_node1715163206548, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1715163950051")

# Script generated for node SQL Query
SqlQuery0 = '''
select  
        serialNumber,  
        birthDay,
        shareWithPublicAsOfDate,
        shareWithResearchAsOfDate,
        registrationDate,
        customerName,
        shareWithFriendsAsOfDate,
        email,
        lastUpdateDate,
        phone
        
 from myDataSource
'''
SQLQuery_node1715164219783 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1715163950051}, transformation_ctx = "SQLQuery_node1715164219783")

# Script generated for node Drop Duplicates
DropDuplicates_node1715164587686 =  DynamicFrame.fromDF(SQLQuery_node1715164219783.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1715164587686")

# Script generated for node customer_curated
customer_curated_node1715164625057 = glueContext.getSink(path="s3://mpetrus-bucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1715164625057")
customer_curated_node1715164625057.setCatalogInfo(catalogDatabase="mpetr",catalogTableName="customer_curated")
customer_curated_node1715164625057.setFormat("json")
customer_curated_node1715164625057.writeFrame(DropDuplicates_node1715164587686)
job.commit()
