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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1715117534395 = glueContext.create_dynamic_frame.from_catalog(database="mpetr", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1715117534395")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT *
FROM myDataSource
WHERE 
sharewithresearchasofdate != 0;
'''
SQLQuery_node1714985309120 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node1715117534395}, transformation_ctx = "SQLQuery_node1714985309120")

# Script generated for node customer_trusted
customer_trusted_node1714940381378 = glueContext.getSink(path="s3://mpetrus-bucket/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1714940381378")
customer_trusted_node1714940381378.setCatalogInfo(catalogDatabase="mpetr",catalogTableName="customer_trusted")
customer_trusted_node1714940381378.setFormat("json")
customer_trusted_node1714940381378.writeFrame(SQLQuery_node1714985309120)
job.commit()
