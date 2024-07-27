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

# Script generated for node Customer_trusted
Customer_trusted_node1722045260406 = glueContext.create_dynamic_frame.from_catalog(database="project_database", table_name="customer_trusted_glue_table", transformation_ctx="Customer_trusted_node1722045260406")

# Script generated for node Accelerometer_landing
Accelerometer_landing_node1722045261112 = glueContext.create_dynamic_frame.from_catalog(database="project_database", table_name="accelerometer_landing", transformation_ctx="Accelerometer_landing_node1722045261112")

# Script generated for node SQL Query
SqlQuery490 = '''
select a.user, a.timestamp, a.x, a.y, a.z from accerlerometer_landing a inner join customer_trusted c 
on a.user = c.email

'''
SQLQuery_node1722045264147 = sparkSqlQuery(glueContext, query = SqlQuery490, mapping = {"accerlerometer_landing":Accelerometer_landing_node1722045261112, "customer_trusted":Customer_trusted_node1722045260406}, transformation_ctx = "SQLQuery_node1722045264147")

# Script generated for node Amazon S3
AmazonS3_node1722045266527 = glueContext.getSink(path="s3://aws-glue-project-vasu/accerlerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1722045266527")
AmazonS3_node1722045266527.setCatalogInfo(catalogDatabase="project_database",catalogTableName="accelerometer_trusted")
AmazonS3_node1722045266527.setFormat("json")
AmazonS3_node1722045266527.writeFrame(SQLQuery_node1722045264147)
job.commit()