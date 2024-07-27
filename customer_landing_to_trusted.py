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
AWSGlueDataCatalog_node1722043353545 = glueContext.create_dynamic_frame.from_catalog(database="project_database", table_name="customer_landing_glue_table", transformation_ctx="AWSGlueDataCatalog_node1722043353545")

# Script generated for node SQL Query
SqlQuery521 = '''
select * from myDataSource
where shareWithResearchAsOfDate != 0 
'''
SQLQuery_node1722043369560 = sparkSqlQuery(glueContext, query = SqlQuery521, mapping = {"myDataSource":AWSGlueDataCatalog_node1722043353545}, transformation_ctx = "SQLQuery_node1722043369560")

# Script generated for node Amazon S3
AmazonS3_node1722043438113 = glueContext.getSink(path="s3://aws-glue-project-vasu/customer/trsuted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1722043438113")
AmazonS3_node1722043438113.setCatalogInfo(catalogDatabase="project_database",catalogTableName="customer_trusted_glue_table")
AmazonS3_node1722043438113.setFormat("json")
AmazonS3_node1722043438113.writeFrame(SQLQuery_node1722043369560)
job.commit()