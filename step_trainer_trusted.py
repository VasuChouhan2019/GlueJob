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

# Script generated for node Step_trainer_Landing
Step_trainer_Landing_node1722049661687 = glueContext.create_dynamic_frame.from_catalog(database="project_database", table_name="step_trainer", transformation_ctx="Step_trainer_Landing_node1722049661687")

# Script generated for node Customer_curated
Customer_curated_node1722049691757 = glueContext.create_dynamic_frame.from_catalog(database="project_database", table_name="customer_curated", transformation_ctx="Customer_curated_node1722049691757")

# Script generated for node SQL Query
SqlQuery579 = '''
select s.sensorreadingtime, s.serialnumber, s.distancefromobject from step_trainer s  inner join customer_curated c 
on s.serialnumber = c.serialnumber
'''
SQLQuery_node1722049715682 = sparkSqlQuery(glueContext, query = SqlQuery579, mapping = {"step_trainer":Step_trainer_Landing_node1722049661687, "customer_curated":Customer_curated_node1722049691757}, transformation_ctx = "SQLQuery_node1722049715682")

# Script generated for node Amazon S3
AmazonS3_node1722050208221 = glueContext.getSink(path="s3://aws-glue-project-vasu/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1722050208221")
AmazonS3_node1722050208221.setCatalogInfo(catalogDatabase="project_database",catalogTableName="step_trainer_trusted")
AmazonS3_node1722050208221.setFormat("json")
AmazonS3_node1722050208221.writeFrame(SQLQuery_node1722049715682)
job.commit()