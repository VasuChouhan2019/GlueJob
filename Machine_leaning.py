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

# Script generated for node Step_trainer_trusted
Step_trainer_trusted_node1722050824320 = glueContext.create_dynamic_frame.from_catalog(database="project_database", table_name="step_trainer_trusted", transformation_ctx="Step_trainer_trusted_node1722050824320")

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1722050826122 = glueContext.create_dynamic_frame.from_catalog(database="project_database", table_name="accelerometer_trusted", transformation_ctx="Accelerometer_trusted_node1722050826122")

# Script generated for node SQL Query
SqlQuery450 = '''
select * from step_trainer_trusted s join accelerometer_trusted a
on s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1722050882931 = sparkSqlQuery(glueContext, query = SqlQuery450, mapping = {"accelerometer_trusted":Accelerometer_trusted_node1722050826122, "step_trainer_trusted":Step_trainer_trusted_node1722050824320}, transformation_ctx = "SQLQuery_node1722050882931")

# Script generated for node Machine_learning_curated
Machine_learning_curated_node1722050923366 = glueContext.getSink(path="s3://aws-glue-project-vasu/machine_learning2/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Machine_learning_curated_node1722050923366")
Machine_learning_curated_node1722050923366.setCatalogInfo(catalogDatabase="project_database",catalogTableName="machine_learning")
Machine_learning_curated_node1722050923366.setFormat("json")
Machine_learning_curated_node1722050923366.writeFrame(SQLQuery_node1722050882931)
job.commit()