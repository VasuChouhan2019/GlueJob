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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1722047276931 = glueContext.create_dynamic_frame.from_catalog(database="project_database", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1722047276931")

# Script generated for node Customer_trusted
Customer_trusted_node1722047277778 = glueContext.create_dynamic_frame.from_catalog(database="project_database", table_name="customer_trusted_glue_table", transformation_ctx="Customer_trusted_node1722047277778")

# Script generated for node SQL Query
SqlQuery461 = '''
select distinct c.customername, c.email, c.phone, c.birthday, c.serialnumber, c.registrationdate, c.lastupdatedate, c.sharewithresearchasofdate, c.sharewithpublicasofdate, c.sharewithfriendsasofdate from customer_trusted c inner join accelerometer_trusted a 
on c.email= a.user
where c.shareWithResearchAsOfDate is not null

'''
SQLQuery_node1722047318383 = sparkSqlQuery(glueContext, query = SqlQuery461, mapping = {"accelerometer_trusted":accelerometer_trusted_node1722047276931, "customer_trusted":Customer_trusted_node1722047277778}, transformation_ctx = "SQLQuery_node1722047318383")

# Script generated for node Amazon S3
AmazonS3_node1722047372772 = glueContext.getSink(path="s3://aws-glue-project-vasu/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1722047372772")
AmazonS3_node1722047372772.setCatalogInfo(catalogDatabase="project_database",catalogTableName="customer_curated")
AmazonS3_node1722047372772.setFormat("json")
AmazonS3_node1722047372772.writeFrame(SQLQuery_node1722047318383)
job.commit()