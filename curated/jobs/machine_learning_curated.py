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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1699809404369 = glueContext.create_dynamic_frame.from_catalog(
    database="nanodegree",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1699809404369",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1699809403396 = glueContext.create_dynamic_frame.from_catalog(
    database="nanodegree",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1699809403396",
)

# Script generated for node Customer Curated
CustomerCurated_node1699809403903 = glueContext.create_dynamic_frame.from_catalog(
    database="nanodegree",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1699809403903",
)

# Script generated for node SQL Query
SqlQuery0 = """
select distinct * 
from step_trainer_trusted 
    join customer_curated on step_trainer_trusted.serialnumber = customer_curated.serialnumber 
    join accelerometer_trusted on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
"""
SQLQuery_node1699810288535 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node1699809403396,
        "accelerometer_trusted": AccelerometerTrusted_node1699809404369,
        "customer_curated": CustomerCurated_node1699809403903,
    },
    transformation_ctx="SQLQuery_node1699810288535",
)

# Script generated for node Amazon S3
AmazonS3_node1699811135734 = glueContext.getSink(
    path="s3://daifs-nanodegree-bucket/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1699811135734",
)
AmazonS3_node1699811135734.setCatalogInfo(
    catalogDatabase="nanodegree", catalogTableName="machine_learning_curated"
)
AmazonS3_node1699811135734.setFormat("json")
AmazonS3_node1699811135734.writeFrame(SQLQuery_node1699810288535)
job.commit()
