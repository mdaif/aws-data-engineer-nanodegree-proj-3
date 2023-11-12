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

# Script generated for node Customer Curated
CustomerCurated_node1699795403143 = glueContext.create_dynamic_frame.from_catalog(
    database="nanodegree",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1699795403143",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1699795376279 = glueContext.create_dynamic_frame.from_catalog(
    database="nanodegree",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1699795376279",
)

# Script generated for node SQL Query
SqlQuery0 = """
select DISTINCT
    step_trainer_landing.sensorReadingTime,
    step_trainer_landing.serialNumber,
    step_trainer_landing.distanceFromObject
from
    step_trainer_landing
join customer_curated
    ON step_trainer_landing.serialNumber = customer_curated.serialNumber
"""
SQLQuery_node1699805395938 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_curated": CustomerCurated_node1699795403143,
        "step_trainer_landing": StepTrainerLanding_node1699795376279,
    },
    transformation_ctx="SQLQuery_node1699805395938",
)

# Script generated for node Amazon S3
AmazonS3_node1699795772339 = glueContext.getSink(
    path="s3://daifs-nanodegree-bucket/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1699795772339",
)
AmazonS3_node1699795772339.setCatalogInfo(
    catalogDatabase="nanodegree", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1699795772339.setFormat("json")
AmazonS3_node1699795772339.writeFrame(SQLQuery_node1699805395938)
job.commit()
