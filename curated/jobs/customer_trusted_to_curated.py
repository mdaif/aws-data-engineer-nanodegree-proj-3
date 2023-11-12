import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1699793285313 = glueContext.create_dynamic_frame.from_catalog(
    database="nanodegree",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1699793285313",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699793311270 = glueContext.create_dynamic_frame.from_catalog(
    database="nanodegree",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1699793311270",
)

# Script generated for node Join
Join_node1699793371733 = Join.apply(
    frame1=CustomerTrusted_node1699793311270,
    frame2=AccelerometerLanding_node1699793285313,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1699793371733",
)

# Script generated for node Drop Fields
DropFields_node1699793760590 = DropFields.apply(
    frame=Join_node1699793371733,
    paths=["z", "y", "x", "user", "timestamp"],
    transformation_ctx="DropFields_node1699793760590",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1699801587508 = DynamicFrame.fromDF(
    DropFields_node1699793760590.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1699801587508",
)

# Script generated for node Amazon S3
AmazonS3_node1699793848209 = glueContext.getSink(
    path="s3://daifs-nanodegree-bucket/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1699793848209",
)
AmazonS3_node1699793848209.setCatalogInfo(
    catalogDatabase="nanodegree", catalogTableName="customer_curated"
)
AmazonS3_node1699793848209.setFormat("json")
AmazonS3_node1699793848209.writeFrame(DropDuplicates_node1699801587508)
job.commit()
