import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1699291474780 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://daifs-nanodegree-bucket/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1699291474780",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699291482188 = Filter.apply(
    frame=CustomerLanding_node1699291474780,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="CustomerTrusted_node1699291482188",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699291485544 = glueContext.getSink(
    path="s3://daifs-nanodegree-bucket/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1699291485544",
)
CustomerTrusted_node1699291485544.setCatalogInfo(
    catalogDatabase="nanodegree", catalogTableName="customer_trusted"
)
CustomerTrusted_node1699291485544.setFormat("json")
CustomerTrusted_node1699291485544.writeFrame(CustomerTrusted_node1699291482188)
job.commit()
