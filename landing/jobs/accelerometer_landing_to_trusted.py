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

# Script generated for node Customer Trusted
CustomerTrusted_node1699385751108 = glueContext.create_dynamic_frame.from_catalog(
    database="nanodegree",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1699385751108",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1699385750882 = glueContext.create_dynamic_frame.from_catalog(
    database="nanodegree",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1699385750882",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT x, y, z, CAST(timestamp AS bigint), user 
FROM accelerometer_landing
JOIN customer_trusted
    ON customer_trusted.email = accelerometer_landing.user
GROUP BY 1, 2, 3, 4, 5
;
"""
SQLQuery_node1699796794389 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": CustomerTrusted_node1699385751108,
        "accelerometer_landing": AccelerometerLanding_node1699385750882,
    },
    transformation_ctx="SQLQuery_node1699796794389",
)

# Script generated for node Amazon S3
AmazonS3_node1699386006259 = glueContext.getSink(
    path="s3://daifs-nanodegree-bucket/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1699386006259",
)
AmazonS3_node1699386006259.setCatalogInfo(
    catalogDatabase="nanodegree", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1699386006259.setFormat("json")
AmazonS3_node1699386006259.writeFrame(SQLQuery_node1699796794389)
job.commit()
