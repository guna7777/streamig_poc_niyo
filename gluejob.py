import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

dataframe_AmazonKinesis= glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:ap-south-1:722285188889:stream/mongo-to-kinesis-stream-test",
        "classification": "json",
        "startingPosition": "latest",
        "inferSchema": "true",
    },
    transformation_ctx="dataframe_AmazonKinesis",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        AmazonKinesis = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        AmazonS3_path = (
            "s3://mongodb-kinesis/passbook"
            + "/ingest_year="
            + "{:0>4}".format(str(year))
            + "/ingest_month="
            + "{:0>2}".format(str(month))
            + "/ingest_day="
            + "{:0>2}".format(str(day))
            + "/ingest_hour="
            + "{:0>2}".format(str(hour))
            + "/"
        )
        AmazonKinesis = Unbox.apply(frame = AmazonKinesis, path = "detail", format="json")


        AmazonKinesis = UnnestFrame.apply(frame = AmazonKinesis) 
        
        
        
        
        
        AmazonS3 = glueContext.write_dynamic_frame.from_options(
            frame=AmazonKinesis,
            connection_type="s3",
            format="glueparquet",
            connection_options={
                "path": AmazonS3_path,
                "compression": "snappy",
                "partitionKeys": [],
            },
            transformation_ctx="AmazonS3",
        )


glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis,
    batch_function=processBatch,
    options={
        "windowSize": "10 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
