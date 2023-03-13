import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_path", "tname"])
job_name = args["JOB_NAME"]
s3_location = args["s3_path"]
table_name = args["tname"]


sc=SparkContext()
glueContext=context.GlueContext(sc)
ob=job.Job(glueContext)
job.init(args["Job_name"],args)
job.init(job_name, args)


dynamic_frame_creation = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={"dynamodb.input.tableName": table_name,
        "dynamodb.throughput.read.percent": "1.0", #(0.5 default -- 1.5 maximum)
        "dynamodb.splits": "100"
    }
)

# we need to map the dynamic frame values according to the type of attributes. The below is the transformation.
transformed_data_frame = dynamic_frame_creation.select_fields(["Id", "name", "rollno"]).apply_mapping([
    ("Id", "int", "id", "int"),
    ("name", "string", "name", "string"),
    ("rollno", "int", "rollno", "int")])
    
    
write_to_s3 = glueContext.write_dynamic_frame.from_options(
    frame=transformed_data_frame,
    connection_type="s3",
    connection_options={
        "path": f"{s3_path}/insrt_tstmp={timestamp}",
        "partitionKeys": "insrt_tstmp",
        transformation_ctx = "write_to_s3"
    },
    format="parquet")

job.commit()
