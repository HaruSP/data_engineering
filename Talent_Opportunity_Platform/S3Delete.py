import sys
import boto3

from pprint import pprint

from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from comlib import get_environment

params = ["JOB_NAME"]
args = getResolvedOptions(sys.argv, params)
print(f"args: {args}")
job_name = args.get("JOB_NAME")
job_run_id = args.get("JOB_RUN_ID")
print("="*50)

s3_src = boto3.resource('s3')

env = get_environment(job_name)
print(f"env: {env}")

bucket = f"hr-rcmd-{env}"
s3_src_bucket = s3_src.Bucket(bucket)
path = "redshift/tmpdir"
pprint(f"bucket: {bucket}")

s3file_delete_cnt = 0

for file in s3_src_bucket.object.filter(Prefix=path):
    s3file_delete_cnt += 1
    s3_src_bucket.object.filter(Prefix=path).delete()

print(f"s3file_delete_cnt: {s3file_delete_cnt}\n")

print("#"*50)
print("End Glue Job")
print("#"*50)