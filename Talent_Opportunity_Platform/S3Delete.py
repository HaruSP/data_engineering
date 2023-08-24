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

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

env = get_environment(job_name)
print(f"env: {env}")

bucket = f"hr-rcmd-{env}"
path = "redshift/tmpdir"
pprint(f"bucket: {bucket}")

s3file_delete_cnt = 0

# 현재 버전의 객체 삭제
for file in s3_resource.Bucket(bucket).objects.filter(Prefix=path):
    s3file_delete_cnt += 1
    file.delete()

# 버전 관리가 활성화된 경우 모든 버전의 객체 삭제
versions = s3_client.list_object_versions(Bucket=bucket, Prefix=path)

if 'DeleteMarkers' in versions:
    for delete_marker in versions['DeleteMarkers']:
        s3_client.delete_object(Bucket=bucket, Key=delete_marker['Key'], VersionId=delete_marker['VersionId'])
        s3file_delete_cnt += 1

if 'Versions' in versions:
    for version in versions['Versions']:
        s3_client.delete_object(Bucket=bucket, Key=version['Key'], VersionId=version['VersionId'])
        s3file_delete_cnt += 1

print(f"s3file_delete_cnt: {s3file_delete_cnt}\n")

print("#"*50)
print("End Glue Job")
print("#"*50)