import sys
import boto3
import time
from datetime import datetime, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import current_timestamp, expr
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from comlib import get_environment
from comlib import get_json_from_s3
from comlib import get_file_from_s3
from comlib import set_nullable_for_columns
from comlib import insert_log_table


params = ["JOB_NAME", "param_date", "param_sql_filename"]
args = getResolvedOptions(sys.argv, params)
print(f"args: {args}")
job_name = args.get("JOB_NAME")
job_run_id = args.get("JOB_RUN_ID")
param_date = args.get('param_date')
param_sql_filename = args.get('param_sql_filename')
print(f"param_date: {param_date}")
print(f"param_sql_filename: {param_sql_filename}")
print("="*50)

red_client = boto3.client("redshift-data")
s3_client = boto3.client("s3")

env = get_environment(job_name)
print(f"env: {env}")

bucket = f"hr-rcmd-{env}"
folder_rs_cfg = f"config/redshift_cfg_{env}.json"
redshift_cfg = get_json_from_s3(s3_client=s3_client, Bucket=bucket, Key=folder_rs_cfg)

print(f"bucket: {bucket}")
print(f"folder_rs_cfg: {folder_rs_cfg}")
print(f"redshift_cfg: {redshift_cfg}")

red_cluster_id= redshift_cfg.get("red_cluster_id")
red_database= redshift_cfg.get("red_database")
red_schema= redshift_cfg.get("red_schema")
red_tmp_dir=redshift_cfg.get("red_tmp_dir")
red_iam_role= redshift_cfg.get("red_iam_role")
red_jdbc_url= f"{redshift_cfg.get('red_jdbc_url')}/{red_database}"
red_secretarn= redshift_cfg.get('red_secretarn')
red_user= redshift_cfg.get('red_user')
red_password= redshift_cfg.get('red_password')
log_table_nm = redshift_cfg.get('red_log_table')

red_log_table = f"{red_schema}.{log_table_nm}"

is_success_write = False
success_cnt = 0
failure_cnt = 0

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

""" log_data default setting """
log_data = {
    "bat_dt" : param_date,
    "bat_req_tm" : "",
    "job_nm" : job_name,
    "taget_tbl_nm" : "",
    "cretn_cnt" : 0,
    "success_yn" : "",
    "error_msg" : "",
    "job_run_id": job_run_id,
    "platform_dt" : "",
}


for sql_filename in param_sql_filename.split(","):
    try:
        start = time.time()

        is_success_write = False
        table_name = "_".join(sql_filename.split(".")[0].split("_")).lower().strip()
        red_target_table = f"{red_schema}.{table_name}"

        print(f"sql_filename: {sql_filename}")
        print(f"table_name: {table_name}")
        print(f"red_target_table: {red_target_table}")

        key_sql = f"redshift/sql/{sql_filename.strip()}"
        data_sql = get_file_from_s3(s3_client, bucket, key_sql).replace(";", "")

        print(f"key_sql : {key_sql}")
        print(f"data_sql : {data_sql}")

        print("="*70)

        """ 1.Redshift T1 SQL 데이터 가져오기 """
        df = spark.read\
            .format("com.databricks.spark.redshift") \
            .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
            .option("query", data_sql) \
            .option("tempdir", red_tmp_dir)\
            .option("aws_iam_role", red_iam_role)\
            .load()
        
        df.printSchema()

        """ 2.Dataframe T1 Schema nullable=False -> True 전체 전처리 """
        df_pre = spark.read.schema(set_nullable_for_columns(df, True))\
            .format("com.databricks.spark.redshift") \
            .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
            .option("query", data_sql) \
            .option("tempdir", red_tmp_dir)\
            .option("aws_iam_role", red_iam_role)\
            .load()

        df_pre = df_pre.withColumn("platform_dt", current_timestamp() + expr("INTERVAL 9 HOURS"))
        df_pre.printSchema()
        df_pre.head(5)

        """ 3. Redshift 테이블에 적재 유형 DELETE ALL -> APPEND ALL """
        df_pre.write\
            .format("com.databricks.spark.redshift") \
            .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
            .option("dbtable", red_target_table) \
            .option("tempdir", red_tmp_dir)\
            .option("aws_iam_role", red_iam_role)\
            .option("preactions", f"delete from {red_target_table}")\
            .mode("append")\
            .save()

        is_success_write = True
        print(f"Success df_pre.write to Redshift Table: {red_target_table}")

    except Exception as e:
        """ setting log_data dict """
        log_data["bat_req_tm"] = "0"
        log_data["taget_tbl_nm"] = red_target_table
        log_data["cretn_cnt"] = 0
        log_data["success_yn"] = "N"
        log_data["error_msg"] = repr(e).replace("'", "")
        log_data["platform_dt"] = datetime.utcnow() + timedelta(hours=9)

        insert_log_table(red_log_table, log_data, red_client, red_cluster_id, red_database, red_secretarn)
        failure_cnt += 1

        raise e
    else:
        """ setting log_data dict """
        end = time.time()
        sec = end - start
        cretn_cnt = df_pre.count()

        log_data["bat_req_tm"] = str(timedelta(seconds=sec)).split(".")[0]
        log_data["taget_tbl_nm"] = red_target_table
        log_data["cretn_cnt"] = cretn_cnt
        log_data["success_yn"] = "Y"
        log_data["platform_dt"] = datetime.utcnow() + timedelta(hours=9)
        
        insert_log_table(red_log_table, log_data, red_client, red_cluster_id, red_database, red_secretarn)
        success_cnt += 1
        print(f"df_pre count: {cretn_cnt}")
        print("="*80)
    finally:
        if is_success_write == True:
            print(f"Success Write S3-File To Redshift table: {red_target_table}")
        else:
            print(f"Failure Write S3-File To Redshift table: {red_target_table}")


print("#"*50)
print(f"success_cnt: {success_cnt}")
print(f"failure_cnt: {failure_cnt}")
print("End Glue Job")
print("#"*50)