# -*- coding: utf-8 -*-
import sys
import re
import os
import boto3
import zipfile
import asyncio
import time
from datetime import datetime, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, DecimalType
from pyspark.sql.functions import current_timestamp, expr
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from comlib import get_json_from_s3
from comlib import insert_log_table
from comlib import get_environment

from keyword_module import colleague_praise_keyword

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from sentence_transformers import SentenceTransformer, util
import torch
import pandas as pd

from pynori.korean_analyzer import KoreanAnalyzer

from transformers import pipeline


params = ["JOB_NAME", "param_date"]
args = getResolvedOptions(sys.argv, params)
print(f"args: {args}")
job_name = args.get("JOB_NAME")
job_run_id = args.get("JOB_RUN_ID")
param_date = args.get('param_date')
print(f"param_date: {param_date}")
print("="*50)

## 개발환경 redshift 접속정보 ##
red_client = boto3.client("redshift-data")
s3_client = boto3.client("s3")

env = get_environment(job_name)

print(f"env: {env}")

bucket = f"hr-rcmd-{env}"
folder_rs_cfg = f"config/redshift_cfg_{env}.json"
redshift_cfg = get_json_from_s3(s3_client=s3_client, Bucket=bucket, Key=folder_rs_cfg)

print(f"bucket: {bucket}")
print(f"folder_rs_cfg: {folder_rs_cfg}")

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

table_name = 't1_emp_kywr_colleague'
red_target_table = f'hrtpr.{table_name}'

is_success_write = False
success_cnt = 0
failure_cnt = 0

# 모델 파일 다운 및 압축풀기 - TAACO_STS
zip_file_path = './TAACO_STS.zip'
open_path = './TAACO_STS'
os.mkdir("/tmp/TAACO_STS")
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(open_path)

print("*"*50, "model loading", "*"*50)
model = SentenceTransformer('/tmp/TAACO_STS')
print("*"*50, "model loading end", "*"*50)



# 형태소분석기 생성 - pynori
nori = KoreanAnalyzer(
           decompound_mode='NONE', # DISCARD or MIXED or NONE
           infl_decompound_mode='NONE', # DISCARD or MIXED or NONE
           discard_punctuation=False,
           output_unknown_unigrams=False,
           pos_filter=False, 
           synonym_filter=False, mode_synonym='NORM', # NORM or EXTENSION
       ) 
       

## SPARK 세션 생성 ##
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
print('*'*50, 'spark session 생성완료', '*'*50)


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

try:
    start = time.time()
    print('*********************************************동료평가+리더십 DataFrame 생성**************************************************')
    ath_df = spark.read.format("com.databricks.spark.redshift") \
                .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
                .option("dbtable", 'hrtpr.t1_emp_ath') \
                .option("tempdir", red_tmp_dir)\
                .option("aws_iam_role", red_iam_role)\
                .load()

    ath_df.createOrReplaceTempView('ath')



    dvsn_df = spark.read.format("com.databricks.spark.redshift") \
            .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
            .option("dbtable", 'hrtpr.t2_dvsn_center') \
            .option("tempdir", red_tmp_dir)\
            .option("aws_iam_role", red_iam_role)\
            .load()

    dvsn_df.createOrReplaceTempView('dvsn')

    emp_df = spark.read.format("com.databricks.spark.redshift") \
                .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
                .option("dbtable", 'hrtpr.t2_emp_center') \
                .option("tempdir", red_tmp_dir)\
                .option("aws_iam_role", red_iam_role)\
                .load()

    emp_df.createOrReplaceTempView('emp')
    emp_sql = """
        select distinct emp.emp_no as info_emp_no
        from emp emp
        inner join ath ath
                on emp.emp_no = ath.ath_emp_no
        where 1=1
        and ath.ath_role_cd like '%R04%'
        """
    emp_df_pre = spark.sql(emp_sql)
    emp_df_pre.createOrReplaceTempView('emp_pre')

    colleague_df = spark.read.format("com.databricks.spark.redshift") \
                .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
                .option("dbtable", 'hrtpr.t1_emp_cllg_valua') \
                .option("tempdir", red_tmp_dir)\
                .option("aws_iam_role", red_iam_role)\
                .load()

    colleague_df.createOrReplaceTempView('colleague')
    colleague_sql = """
        select trim(c.emp_no) as emp_no
            , array_join(collect_list(c.cllg_valua), ', ') as colleague_text
        from colleague c
            inner join emp_pre e on c.emp_no=e.info_emp_no
        group by c.emp_no
        """
    colleague_df = spark.sql(colleague_sql)

    colleague_df.printSchema()
    colleague_df.show(10)


    print('*********************************************동료평가+리더십 DataFrame 생성완료**************************************************')


    # 동료평가
    colleague_list = colleague_df.collect()
    colleague_ky_list = []

    print('*'*20, '동료평가 키워드 생성시작', '*'*20)
    # 동료데이터 키워드 생성
    loop = asyncio.get_event_loop()
    for colleague in colleague_list:
        emp_no = colleague['emp_no']
        colleague_opi = colleague['colleague_text']
        
        try:
            # colleague_opi = re.sub(emoj, '', colleague_opi)
            colleague_opi += '. '
            colleague_opi = colleague_opi.replace('$%^', '. ')
            colleague_opi = re.sub('[^A-Za-z0-9ㄱ-힣!?, \./]', ' ', colleague_opi)
            colleague_opi = re.sub(' +', ' ', colleague_opi)
            
            result = asyncio.run(colleague_praise_keyword(emp_no, colleague_opi, nori, model))
            colleague_ky_list+=result
            
        except Exception as e:
            print(f'Error : {e}')
            print(f'에러발생한 직원번호 : {emp_no}')
            print(f'평가내용 : {colleague_opi}')
            colleague_ky_list+=[(emp_no, None, None)]
        

        # break

    print('*'*20, '동료평가 키워드 생성완료', '*'*20)


    print('*************************spark dataframe으로 변환시작**************************')

    schema = StructType([StructField("emp_no", StringType(), True), StructField("KYWR_CLLG_VALUA", StringType(), True), StructField("KYWR_CLLG_VALUA_RANK", LongType(), True)])
    colleague_df = spark.createDataFrame(colleague_ky_list, schema=schema)

    print('*************************spark dataframe으로 변환완료**************************')

    t1_emp_kywr_df = colleague_df.withColumnRenamed("emp_no", "KYWR_EMP_NO")\
                            .withColumn("platform_dt", current_timestamp() + expr("INTERVAL 9 HOURS"))


    print(f' 최종적으로 적재할 df : {t1_emp_kywr_df.printSchema()}')


    print('*********************load redshift start*******************************************')
    t1_emp_kywr_df.write\
        .format("com.databricks.spark.redshift") \
        .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
        .option("dbtable", red_target_table) \
        .option("tempdir", red_tmp_dir)\
        .option("aws_iam_role", red_iam_role)\
        .option("preactions", f"delete from {red_target_table}")\
        .mode("append")\
        .save()

    print('*********************load redshift end*******************************************')

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
        cretn_cnt = t1_emp_kywr_df.count()

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