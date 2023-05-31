import sys
import boto3
import time

from datetime import datetime, timedelta
from pprint import pprint

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from comlib import get_environment
from comlib import get_json_from_s3
from comlib import insert_log_table
from comlib import get_run_query


params = ["JOB_NAME", "param_bkup_date", "param_hrmart_store_day", "param_tier_store_day"]
args = getResolvedOptions(sys.argv, params)
print(f"args: {args}")
job_name = args.get("JOB_NAME")
job_run_id = args.get("JOB_RUN_ID")
param_bkup_date = args.get('param_bkup_date')
param_hrmart_store_day = args.get('param_hrmart_store_day')
param_tier_store_day = args.get('param_tier_store_day')
print(f"param_bkup_date: {param_bkup_date}")
print(f"param_hrmart_store_day: {param_hrmart_store_day}")
print(f"param_tier_store_day: {param_tier_store_day}")
print("="*50)

red_client = boto3.client("redshift-data")
s3_client = boto3.client("s3")
s3_src = boto3.resource('s3')

env = get_environment(job_name)
print(f"env: {env}")

bucket = f"hr-rcmd-{env}"
folder_bkup_cfg = "config/bkup_tbl.json"
bkup_tbls = get_json_from_s3(s3_client=s3_client, Bucket=bucket, Key=folder_bkup_cfg)
folder_rs_cfg = f"config/redshift_cfg_{env}.json"
redshift_cfg = get_json_from_s3(s3_client=s3_client, Bucket=bucket, Key=folder_rs_cfg)
pprint(f"bucket: {bucket}")
pprint(f"bkup_tbls: {bkup_tbls}")
pprint(f"folder_rs_cfg: {folder_rs_cfg}")
pprint(f"redshift_cfg: {redshift_cfg}")

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
red_bkup_schema = "backup"

s3_src_bucket = s3_src.Bucket(bucket)
prefix_t0 = 'hr_mart/backup/'
date_hrmart = datetime.utcnow() + timedelta(hours=9) - timedelta(days=int(param_hrmart_store_day))
date_hrmart_limit_ymd = date_hrmart.strftime("%Y%m%d")
folder_ymd = set()

print(f"date_hrmart: {date_hrmart}")
print(f"date_hrmart_limit_ymd: {date_hrmart_limit_ymd}")

is_success_write = False
empty_cnt = 0
success_cnt = 0
failure_cnt = 0

s3file_delete_cnt = 0
s3file_skip_cnt = 0

ddl_success_tbls = []
ddl_failure_tbls = []
ddl_success_cnt = 0
ddl_failure_cnt = 0

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

""" log_data default setting """
log_data = {
    "bat_dt" : param_bkup_date,
    "bat_req_tm" : "",
    "job_nm" : job_name,
    "taget_tbl_nm" : "",
    "cretn_cnt" : 0,
    "success_yn" : "",
    "error_msg" : "",
    "job_run_id": job_run_id,
    "platform_dt" : "",
}

if param_bkup_date == "" or param_bkup_date is None:
    raise ValueError(f"param_bkup_date 존재하지 않습니다.: {param_bkup_date}")
if param_hrmart_store_day == "" or param_hrmart_store_day is None:
    raise ValueError(f"param_hrmart_store_day 존재하지 않습니다.:{param_hrmart_store_day}")    
if param_tier_store_day == "" or param_tier_store_day is None:
    raise ValueError(f"param_tier_store_day 존재하지 않습니다.:{param_tier_store_day}")

def get_spark_df(query):
    try:
        df = spark.read\
            .format("com.databricks.spark.redshift") \
            .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
            .option("query", query) \
            .option("tempdir", red_tmp_dir)\
            .option("aws_iam_role", red_iam_role)\
            .load()
    except Exception as e:
        print(f"raise Exception: {e}")
        raise e
    return df


"""
- 원본테이블 DDL과 백업테이블 DDL과 일치여부 검증 프로세스
    - 백업테이블 DDL은 원본테이블 DDL에 BKUP_DT 칼럼이 추가된 형태임.
        (BKUP_DT, COL1, COL2, ... 칼럼순서 일치해야함)
"""
for src_tbl, bkup_tbl in bkup_tbls.items():
    try:
        src_table = f"{red_schema}.{src_tbl}"
        red_bkup_table = f"{red_bkup_schema}.{bkup_tbl}"

        src_sql = f"select * from {src_table} limit 1"
        bkup_sql = f"select * from {red_bkup_table} limit 1"
        df_src  = get_spark_df(src_sql)
        df_bkup = get_spark_df(bkup_sql)
        
        df_bkup_pre = df_bkup.drop("bkup_dt")

        schema_src   = [col for col in df_src.columns]
        schema_bkup_pre = [col for col in df_bkup_pre.columns]

        if schema_src != schema_bkup_pre:
            ddl_failure_cnt += 1
            ddl_failure_tbls.append(red_bkup_table)
            print(f"X 테이블 DDL 불일치:{src_table} <> {red_bkup_table}")
            print(f"schema_src: {schema_src}\n")
            print(f"schema_bkup_pre: {schema_bkup_pre}")
        else:
            ddl_success_cnt += 1
            ddl_success_tbls.append(red_bkup_table)
            print(f"O 테이블 DDL 일치:{src_table} == {red_bkup_table}")
    except Exception as e:
        print(e)
        raise e

print(f"ddl_failure_cnt: {ddl_failure_cnt}")
print(f"ddl_success_cnt: {ddl_success_cnt}")

pprint(f"ddl_failure_tbls:\n {ddl_failure_tbls}")
pprint(f"ddl_success_tbls:\n {ddl_success_tbls}")

if ddl_failure_cnt > 0 :
    msg = f"원본 테이블 DDL과 백업 테이블 DDL이 일치하지 않습니다: \n{ddl_failure_tbls}"
    print(msg)
    raise AssertionError(msg)

print("="*100)


"""
- S3 백업파일 삭제 프로세스
    - S3 백업폴더의 중복제거된 날짜를 구함
    - 삭제보관날짜보다 이전인 날짜의 폴더(prefix)아래의 전체 파일 삭제함
"""
try:
    """
    S3 백업파일 삭제 프로세스 1
    샘플데이터
    ['hr_mart', 'backup', '20221218', 'inst1_tskxbho01_20221218.parquet']
    ['hr_mart', 'backup', '20221218', 'inst1_tskxbho03_20221218.parquet']
    """
    for k in s3_src_bucket.objects.filter(Prefix=prefix_t0):
        key_split = k.key.split('/')
        if key_split[0] and key_split[1] and len(key_split[2]) == 8:
            # print(key_split)
            folder_ymd.add(key_split[2])
            
    folder_ymd = sorted(list(folder_ymd))
    print(f"중복제거된 폴더 날짜(YMD): {folder_ymd}")

    """ S3 백업파일 삭제 프로세스 2 """
    for ymd in folder_ymd:
        prefilx_t0_date = f"{prefix_t0}{ymd}/"
        if ymd < date_hrmart_limit_ymd:
            s3file_delete_cnt += 1
            s3_src_bucket.objects.filter(Prefix=prefilx_t0_date).delete()
            print(f"Delete 삭제 대상 폴더: {prefilx_t0_date}, ymd: {ymd}, date_hrmart_limit_ymd: {date_hrmart_limit_ymd}")
        else:
            s3file_skip_cnt += 1
            print(f"Skip 보관 대상 폴더: {prefilx_t0_date}, ymd: {ymd}, date_hrmart_limit_ymd: {date_hrmart_limit_ymd}")
except Exception as e:
    raise e
finally:
    print(f"s3file_skip_cnt: {s3file_skip_cnt},\n s3file_delete_cnt: {s3file_delete_cnt}")
    print("## Done S3 백업 폴더 삭제 프로세스 ##")
    print("="*100)


"""
- Redshift 테이블 데이터 삭제 및 백업 프로세스
    - S3에 백업 테이블 리스트 Loop(순회)
    - 보관 주기 지난 데이터는 삭제
        ex)보관 주기가 14일 일때 14일이 지난 데이터는 삭제
    - 백업할 날짜의 원본 테이블 데이터 조회
        - 데이터 있으면
            백업 테이블의 백업일자에 Append
        - 데이터가 없으면
            다음 테이블 조회
    # 단, 백업시 백업스키마.백업테이블이 미리 생성 되어있어야함.
"""
for src_tbl, bkup_tbl in bkup_tbls.items():
    try:
        start = time.time()
        is_success_write = False

        src_table = f"{red_schema}.{src_tbl}"
        red_bkup_table = f"{red_bkup_schema}.{bkup_tbl}"
        print(f"src_table: {src_table}")
        print(f"red_bkup_table: {red_bkup_table}")


        """ 백업일자에 적재된 원본 테이블 데이터 조회 쿼리 생성"""
        data_sql = f"""
            select '{param_bkup_date}' as bkup_dt
                 , *
              from {src_table}
             where to_char(platform_dt, 'yyyyMMdd') = '{param_bkup_date}'
        """
        print(data_sql)
        
        """ Redshift 백업 데이터 프레임 생성 """
        df = get_spark_df(data_sql)

        """ 삭제주기 지난 데이터 삭제 처리"""
        query_delete = f"delete from {red_bkup_table} where bkup_dt < to_char((current_timestamp + INTERVAL '9 hours') - INTERVAL '{param_tier_store_day} day', 'yyyyMMdd')"
        status, query_id = get_run_query(red_client, red_cluster_id, red_database, red_secretarn, query_delete)
        print(f"query_delete: {query_delete}")
        print(f"status: {status}, query_id: {query_id}")        

        """ 배치일자에 적재된 데이터 있는지 확인
            없으면 해당 테이블 continue
            있으면 적재
         """
        df_cnt = df.count()
        print(f"{src_table} df_cnt: {df_cnt}")
        if df_cnt <= 0:
            print(f"해당 일자에 원본 테이블 데이터가 존재하지 않습니다.{src_table}")
            empty_cnt += 1
            continue

        df.printSchema()
        for row in df.head(2):
            pprint(row)

        """
        백업 데이터 적재 
        적재 프로세스:
             1. preactions(query_del_today) -> 2. append 
            당일 적재된 데이터 있으면 삭제하고 적재(DELETE -> INSERT)
        """
        df.write\
            .format("com.databricks.spark.redshift") \
            .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
            .option("dbtable", red_bkup_table) \
            .option("tempdir", red_tmp_dir)\
            .option("aws_iam_role", red_iam_role)\
            .option("preactions", f"delete from {red_bkup_table} where bkup_dt = '{param_bkup_date}';")\
            .mode("append")\
            .save()

        is_success_write = True

    except Exception as e:
        print(f"raise Exception: {e}")
        print(f"Failure Backup table: {red_bkup_table}")

        """ setting log_data dict """
        log_data["bat_req_tm"] = "0"
        log_data["taget_tbl_nm"] = red_bkup_table
        log_data["cretn_cnt"] = 0
        log_data["success_yn"] = "N"
        log_data["error_msg"] = repr(e).replace("'", "")
        log_data["platform_dt"] = datetime.utcnow() + timedelta(hours=9)

        insert_log_table(red_log_table, log_data, red_client, red_cluster_id, red_database, red_secretarn)

        failure_cnt += 1
        raise e
    else:
        print(f"Success Backup table: {red_bkup_table}")
        """ setting log_data dict """
        end = time.time()
        sec = end - start
        cretn_cnt = df_cnt

        log_data["bat_req_tm"] = str(timedelta(seconds=sec)).split(".")[0]
        log_data["taget_tbl_nm"] = red_bkup_table
        log_data["cretn_cnt"] = cretn_cnt
        log_data["success_yn"] = "Y"
        log_data["platform_dt"] = datetime.utcnow() + timedelta(hours=9)
        
        insert_log_table(red_log_table, log_data, red_client, red_cluster_id, red_database, red_secretarn)
        success_cnt += 1
    finally:
        print("#"*100)

print("#"*50)
print(f"empty_cnt: {empty_cnt}")
print(f"failure_cnt: {failure_cnt}")
print(f"success_cnt: {success_cnt}")
print("End Glue Job")
print("#"*50)