import sys
import boto3
import json
import time
import gzip

from pprint import pprint
from datetime import datetime, timedelta
from collections import defaultdict
from elasticsearch import helpers

from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from comlib import make_path
from comlib import run_cmd_ls
from comlib import run_cmd_move
from comlib import get_json_from_s3
from comlib import get_es_client
from comlib import get_run_query
from comlib import insert_log_table
from comlib import get_environment


params = ["JOB_NAME", "param_date"]
args = getResolvedOptions(sys.argv, params)
pprint(f"args: {args}")
job_name = args.get("JOB_NAME")
job_run_id = args.get("JOB_RUN_ID")
param_date = args.get('param_date')
print(f"param_date: {param_date}")
print("="*50)

red_client = boto3.client("redshift-data")
s3_client = boto3.client("s3")
env = get_environment(job_name)

print(f"env: {env}")

bucket = f"hr-rcmd-{env}"
folder_rs_cfg = f"config/redshift_cfg_{env}.json"
folder_es_cfg = f"config/es_cfg_{env}.json"

redshift_cfg = get_json_from_s3(s3_client=s3_client, Bucket=bucket, Key=folder_rs_cfg)
es_cfg = get_json_from_s3(s3_client=s3_client, Bucket=bucket, Key=folder_es_cfg)

agg_dict = defaultdict(dict)
index_name = f"hr_dty_rcmd_emp_{param_date}"
is_success_write = False
s3_bucket_folder = f"s3://{bucket}/redshift/unload/hr_dty_rcmd_emp/"
prefix_gzip_filename = f"{index_name}_"
s3_unload_gzip_filepath = make_path(s3_bucket_folder, prefix_gzip_filename)

print(f"bucket: {bucket}")
print(f"folder_rs_cfg: {folder_rs_cfg}")
print(f"index_name: {index_name}")
print(f"s3_bucket_folder: {s3_bucket_folder}")
print(f"prefix_gzip_filename: {prefix_gzip_filename}")
print(f"s3_unload_gzip_filepath: {s3_unload_gzip_filepath}")

red_cluster_id= redshift_cfg.get("red_cluster_id")
red_database= redshift_cfg.get("red_database")
red_schema= redshift_cfg.get("red_schema")
red_tmp_dir=redshift_cfg.get("red_tmp_dir")
red_iam_role= redshift_cfg.get("red_iam_role")
red_jdbc_url= f"{redshift_cfg.get('red_jdbc_url')}/{red_database}"
red_secretarn= redshift_cfg.get('red_secretarn')
log_table_nm = redshift_cfg.get('red_log_table')
red_log_table = f"{red_schema}.{log_table_nm}"


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
    

cretn_cnt_sum = 0
try:
    start = time.time()

    es = get_es_client(
            path_ca_certs="./http_ca.crt",
            es_username=es_cfg.get("es_username"),
            es_password=es_cfg.get("es_password"),
            env=env
        )

    unload_sql = f"""
    UNLOAD
    (
    $$
    -- PK : dre_cd
    select
            nvl(concat(f.emp_no, f.f_dty_cd), '') as dre_cd
            , nvl(f.emp_no, '') as dre_emp_no
            , nvl(info_jobcl_cd, '') as dre_jobcl_cd
            , nvl(f.f_jobty_cd, '') as dre_jobty_cd
            , nvl(f.f_jbln_cd, '') as dre_jbln_cd
            , nvl(f.f_dty_cd, '') as dre_dty_cd
            , nvl(f.f_dty_rnk, 0) as dre_dty_rnk
            , nvl(f.f_dty_part_rnk , 0) as dre_dty_part_rnk
            , nvl(f.f_part_score, 0.0) as dre_part_score
            , nvl(f.f_score, 0.0) as dre_score
            , nvl(f01_score, 0.0) as f01_score
            , nvl(f02_score, 0.0) as f02_score
            , nvl(f03_score, 0.0) as f03_score
            , nvl(f04_score, 0.0) as f04_score
            , nvl(f05_score, 0.0) as f05_score
            , nvl(f06_score, 0.0) as f06_score
            , nvl(f07_score, 0.0) as f07_score
            , nvl(f08_score, 0.0) as f08_score
            , nvl(ruleout_yn, '') as ruleout_yn
            , nvl(t.array_trng_y_dvsn_cd, '') as array_trng_y_dvsn_cd
            , nvl(t.array_trng_n_dvsn_cd, '') as array_trng_n_dvsn_cd
            , to_char(f.platform_dt, 'YYYYMMDD') as platform_dt
    from hrtpr.t3_emp_feature f
        left outer join (
            select trng_emp_no
                 , trng_emp_nm
                 , listagg(distinct trng_dvsn_cd, ',') as array_trng_y_dvsn_cd
                 , '' AS array_trng_n_dvsn_cd
              from hrtpr.t1_emp_trng
             where trng_try_yn = 'Y'  
             group by trng_emp_no, trng_emp_nm
             UNION ALL
            select trng_emp_no
                 , trng_emp_nm
                 , '' AS array_trng_y_dvsn_cd
                 , listagg(distinct trng_dvsn_cd, ',') as array_trng_n_dvsn_cd
              from hrtpr.t1_emp_trng
             where trng_try_yn = 'N'
             group by trng_emp_no, trng_emp_nm 
        ) t ON f.emp_no = t.trng_emp_no
        left outer join (
      		select emp_no
      			, ruleout_yn
      		from t1_ruleout_emp
        ) o on f.emp_no = o.emp_no        
        left outer join (
      		select info_emp_no
      			, info_jobcl_cd
      		from t1_emp_info
        ) info on f.emp_no = info.info_emp_no
    $$
    )
    TO '{s3_unload_gzip_filepath}'
    iam_role '{red_iam_role}'
    json
    parallel OFF gzip
    allowoverwrite
    """

    """
    1. RS UNLOAD SQL 실행하여 S3에 GZ 파일 업로드
    2. S3에 있는 GZ파일을 Glue에 MOVE 함
    3. Glue에서 GZ파일 읽어서 AGG Dictionary 만듬
    4. AGG Dictionary를 Bulk API 용 LIST의 JSON 문자열 만들기
    5. ES BULK API 실행
    6. Response Bulk API 건수 확인
    """
    status, query_id = get_run_query(red_client, red_cluster_id, red_database, red_secretarn, unload_sql)
    print(f"status: {status}, query_id: {query_id}")
    print("## 1. Success RS UNLOAD ##")

    """ S3 적재경로에 파일이 이미 존재한다면 MOVE 하기 """
    res_status_code, file_exists_name = run_cmd_ls(s3_bucket_folder)
    if res_status_code == 0:
        exists_file_path = make_path(s3_bucket_folder, file_exists_name)
        res_status_code2 = run_cmd_move(exists_file_path, "./")
        if res_status_code2 == 0:
            print("## 2. Success file move to Glue Linux Directory ##")
        else:
            print("## 2. Failure file move to Glue Linux Directory ##")
        print(f"res_status_code2: {res_status_code2}")
        print(f"file_exists_name: {file_exists_name}")

    """ 데이터를 es에 색인할 es json doc 형태로 변환 후 색인 """
    def es_indexing():
        def make_docs():
            for k, v in agg_dict.items():
                # print(k)
                doc = {
                    "_index": index_name,
                    "_id": k,
                    "_source": json.dumps(v, ensure_ascii=False)
                }
                yield doc

        global cretn_cnt_sum
        """ Elasticsearch bulk API RUN """
        print("## Elasticsearch bulk API RUN ##")
        res = helpers.bulk(es, make_docs())
        if res:
            is_success_write = True
            cretn_cnt_sum += res[0]
            print(f"{index_name} {cretn_cnt_sum}건 적재되었습니다.")
        else:
            is_success_write = False
            print(f"{index_name} {cretn_cnt_sum}건 적재되었습니다.")

    """ Gzip 파일 읽어서 AGG JSON 만들기 """
    with gzip.open(f"./{file_exists_name}", mode="r") as f:
        for idx, line in enumerate(f, 0):
            """ es 문서 30만건 생성시 bulk 색인 """
            if idx > 0 and idx % 300000 == 0:
                #print(f"idx: {idx}")
                es_indexing()
                agg_dict.clear()

            src_dict = json.loads(line.decode("utf-8"))
            dre_cd = src_dict["dre_cd"]

            """ 1. agg_dict 매핑 정의하기(직무추천코드 별) """
            if agg_dict.get(dre_cd) is None:
                agg_dict[dre_cd]["dre_cd"] = str()
                agg_dict[dre_cd]["dre_emp_no"] = str()
                agg_dict[dre_cd]["dre_jobcl_cd"] = str()
                agg_dict[dre_cd]["dre_jobty_cd"] = str()
                agg_dict[dre_cd]["dre_jbln_cd"] = str()
                agg_dict[dre_cd]["dre_dty_cd"] = str()
                agg_dict[dre_cd]["ruleout_yn"] = str()
                agg_dict[dre_cd]["platform_dt"] = str()

                agg_dict[dre_cd]["dre_dty_rnk"] = int()
                agg_dict[dre_cd]["dre_dty_part_rnk"] = int()

                agg_dict[dre_cd]["f01_score"] = float()
                agg_dict[dre_cd]["f02_score"] = float()
                agg_dict[dre_cd]["f03_score"] = float()
                agg_dict[dre_cd]["f04_score"] = float()
                agg_dict[dre_cd]["f05_score"] = float()
                agg_dict[dre_cd]["f06_score"] = float()
                agg_dict[dre_cd]["f07_score"] = float()
                agg_dict[dre_cd]["f08_score"] = float()
                agg_dict[dre_cd]["dre_score"] = float()
                agg_dict[dre_cd]["dre_part_score"] = float()

                # array 필드 생성
                agg_dict[dre_cd]["array_trng_y_dvsn_cd"] = list()
                agg_dict[dre_cd]["array_trng_n_dvsn_cd"] = list()
                
                # dense_vector 필드(dimension: 8) 생성
                agg_dict[dre_cd]["feature_vector"] = list()

            """ 2. 직무추천코드별 딕셔너리에 값 넣기
                ## Key, Value 값 넣기 ## """
            agg_dict[dre_cd]["dre_cd"] = dre_cd
            if src_dict.get("dre_emp_no"): agg_dict[dre_cd]["dre_emp_no"] = src_dict.get("dre_emp_no")
            if src_dict.get("dre_jobcl_cd"): agg_dict[dre_cd]["dre_jobcl_cd"] = src_dict.get("dre_jobcl_cd")
            if src_dict.get("dre_jobty_cd"): agg_dict[dre_cd]["dre_jobty_cd"] = src_dict.get("dre_jobty_cd")
            if src_dict.get("dre_jbln_cd"): agg_dict[dre_cd]["dre_jbln_cd"] = src_dict.get("dre_jbln_cd")
            if src_dict.get("dre_dty_cd"): agg_dict[dre_cd]["dre_dty_cd"] = src_dict.get("dre_dty_cd")
            if src_dict.get("ruleout_yn"): agg_dict[dre_cd]["ruleout_yn"] = src_dict.get("ruleout_yn")
            if src_dict.get("platform_dt"): agg_dict[dre_cd]["platform_dt"] = src_dict.get("platform_dt")

            if src_dict.get("dre_dty_rnk"): agg_dict[dre_cd]["dre_dty_rnk"] = src_dict.get("dre_dty_rnk")
            if src_dict.get("dre_dty_part_rnk"): agg_dict[dre_cd]["dre_dty_part_rnk"] = src_dict.get("dre_dty_part_rnk")
            
            if src_dict.get("f01_score"):
                agg_dict[dre_cd]["f01_score"] = src_dict.get("f01_score")
                agg_dict[dre_cd]['feature_vector'].append(src_dict.get("f01_score")) #dense_vector 의 dimension 에 추가
            else:
                agg_dict[dre_cd]['feature_vector'].append(0.0)                       #dense_vector 의 dimension 에 추가
            if src_dict.get("f02_score"):
                agg_dict[dre_cd]["f02_score"] = src_dict.get("f02_score")
                agg_dict[dre_cd]['feature_vector'].append(src_dict.get("f02_score")) #dense_vector 의 dimension 에 추가
            else:
                agg_dict[dre_cd]['feature_vector'].append(0.0)                       #dense_vector 의 dimension 에 추가
            if src_dict.get("f03_score"):
                agg_dict[dre_cd]["f03_score"] = src_dict.get("f03_score")
                agg_dict[dre_cd]['feature_vector'].append(src_dict.get("f03_score")) #dense_vector 의 dimension 에 추가
            else:
                agg_dict[dre_cd]['feature_vector'].append(0.0)                       #dense_vector 의 dimension 에 추가
            if src_dict.get("f04_score"):
                agg_dict[dre_cd]["f04_score"] = src_dict.get("f04_score")
                agg_dict[dre_cd]['feature_vector'].append(src_dict.get("f04_score")) #dense_vector 의 dimension 에 추가
            else:
                agg_dict[dre_cd]['feature_vector'].append(0.0)                       #dense_vector 의 dimension 에 추가
            if src_dict.get("f05_score"):
                agg_dict[dre_cd]["f05_score"] = src_dict.get("f05_score")
                agg_dict[dre_cd]['feature_vector'].append(src_dict.get("f05_score")) #dense_vector 의 dimension 에 추가
            else:
                agg_dict[dre_cd]['feature_vector'].append(0.0)                       #dense_vector 의 dimension 에 추가
            if src_dict.get("f06_score"):
                agg_dict[dre_cd]["f06_score"] = src_dict.get("f06_score")
                agg_dict[dre_cd]['feature_vector'].append(src_dict.get("f06_score")) #dense_vector 의 dimension 에 추가
            else:
                agg_dict[dre_cd]['feature_vector'].append(0.0)                       #dense_vector 의 dimension 에 추가
            if src_dict.get("f07_score"):
                agg_dict[dre_cd]["f07_score"] = src_dict.get("f07_score")
                agg_dict[dre_cd]['feature_vector'].append(src_dict.get("f07_score")) #dense_vector 의 dimension 에 추가
            else:
                agg_dict[dre_cd]['feature_vector'].append(0.0)                       #dense_vector 의 dimension 에 추가
            if src_dict.get("f08_score"):
                agg_dict[dre_cd]["f08_score"] = src_dict.get("f08_score")
                agg_dict[dre_cd]['feature_vector'].append(src_dict.get("f08_score")) #dense_vector 의 dimension 에 추가
            else:
                agg_dict[dre_cd]['feature_vector'].append(0.0)                       #dense_vector 의 dimension 에 추가

            if src_dict.get("dre_score"): agg_dict[dre_cd]["dre_score"] = src_dict.get("dre_score")
            if src_dict.get("dre_part_score"): agg_dict[dre_cd]["dre_part_score"] = src_dict.get("dre_part_score")

            """ 
            ## Nested 필드 생성 ## 
            1. 공백이 아닌 값만 append 함.
            2. YN 칼럼은 unload sql에서 nval(col, '') 처리함.
            """

            """ value 가 여러개인 Array 형태의 필드들 생성 """
            if src_dict.get('array_trng_y_dvsn_cd'):
                agg_dict[dre_cd]['array_trng_y_dvsn_cd'].extend(src_dict.get('array_trng_y_dvsn_cd').split(","))
            if src_dict.get('array_trng_n_dvsn_cd'):
                agg_dict[dre_cd]['array_trng_n_dvsn_cd'].extend(src_dict.get('array_trng_n_dvsn_cd').split(","))

        """ es 문서 색인되지 않은 잔여 bulk 색인 """
        if len(agg_dict) > 0:
            es_indexing()
            agg_dict.clear()

except Exception as e:
    """ setting log_data dict """
    log_data["bat_req_tm"] = "0"
    log_data["taget_tbl_nm"] = index_name
    log_data["cretn_cnt"] = cretn_cnt_sum
    log_data["success_yn"] = "N"
    log_data["error_msg"] = repr(e).replace("'", "")
    log_data["platform_dt"] = datetime.utcnow() + timedelta(hours=9)

    insert_log_table(red_log_table, log_data, red_client, red_cluster_id, red_database, red_secretarn)

    print(f"raise Exception: {e}")
    raise e
else:
    """ setting log_data dict """
    end = time.time()
    sec = end - start

    log_data["bat_req_tm"] = str(timedelta(seconds=sec)).split(".")[0]
    log_data["taget_tbl_nm"] = index_name
    log_data["cretn_cnt"] = cretn_cnt_sum
    log_data["success_yn"] = "Y"
    log_data["platform_dt"] = datetime.utcnow() + timedelta(hours=9)

    insert_log_table(red_log_table, log_data, red_client, red_cluster_id, red_database, red_secretarn)

finally:
    if is_success_write == True:
        print(f"5.Success ES Indexing: {index_name}")
    else:
        print(f"5.Failure ES Indexing: {index_name}")