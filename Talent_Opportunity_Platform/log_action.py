from dis import show_code
from tempfile import tempdir
import boto3
import json
import time
import sys
import boto3
import json
import time

from pprint import pprint
from datetime import datetime, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from comlib import get_environment
from comlib import get_json_from_s3
from comlib import get_es_client
from comlib import insert_log_table


params = ["JOB_NAME", "param_date"]
args = getResolvedOptions(sys.argv, params)
print(f"args: {args}")
job_name = args.get("JOB_NAME")
job_run_id = args.get("JOB_RUN_ID")
param_date = args.get('param_date')

print(f"job_name: {job_name}")
print(f"param_date: {param_date}")

red_client = boto3.client("redshift-data")
s3_client = boto3.client("s3")

env = get_environment(job_name)
print(f"env: {env}")

bucket = f"hr-rcmd-{env}"
folder_rs_cfg = f"config/redshift_cfg_{env}.json"
folder_es_cfg = f"config/es_cfg_{env}.json"

redshift_cfg = get_json_from_s3(s3_client=s3_client, Bucket=bucket, Key=folder_rs_cfg)
es_cfg = get_json_from_s3(s3_client=s3_client, Bucket=bucket, Key=folder_es_cfg)

print(f"bucket: {bucket}")

red_cluster_id= redshift_cfg.get("red_cluster_id")
red_database= redshift_cfg.get("red_database")
red_schema= redshift_cfg.get("red_schema")
red_tmp_dir=redshift_cfg.get("red_tmp_dir")
red_iam_role= redshift_cfg.get("red_iam_role")
red_jdbc_url= f"{redshift_cfg.get('red_jdbc_url')}/{red_database}"
red_secretarn= redshift_cfg.get('red_secretarn')
red_user= redshift_cfg.get('red_user')
red_password= redshift_cfg.get('red_password')
red_target_table="hrtpr.t1_emp_keyin"
log_table_nm = redshift_cfg.get('red_log_table')
red_log_table = f"{red_schema}.{log_table_nm}"

is_exists_index = True
is_success_write = False
success_cnt = 0
failure_cnt = 0
lst_hr_index=[]

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

def value_check(id, input_value):
    try:
        value = df.select(id, df[input_value]).withColumn('row_num',lit(1))

    except Exception as e:
        value = None

    return value

filed_list = [
    "action_ejs",
    "action_method",
    "action_nm",
    "action_param",
    "action_type",
    "action_url",
    "add_datetime",
    "backDoor",
    "emp_dept_cd",
    "emp_dept_nm",
    "emp_nm",
    "emp_no",
    "error_cd",
    "error_text",
    "etc",
    "hqLogin",
    "info_dty_cd",
    "info_jbln_cd",
    "info_jobty_cd",
    "jobcl_cd",
    "logined",
    "private_ip",
    "public_ip",
    "req_rawdata",
    "res_rawdata",
    "role",
    "screen_id",
    "screen_nm",
    "search_count",
    "search_term",
    "search_text",
    "search_time"
]

def value_checking(input):
    print(input)

try:
    start = time.time()

    es = get_es_client(
            path_ca_certs="./http_ca.crt",
            es_username=es_cfg.get("es_username"),
            es_password=es_cfg.get("es_password"),
            env=env
        )

    # 가장 최신 인덱스명 가져오기
    indices = es.indices.get_alias().keys()
    for x in indices:
        if x.startswith('hr_log_action') == True:
            lst_hr_index.append(x)
    lst_hr_index.sort(reverse=True)
    if not lst_hr_index:
        print("lst_hr_index is empty, skipping the process.")
        is_exists_index = False
    else:
        # ES index / _doc 
        es_query_config = {
            "index": "hr_log_action*",
            "scroll": "2m",
            "size": 10000,
            "query": {"match_all": {}}
        }

        res = es.search(
            index=es_query_config.get("index"),
            scroll=es_query_config.get("scroll"),
            size=es_query_config.get("size"),
            source=es_query_config.get("source"),
            query=es_query_config.get("query")
        )

        # Get the scroll ID
        sid = res['_scroll_id']
        scroll_size = len(res['hits']['hits'])     # 페이징 사이즈 1701(한 페이징에 10doc)

        lst = []
        cnt = 0

        while scroll_size > 0:
            """Scrolling..."""

            cnt +=len(res['hits']['hits'])

            res_hits = res["hits"]["hits"]
            for x in res_hits:
                doc = x.get("_source")

                for i in field_list:
                    try:
                        type(doc[i])
                    except:
                        doc[i] = None

                doc["_id"] = x["_id"]
                lst.append(doc)

            res = es.scroll(scroll_id=sid, scroll='2m')

            # Update the scroll ID
            sid = res['_scroll_id']

            # Get the number of results that returned in the last scroll
            scroll_size = len(res['hits']['hits'])

        es.clear_scroll(scroll_id=sid)
        
        print(f"doc 총 건수: {cnt}")
        print("******************* (ES → lst) FIN *******************") 

        df = sc.parallelize(lst).map(lambda x: json.dumps(x))
        df = spark.read.json(df)
        df.printSchema()

        df.show()

        df.createOrReplaceTempView("df_unionbyname")


        """Key, Value 값인 df 생성"""



        df_action_ejs     = value_check(df.id, "action_ejs")
        df_action_method  = value_check(df.id, "action_method")
        df_action_nm      = value_check(df.id, "action_nm")
        df_action_param   = value_check(df.id, "action_param")
        df_action_type    = value_check(df.id, "action_type")		
        df_action_url     = value_check(df.id, "action_url")
        df_add_datetime   = value_check(df.id, "add_datetime")
        df_backDoor       = value_check(df.id, "backDoor")
        df_emp_dept_cd    = value_check(df.id, "emp_dept_cd")
        df_emp_dept_nm    = value_check(df.id, "emp_dept_nm")
        df_emp_nm         = value_check(df.id, "emp_nm")
        df_emp_no         = value_check(df.id, "emp_no")
        df_error_cd       = value_check(df.id, "error_cd")
        df_error_text     = value_check(df.id, "error_text")
        df_etc            = value_check(df.id, "etc")
        df_hqLogin        = value_check(df.id, "hqLogin")
        df_info_dty_cd    = value_check(df.id, "info_dty_cd")
        df_info_jbln_cd   = value_check(df.id, "info_jbln_cd")
        df_info_jobty_cd  = value_check(df.id, "info_jobty_cd")
        df_jobcl_cd       = value_check(df.id, "jobcl_cd")
        df_logined        = value_check(df.id, "logined")
        df_private_ip     = value_check(df.id, "private_ip")
        df_public_ip      = value_check(df.id, "public_ip")
        df_req_rawdata    = value_check(df.id, "req_rawdata")
        df_res_rawdata    = value_check(df.id, "res_rawdata")
        df_role           = value_check(df.id, "role")
        df_screen_id      = value_check(df.id, "screen_id")
        df_screen_nm      = value_check(df.id, "screen_nm")
        df_search_count   = value_check(df.id, "search_count")
        df_search_term    = value_check(df.id, "search_term")
        df_search_text    = value_check(df.id, "search_text")
        df_search_time    = value_check(df.id, "search_time")
        
        join_key = ["emp_no", "row_num"]

        df_unionbyname = df_action_ejs.join(df_action_method, join_key, "full")\
                                    .join(df_action_nm, join_key, "full")\
                                    .join(df_action_param, join_key, "full")\
                                    .join(df_action_type, join_key, "full")\
                                    .join(df_action_url, join_key, "full")\
                                    .join(df_add_datetime, join_key, "full")\
                                    .join(df_backDoor, join_key, "full")\
                                    .join(df_emp_dept_cd, join_key, "full")\
                                    .join(df_emp_dept_nm, join_key, "full")\
                                    .join(df_emp_nm, join_key, "full")\
                                    .join(df_emp_no, join_key, "full")\
                                    .join(df_error_cd, join_key, "full")\
                                    .join(df_error_text,join_key, "full")\
                                    .join(df_etc,join_key, "full")\
                                    .join(df_hqLogin,join_key, "full")\
                                    .join(df_info_dty_cd,join_key, "full")\
                                    .join(df_info_jbln_cd,join_key, "full")\
                                    .join(df_info_jobty_cd,join_key, "full")\
                                    .join(df_jobcl_cd,join_key, "full")\
                                    .join(df_logined,join_key, "full")\
                                    .join(df_private_ip,join_key, "full")\
                                    .join(df_public_ip,join_key, "full")\
                                    .join(df_req_rawdata,join_key, "full")\
                                    .join(df_res_rawdata,join_key, "full")\
                                    .join(df_role,join_key, "full")\
                                    .join(df_screen_id,join_key, "full")\
                                    .join(df_screen_nm,join_key, "full")\
                                    .join(df_search_count,join_key, "full")\
                                    .join(df_search_term,join_key, "full")\
                                    .join(df_search_text,join_key, "full")\
                                    .join(df_search_time,join_key, "full")
        
        df_unionbyname.printSchema()

        """union한 df TempView 생성"""
        df_unionbyname.createOrReplaceTempView("df_unionbyname")

        """레드시프트 칼럼 순서에 맞게 df 정렬 / 모든칼럼 nvl처리"""
        select_df = spark.sql("""
        select 
            nvl(_id, '') as _id
            ,nvl(action_ejs, '') as action_ejs
            ,nvl(action_method, '') as action_method
            ,nvl(action_nm,'') as action_nm
            ,nvl(action_param,'') as action_param
            ,nvl(action_type,'') as action_type
            ,nvl(action_url,'') as action_url
            ,to_timestamp(substring(nvl(add_datetime,'99991231 00:00:00'),1,4)||'-'||substring(nvl(add_datetime,'99991231 00:00:00'),5,2)||'-'||substring(nvl(add_datetime,'99991231 00:00:00'),7,2)||substring(nvl(add_datetime,'99991231 00:00:00'),9,9)) as add_datetime
            ,nvl(backDoor,'') as backDoor
            ,nvl(emp_dept_cd,'') as emp_dept_cd
            ,nvl(emp_dept_nm,'') as emp_dept_nm
            ,nvl(emp_nm,'') as emp_nm
            ,nvl(emp_no,'') as emp_no
            ,nvl(error_cd,'') as error_cd
            ,nvl(error_text,'') as error_text
            ,nvl(etc,'') as etc
            ,nvl(hqLogin,'') as hqLogin
            ,nvl(info_dty_cd,'') as info_dty_cd
            ,nvl(info_jbln_cd,'') as info_jbln_cd
            ,nvl(info_jobty_cd,'') as info_jobty_cd
            ,nvl(jobcl_cd,'') as jobcl_cd
            ,nvl(logined,'') as logined
            ,nvl(private_ip,'') as private_ip
            ,nvl(public_ip,'') as public_ip
            ,nvl(req_rawdata,'') as req_rawdata
            ,nvl(res_rawdata,'') as res_rawdata
            ,nvl(role,'') as role
            ,nvl(screen_id,'') as screen_id
            ,nvl(screen_nm,'') as screen_nm
            ,nvl(search_count,'') as search_count
            ,nvl(search_term,'') as search_term
            ,nvl(search_text,'') as search_text
            ,nvl(search_time,'') as search_time
        from df_unionbyname
        """)

        print("checkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkking")
        select_df.show(3)

        select_df = select_df.withColumn("platform_dt", current_timestamp() + expr("INTERVAL 9 HOURS"))
        select_df.printSchema()
        
        """ Redshift 테이블에 적재
            DELETE 전체 건 -> APPEND 전체 건
        """
        select_df.write\
            .format("com.databricks.spark.redshift") \
            .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
            .option("dbtable", red_target_table) \
            .option("tempdir", red_tmp_dir)\
            .option("aws_iam_role", red_iam_role)\
            .option("preactions", f"delete from {red_target_table}")\
            .mode("append")\
            .save()

        is_success_write = True
        print("******************* RS write FIN *******************")


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

    print(f'Error : {e}')
    raise e

else:
    if is_exists_index:
        cretn_cnt = select_df.count()
    else:
        cretn_cnt = 0

    """ setting log_data dict """
    end = time.time()
    sec = end - start

    log_data["bat_req_tm"] = str(timedelta(seconds=sec)).split(".")[0]
    log_data["taget_tbl_nm"] = red_target_table
    log_data["cretn_cnt"] = cretn_cnt
    log_data["success_yn"] = "Y"
    log_data["platform_dt"] = datetime.utcnow() + timedelta(hours=9)
        
    insert_log_table(red_log_table, log_data, red_client, red_cluster_id, red_database, red_secretarn)
    success_cnt += 1
    print(f"select_df count: {cretn_cnt}")
    print("="*80)

finally:
    if is_success_write == True:
        print(f"Success Write S3-File To Redshift table: {red_target_table}")
    else:
        print(f"Failure Write S3-File To Redshift table: {red_target_table}")