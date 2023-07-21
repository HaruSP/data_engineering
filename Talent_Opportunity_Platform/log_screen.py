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
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, DateType

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
param_month = args.get('param_month')

print(f"job_name: {job_name}")
print(f"param_date: {param_date}")
print(f"param_month: {param_month}")

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
red_target_table="hrtpr.log_screen"
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


field_list = [
    "emp_no",
    "emp_nm",
    "emp_dept_cd",
    "emp_dept_nm",
    "info_jobty_cd",
    "info_jbln_cd",
    "info_dty_cd",
    "jobcl_cd",
    "private_ip",
    "screen_nm",
    "add_datetime"
]


try:
    start = time.time()

    es = get_es_client(
            path_ca_certs="./http_ca.crt",
            es_username=es_cfg.get("es_username"),
            es_password=es_cfg.get("es_password"),
            env=env
        )

    # 인덱스 존재 체크
    indices = es.indices.get_alias().keys()
    for x in indices:
        if x.startswith('hr_log_action') == True:
            lst_hr_index.append(x)
    lst_hr_index.sort(reverse=True)
    if not lst_hr_index:
        print("lst_hr_index is empty, skipping the process.")
        is_exists_index = False
    else:
        index_name = lst_hr_index[0]
        print("### 최근 생성된 인덱스명 : ", index_name)

        # ES index / _doc 
        es_query_config = {
            "index": "index_name*",
            "scroll": "2m",
            "size": 10000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "backDoor": "false"
                            }
                        },
                        {
                            "match": {
                                "logined": "true"
                            }
                        }
                    ],
                    "must_not": [
                        {
                            "match": {
                                "screen_nm": "권한 미 존재."
                            }
                        },
                        {
                            "match": {
                                "screen_nm": ""
                            }
                        }
                    ]
                }
            }
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
        total_hits = res['hits']['total']['value']

        lst = []
        cnt = 0

        while scroll_size > 0:
            """Scrolling..."""
            cnt +=len(res['hits']['hits'])
            res_hits = res["hits"]["hits"]
            
            for x in res_hits:
                doc = {"_id": x["_id"]}

                for i in field_list:
                    try:
                        if x ["_source"][i] == "-":
                            doc[i] = None
                        else:
                            doc[i] = x["_source"][i]
                    # 필드가 없는 경우
                    except:
                        doc[i] = None

                lst.append(doc)

            if len(lst) >= total_hits:
                break

            res = es.scroll(scroll_id=sid, scroll='2m')

        print("*"*30)

        schema = StructType(
            [
                StructField("_id",StringType(), nullable=True),
                StructField("emp_no",StringType(), nullable=True),
                StructField("emp_nm",StringType(), nullable=True),
                StructField("emp_dept_cd",StringType(), nullable=True),
                StructField("emp_dept_nm",StringType(), nullable=True),
                StructField("info_jobty_cd",StringType(), nullable=True),
                StructField("info_jbln_cd",StringType(), nullable=True),
                StructField("info_dty_cd",StringType(), nullable=True),
                StructField("jpbcl_cd",StringType(), nullable=True),
                StructField("private_ip",StringType(), nullable=True),
                StructField("screen_nm",StringType(), nullable=True),
                StructField("add_datetime",StringType(), nullable=True)
            ]
        )

        df = spark.createDataFrame(lst, schema)
        
        print(f"doc 총 건수: {cnt}")
        print("******************* (ES → lst) FIN *******************") 

        df = sc.parallelize(lst).map(lambda x: json.dumps(x))
        df = spark.read.json(df)
        df.printSchema()

        df.show()

        df.createOrReplaceTempView("df_unionbyname")


        """레드시프트 칼럼 순서에 맞게 df 정렬 / 모든칼럼 nvl처리"""
        select_df = spark.sql(f"""
        select
            {param_month} as use_dt
            ,nvl(_id, '') as _id
            ,nvl(emp_no,'') as emp_no      
            ,nvl(emp_nm,'') as emp_nm   
            ,nvl(emp_dept_cd,'') as emp_dept_cd
            ,nvl(emp_dept_nm,'') as emp_dept_nm    
            ,nvl(info_jobty_cd,'') as info_jobty_cd     
            ,nvl(info_jbln_cd,'') as info_jbln_cd   
            ,nvl(info_dty_cd,'') as info_dty_cd      
            ,nvl(jobcl_cd,'') as jobcl_cd
            ,nvl(private_ip,'') as private_ip
            ,case
                when screen_nm like '- 직원%' then
                    case
                        when screen_nm like '%나의 정보' then '직원 나의정보'
                        when screen_nm like '%질문과 답변' then '직원 질문과답변'
                        when screen_nm like '%관심직무 관리' then '직원 관심직무관리'
                        when screen_nm like '%부서상세정보' then '직원 부서상세정보'
                        else substring(replace(nvl(screen_nm,''), '> ', ''), 3)
                    end
                when screen_nm like '직원%' then
                    case
                        when screen_nm like '%부서 상세정보' then '직원 부서상세정보'
                        when screen_nm like '%=직무 상세정보' then '직원 직무상세정보'
                        when screen_nm like '%직무탐색' then '직원 직무탐색'
                        else screen_nm
                    end
                when screen_nm like '- 본부부서%' then
                    case
                        when screen_nm like '%관심직원 관리' then '본부부서 나의인재스크랩'
                        when screen_nm like '%부서정보 수정' then '직원 부서정보수정'
                        else screen_nm
                    end
                when screen_nm like '본부부서%' then
                    case
                        when screen_nm like '%관심직원 관리' then '본부부서 나의인재스크랩'
                        when screen_nm like '%부서정보 수정' then '직원 부서정보수정'
                        else screen_nm
                    end
                when screen_nm like '공통%' then
                    case
                        when screen_nm like '%공지/자료실 리스트' then '영업점 공지/자료실'
                        when screen_nm like '%공지/자료실 리스트 상세' then '영업점 공지/자료실/상세페이지'
                        else replace(nvl(screen_nm,''), '공통 > ', '영업점')
                    end
                when screen_nm = '영업점 부서 정보' then '영업점 인원현황'
                when screen_nm = '로그인' then '본부부서 로그인'
                when screen_nm like ' - 직원%' then substring(replace(nvl(screen_nm,''), '> ', ''), 4)
                else screen_nm
            end screen_nm -- 이상 데이터 통일                                                         
            ,to_timestamp(substring(nvl(add_datetime,'99991231 00:00:00'),1,4)||'-'||substring(nvl(add_datetime,'99991231 00:00:00'),5,2)||'-'||substring(nvl(add_datetime,'99991231 00:00:00'),7,2)||substring(nvl(add_datetime,'99991231 00:00:00'),9,9)) as add_datetime
        from df_unionbyname
        """)

        print("checkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkking")
        select_df.show(3)

        select_df = select_df.withColumn("platform_dt", current_timestamp() + expr("INTERVAL 9 HOURS"))
        select_df.printSchema()
        
        """ Redshift 테이블에 적재
            DELETE 전체 건 -> APPEND 전체 건
        """
        if total_hits == select_df.count():
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