import json
import subprocess
import time

from pprint import pprint
from typing import Dict, Tuple
from pyspark.sql.types import StructType, StructField, StringType

print(f"Success glue Python library Import {__file__}")

"""
작성방법
def template_function(param1: str, param2: str) -> str:
    템플릿 함수입니다.
    Args:
        param1: ~
        param2: ~
    Returns:
        ~ 반환합니다.
    Raises:
        AttributeError: 예외 설명
    Examples:

    return template_function
"""

def get_environment(job_name):
    """
    job name에서 환경변수를 가져와서 리턴합니다.
    Args:
        job_name: Glue Job name
    Returns:
        env
        example: dev or stg or prd
    Raises:
        ValueError
    Examples:
        job_name = 'BTEsEmpCenterIndex_dev'
        env = get_environment(job_name)
    """    
    try:
        env_list = ["dev", "stg", "prd"]

        if job_name == '' or job_name == False:
            raise ValueError(f"job_name is not found: {job_name}")
        if len(job_name.split("_")) < 2:
            raise ValueError(f"job_name length is not 2: {job_name}")
        if job_name.split("_")[1].lower() not in env_list:
            raise ValueError(f"env not exists: {job_name}, {env_list}")
        
        env = job_name.split("_")[1].lower()
    except Exception as e:
        print(f"raise exception: {e}")
        raise e

    return env


def get_json_from_s3(s3_client, Bucket, Key) -> Dict:
    """
    S3 저장되어 있는 json 파일을 읽어서 딕셔너리 형태로 반환합니다.
    단, 파일이 UTF-8로 작성되어 있어야 정상적으로 파일을 읽을 수 있음.
    Args:
        s3_client: boto3 s3 클라이언트
        Bucket: 버킷명
        Key: 오브젝트 키
    Returns:
        data_dict
    Raises:
        None
    Examples:
        import boto3
        s3_client = boto3.client("s3")
        bucket_cfg = "hr-rcmd-dev"
        key_cfg = "glue/config/hrmart_cfg_dev.json"
        test_dict = get_json_from_s3(s3_client=s3_client, Bucket=bucket_cfg, Key=key_cfg)
    """            
    try:
        data = s3_client.get_object(Bucket=Bucket, Key=Key)
        json_text_bytes = data['Body'].read().decode('utf-8')
        data_dict = json.loads(json_text_bytes)
    except Exception as e:
        print(f"Error Occurred get_json_from_s3: {e}")
        raise e
    return data_dict


def get_file_from_s3(s3_client, Bucket, Key) -> Dict:
    """ 
    S3 파일을 읽어서 utf-8 디코딩한 데이터를 반환합니다.
    단, 파일이 UTF-8로 작성되어 있어야 정상적으로 파일을 읽을 수 있음.
    Args:
        s3_client: boto3 s3 클라이언트
        Bucket: 버킷명
        Key: 오브젝트 키
    Returns:
        data_dict
    Raises:
        None
    Example:
        import boto3
        s3_client = boto3.client("s3")    
        bucket = "hr-rcmd-stg"
        key_sql = "redshift/sql/casting_query/inst1_tskxban01.sql"
        sql_file = get_file_from_s3(s3_client, bucket, key_sql)
        print(sql_file)
        print(type(sql_file)) # str
    """
    try:
        data = s3_client.get_object(Bucket=Bucket, Key=Key)
        file = data['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error Occurred get_file_from_s3: {e}")
        raise e
    return file


def make_path(*args):
    """
    가변인자를 받아서 패스를 만듭니다.
    Args:
        *args: 파일 디렉토리 또는 파일명
    Returns:
        문자열 패스
    Raises:
        None
    Examples:
        temp_path = make_path(bucket_name, folder_hrmart, db2_schema1, tbl)
         dir_write = f"s3://{temp_path}/"
    """        
    return "/".join(map(lambda x: str(x).strip('/'), args))


def run_cmd_ls(source_path):
    """
    - aws s3 ls <source>
    aws cli ls 명령어를 사용하여 디렉토리의 파일리스트 또는 파일 정보를 읽습니다.
    Args:
        source_path: S3 원본 파일경로
    Returns:
        상태코드, 파일명
    Raises:
        None
    Examples:
        res_status_code, file_exists_name = run_cmd_ls(dir_write)
    """
    cmd_ls = f"aws s3 ls {source_path}"
    res = subprocess.run(args=cmd_ls, shell=True, capture_output=True, encoding="utf-8")
    print(f"cmd:{cmd_ls}")
    print(res)
    if res.returncode == 0:
        return (res.returncode, res.stdout.split()[-1])
    else:
        return (res.returncode, None)


def run_cmd_move(source_path, target_path):
    """
    - aws s3 mv <source> <target>
    aws cli mv 명령어를 사용하여 파일 이동 및 파일 이름 변경합니다.
    Args:
        source_path: S3 원본 파일경로
        target_path: S3 타겟 파일경로
    Returns:
        상태코드
    Raises:
        None
    Examples:
        res_status_code2 = run_cmd_move(exists_file_path, dir_backup)
    """
    cmd_mv = f"aws s3 mv {source_path} {target_path}"
    res  = subprocess.run(args=cmd_mv, shell=True, capture_output=True, encoding="utf-8")
    print(f"cmd:{cmd_mv}")
    print(res)
    return res.returncode


def run_cmd_copy(source_path, target_path):
    """
    - aws s3 cp <source> <target>
    aws cli cp 명령어를 사용하여 파일 이동 및 파일 복사합니다
    Args:
        source_path: S3 원본 파일경로
        target_path: S3 타겟 파일경로
    Returns:
        상태코드
    Raises:
        None
    Examples:
    """
    cmd_cp = f"aws s3 cp {source_path} {target_path}"
    res  = subprocess.run(args=cmd_cp, shell=True, capture_output=True, encoding="utf-8")
    print(f"cmd:{cmd_cp}")
    print(res)
    return res.returncode


def run_cmd_rm(target_path):
    """
    - aws s3 rm <target>
    aws cli ls 명령어를 사용하여 디렉토리의 파일리스트 또는 파일 정보를 읽습니다.
    Args:
        str_path: S3 원본 파일경로
    Returns:
        상태코드, 파일명
    Raises:
        None
    Examples:
        res_status_code = run_cmd_rm(dir_write)
    """
    cmd_rm = f"aws s3 rm {target_path}"
    res = subprocess.run(args=cmd_rm, shell=True, capture_output=True, encoding="utf-8")
    print(f"cmd:{cmd_rm}")
    print(res)
    if res.returncode == 0:
        return (res.returncode, res.stdout.split()[-1])
    else:
        return (res.returncode, None)    


def get_es_client(path_ca_certs, es_username, es_password, env, request_timeout=300, max_retries=3, retry_on_timeout=True):
    """
        EC2 Elasticsearch 클라이언트를 반환합니다.
        env에 따라서 ES hostname 설정 커넥션 생성합니다.
    Args:
        path_ca_certs: glue linux내의 es cert 위치
        es_username: es유저명
        es_password: es패스워드
        env: 환경(dev, stg, prd)
        request_timeout: 시간초과
        max_retries: 최대 시도횟수
        retry_on_timeout: 재시도 시간초과
    Returns:
        ES 클라이언트
    Examples:
        es = get_es_client(
            path_ca_certs="./http_ca.crt",
            es_username=es_cfg.get("es_username"),
            es_password=es_cfg.get("es_password"),
            env="dev"
        )
    """
    from elasticsearch import Elasticsearch
    
    try:
        NODES = []
        hostname = []
        env = env.lower()

        if env=="dev":
            hostname.append("https://ip-10-141-42-77.ap-northeast-2.compute.internal:9200")
        elif env=="stg":
            hostname.append("https://ip-10-141-43-66.ap-northeast-2.compute.internal:9200")
        elif env=="prd":
            hostname.append("https://ip-10-140-42-100.ap-northeast-2.compute.internal:9200")
            hostname.append("https://ip-10-140-42-105.ap-northeast-2.compute.internal:9200")
            hostname.append("https://ip-10-140-42-108.ap-northeast-2.compute.internal:9200")
            hostname.append("https://ip-10-140-42-112.ap-northeast-2.compute.internal:9200")
            hostname.append("https://ip-10-140-43-18.ap-northeast-2.compute.internal:9200")
            hostname.append("https://ip-10-140-43-46.ap-northeast-2.compute.internal:9200")
            hostname.append("https://ip-10-140-43-71.ap-northeast-2.compute.internal:9200")
            hostname.append("https://ip-10-140-43-111.ap-northeast-2.compute.internal:9200")

        NODES.extend(hostname)

        es = Elasticsearch(
                NODES,
                http_compress=True,
                verify_certs=True,
                ca_certs=path_ca_certs,
                basic_auth=(es_username, es_password),
                request_timeout=request_timeout,
                max_retries=max_retries,
                retry_on_timeout=retry_on_timeout
        )
        resp = es.info()
        pprint(f"resp: {resp}")
        pprint(f"es: {es}")
    except Exception as e:
        pprint("Failure Elasticsearch Connection")
        raise e
    else:
        pprint("Success Elasticsearch Connection")
    return es



def set_nullable_for_columns(df, nullable):
    """
    spark dataframe colum nullable = false 또는 true로 변환 가능한 함수
    Args:
        df: 데이터프레임.
        nullable: True or False
    Returns:
        new_schema: 변환된 데이터 프레임
    Raises:
        Exception
    Examples:
        df_pre = spark.read.schema(set_nullable_for_columns(df, True))\
            .format("com.databricks.spark.redshift") \
            .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}") \
            .option("query", data_sql) \
            .option("tempdir", red_tmp_dir)\
            .option("aws_iam_role", red_iam_role)\
            .load()
    """
    try:
        df_schema_convert = StructType(
            [StructField(f.name, f.dataType, nullable, f.metadata)
                if (isinstance(f.dataType, StringType)) 
                else StructField(f.name, f.dataType, nullable, f.metadata) 
                    for f in df.schema.fields]
        )
    except Exception as e:
        print(f"Error Occurred: {e}")
        raise e
    
    return df_schema_convert


def run_execute_statement(red_client, red_cluster_id, red_database, red_secretarn, sql_statement, with_event=True) -> str:
    """
    Redshift Query 실행하여 query_id 반환합니다.
    """
    try:
        api_response = red_client.execute_statement(
            ClusterIdentifier=red_cluster_id,
            Database=red_database,
            SecretArn=red_secretarn,
            Sql=sql_statement,
            WithEvent=with_event
        )
        query_id = api_response["Id"]
    except Exception as e:
        print(f"Raise Exception: {e}")
        raise e
    return query_id


def get_describe_statement(red_client, query_id) -> str:
    """
    Redshift의 query_id값의 Status 값 반환합니다.
    Status: SUBMITTED | PICKED | STARTED | FINISHED | ABORTED | FAILED | ALL
    """
    try:
        desc = red_client.describe_statement(Id=query_id)
        status = desc["Status"]
        return status.strip('"')
    except Exception as e:
        print(f"Raise Exception: {e}")
        raise e        
    

def get_run_query(red_client, red_cluster_id, red_database, red_secretarn, query) -> Tuple[str, str]:
    """
    redshift에 query 실행하고 query 상태값과 query_id를 반환합니다.
    """
    try:
        query_id = run_execute_statement(red_client, red_cluster_id, red_database, red_secretarn, query)
        print(f"query_id: {query_id}")

        while(1):
            status = get_describe_statement(red_client, query_id)
            print(status)
            
            if status in ["FINISHED"]:
                print(f"break status:: {status}")
                break
            if status in ["ABORTED", "FAILED"]:
                print(f"raise Exception status: {status}")
                raise Exception

            time.sleep(5)

        return status, query_id
    except Exception as e:
        print(f"Raise Exception: {e}")
        raise e


def get_statement_result(red_client, query_id) -> Dict:
    """
    Redshift Query result 값을 반환합니다.
    """
    try:
        response = red_client.get_statement_result(Id=query_id)
        return response
    except Exception as e:
        print(f"Raise Exception: {e}")
        raise e


def insert_log_table(red_log_table, log_data: Dict, red_client, red_cluster_id, red_database, red_secretarn):
    """
    로그테이블 적재하는 공통함수 입니다.
    1. 글루 스크립트에서 default log_data 값 설정
    2. try_catch_excpet 에서 try, except 각각에서 값 설정
    3. inser query 문자열 생성
    4. redshift query 실행
    """
    try:
        print(f"log_data: {log_data}")
        
        query = f"""
        INSERT INTO {red_log_table} (bat_dt, bat_req_tm, job_nm, taget_tbl_nm, cretn_cnt, success_yn, error_msg, job_run_id, platform_dt) 
        VALUES ('{log_data.get("bat_dt")}', '{log_data.get("bat_req_tm")}', '{log_data.get("job_nm")}', '{log_data.get("taget_tbl_nm")}', {log_data.get("cretn_cnt")}, '{log_data.get("success_yn")}', '{log_data.get("error_msg")}', '{log_data.get("job_run_id")}', '{log_data.get("platform_dt")}')
        """
        print(f"query: {query}")

        status, query_id = get_run_query(red_client, red_cluster_id, red_database, red_secretarn, query)
        print(f"status: {status}, query_id: {query_id}")
    except Exception as e:
        print(e)
        raise e

