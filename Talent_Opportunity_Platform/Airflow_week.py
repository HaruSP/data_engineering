from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException

from datetime import datetime, timedelta
from typing import Dict

import pendulum
import time
import os,json,boto3,re


def get_env_dag(dag_name):
    dag_env = dag_name.split("-")[0].lower()
    if dag_env not in ['dev', 'stg', 'prd']:
        raise ValueError(f"env not found: {dag_name}")
    return dag_env


local_tz = pendulum.timezone("Asia/Seoul")
dag_name = os.path.basename(__file__).replace(".py", "")

"""A/F 시작시간 01:00 > 데이터 생성날짜 기준은 1일전 데이터"""
date = datetime.utcnow() + timedelta(hours=9) - timedelta(days=1)
date_today = datetime.utcnow() + timedelta(hours=9)
weekday = date_today.weekday() # (Mon, 0), (Tue, 1), (Wed, 2), (Thu, 3), (Fri, 4), (Sat, 5), (Sun, 6)
date_ymd = date.strftime("%Y%m%d")

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

env = get_env_dag(dag_name)

bucket = f"hr-rcmd-{env}"
key_t0_tbl = "config/t0_hrmart_tbl.json"
key_t1_tbl = "config/t1_domain_tbl.json"


""" DAG default_args 설정 """
default_args = {
    "owner": "hrptr",
    "wait_for_downstream":False,
    "depends_on_past": False,
    "start_date": datetime(2022,10,13, tzinfo=local_tz),
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def get_obj_from_s3(s3_client, Bucket, Key) -> Dict:
    """
    S3 파일을 딕셔너리 형태로 반환합니다. 
    obj = get_obj_from_s3(s3, bucket, key)
    for k,v in obj.items():
        print(f"k: {k}, v: {v}")
    """
    try:
        data = s3_client.get_object(Bucket=Bucket, Key=Key)
        json_text_bytes = data['Body'].read().decode('utf-8')
        data_dict = json.loads(json_text_bytes)
    except Exception as e:
        print(f"Error Occurred: {e}")
        raise e
    return data_dict


def get_hrmart_tbl_lst(key):
    """
    Args:
        path_filename: S3 파일 경로
    Returns:
        inst1_tbls, inst2_tbls, inst3_tbls
    Raises:
        Exception
    Examples:
        path_filename = "s3://hr-rcmd-dev/config/t0_hrmart_tbl.json"
        inst1, inst2, inst3 = get_hrmart_tbl_lst(path_filename)
    """ 
    inst1_tbls = []
    inst2_tbls = []
    inst3_tbls = []
    try:
        obj = get_obj_from_s3(s3_client, bucket, key)
        for k,v in obj.items():
            if re.search("inst1", k):
                inst1_tbls.append(v)
            elif re.search("inst2", k):
                inst2_tbls.append(v)
            else:
                inst3_tbls.append(v)
        
        return inst1_tbls, inst2_tbls, inst3_tbls
    except Exception as e:
        print(f"Occured Error: {e}")
        raise e


def get_t1_sql_files(key):
    """
    Args:
        path: S3 파일 경로
    Returns:
        t1_emp_sql_files, t1_dvsn_sql_files, t1_duty_sql_files
    Raises:
        Exception
    Examples:
        path = "s3://hr-rcmd-dev/glue/config/t1_domain_tbl.json"
        inst1, inst2, inst3 = get_hrmart_tbl_lst(path)
    """ 
    t1_emp_sql_files  = []
    t1_duty_sql_files = []
    t1_dvsn_sql_files = []
    t1_pbof_sql_files = []
    
    try:
        obj = get_obj_from_s3(s3_client, bucket, key)
        for k,v in obj.items():
            if re.search("EMP", k):
                t1_emp_sql_files.append(v)
            elif re.search("DUTY", k):
                t1_duty_sql_files.append(v)
            elif re.search("DVSN", k):
                t1_dvsn_sql_files.append(v)
            else:
                t1_pbof_sql_files.append(v)
    
        return t1_emp_sql_files, t1_duty_sql_files, t1_dvsn_sql_files, t1_pbof_sql_files
    except Exception as e:
        print(f"Occured Error: {e}")
        raise e
    

def get_task_by_schedule_type(task_id, job_name, job_arguments:Dict={}, numberofworkers=2, WorkerType='G.1X'):
    """
    Glue Job 배치 스케쥴 타입에 따라서 RUN 또는 SKIP 태스크 생성
    테스트용 task 생성 방법
        # task = test_dummy_task(task_id=task_id, job_name=job_name, schedule_type=schedule_type)
    """
    try:
        task = None
        schedule_type = batch_schedule_type_by_job_name.get(job_name)
        # task = test_dummy_task(task_id=task_id, job_name=job_name, schedule_type=schedule_type)

        if schedule_type == 'D':
            task = make_task_glue_job(task_id=task_id, job_name=job_name, job_arguments=job_arguments, numberofworkers=numberofworkers, WorkerType=WorkerType)
        elif schedule_type == 'W':
            if weekday == 0:
                task = make_task_glue_job(task_id=task_id, job_name=job_name, job_arguments=job_arguments, numberofworkers=numberofworkers, WorkerType=WorkerType)
            else:
                task = make_task_skip(task_id)
    except Exception as e:
        raise e

    return task


def make_task_glue_job(task_id, job_name, job_arguments:Dict, numberofworkers=2, WorkerType='G.1X', trigger_rule=TriggerRule.NONE_FAILED):
    try:
        task_glue_job = PythonOperator(
            task_id=task_id,
            python_callable=start_glue_job,
            op_kwargs= {
                "job_name": job_name,
                "arguments":job_arguments,
                "numberofworkers":numberofworkers,
                "WorkerType":WorkerType,
            },
            provide_context=True,
            trigger_rule=trigger_rule,
        )
        
        return task_glue_job
    except Exception as e:
        print(f"Occured Error: {e}")
        raise e


def start_glue_job(job_name, arguments={}, numberofworkers=2, WorkerType="G.1X"):
    """
    glue_client.get_job_run JobRunState 상태메세지 유형
    최초 시작시 RUNNING
    성공시 SUCCEEDED
    실패시 FAILED
    - 'JobRunState' 상태코드 값 N개: 
        'STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'SUCCEEDED'|'FAILED'
        |'TIMEOUT'|'ERROR'|'WAITING',
    """
    print(f"start_glue_job job_name: {job_name}")
    print(f"glue job arguments: {arguments}")
    try:
        res = glue_client.start_job_run(
            JobName=job_name,
            Arguments = arguments,
            WorkerType = WorkerType,
            NumberOfWorkers=numberofworkers
        )
        job_run_id = res.get('JobRunId')
        print(f"job_run_id: {job_run_id}")
        print(f"res\n: {res}")

        while True:
            res_status = glue_client.get_job_run(
                    JobName=job_name,
                    RunId=job_run_id
                )
            status = res_status['JobRun']['JobRunState']        
            print(f"glue job status: {status}")

            if status in ["FAILED","STOPPING","STOPPED","TIMEOUT","ERROR"]:
                raise Exception
            if status != "RUNNING":
                break

            time.sleep(10)

    except Exception as e:
        print(f"raise Exception: {e}")
        raise e
    else:
        print(f"Glue Job Name: {job_name}")
        print(f"Glue Job job_run_id: {job_run_id}")
        print(f"Glue Job status: {res_status['JobRun']['JobRunState']}")


def make_task_skip(task_id):
    task_py = PythonOperator(
        task_id=task_id,
        python_callable=func_airflow_skip_exception,
        provide_context=True,
    )
    return task_py


def func_airflow_skip_exception():
    raise AirflowSkipException


def test_dummy_task(task_id, job_name, schedule_type, trigger_rule=TriggerRule.NONE_FAILED):
    task_py2 = PythonOperator(
        task_id=task_id,
        python_callable=dummy_func,
        op_kwargs= {
            "job_name": job_name,
            "schedule_type": schedule_type,
        },        
        provide_context=True,
        trigger_rule=trigger_rule,
    )
    return task_py2


def dummy_func(job_name, schedule_type):
    print("Run Dummy Python Operator")
    print(f"job_name: {job_name}")
    print(f"schedule_type: {schedule_type}")
    time.sleep(10)
    print(datetime.now().strftime('%Y-%m-%d'))
    print("End Dummy Python Operator")


""" 적재 대상 테이블 리스트 가져오기 """
schema_inst1, schema_inst2, schema_inst3 = get_hrmart_tbl_lst(key=key_t0_tbl)
t1_emp_sql, t1_duty_sql, t1_dvsn_sql, t1_pbof_sql = get_t1_sql_files(key=key_t1_tbl)

# DAILY JOB_NAME
JOB_BTLoadS3ToT0 = f"BTLoadS3ToT0_{env}"
JOB_BTEsRvrsEmpKeyin = f"BTEsRvrsEmpKeyin_{env}"
JOB_BTLoadRsT1 = f"BTLoadRsT1_{env}"
JOB_BTLoadRsT2Emp = f"BTLoadRsT2Emp_{env}"
JOB_BTEsEmpCenterIndex = f"BTEsEmpCenterIndex_{env}"
JOB_BTEsMngIndex = f"BTEsMngIndex_{env}"
# WEEKLY JOB_NAME
JOB_BTLoadRsT2EmpBefAft = f"BTLoadRsT2EmpBefAft_{env}"
JOB_BTFeatureF01 = f"BTFeatureF01_{env}"
JOB_BTFeatureF02 = f"BTFeatureF02_{env}"
JOB_BTDtyMetrics = f"BTDtyMetrics_{env}"
JOB_BTEsDtyRcmdEmpIndex = f"BTEsDtyRcmdEmpIndex_{env}"

"""
글루 잡 배치 주기 세팅
DAILY -> D
WEEKLY -> W
"""
batch_schedule_type_by_job_name = {
    # DAILY
      JOB_BTLoadS3ToT0 : "D"
    , JOB_BTEsRvrsEmpKeyin : "D"
    , JOB_BTLoadRsT1 : "D"
    , JOB_BTLoadRsT2Emp : "D"
    , JOB_BTEsEmpCenterIndex : "D"
    , JOB_BTEsMngIndex : "D"
    # WEEKLY
    , JOB_BTLoadRsT2EmpBefAft :"D"
    , JOB_BTFeatureF01 : "D"
    , JOB_BTFeatureF02 : "D"
    , JOB_BTDtyMetrics : "D"
    , JOB_BTEsDtyRcmdEmpIndex : "D"
}

""" Glue Job parameter setting"""
job_args_t0_inst1 = {"--param_date": date_ymd, "--param_schema_tbl": ",".join(schema_inst1)}
job_args_t0_inst2 = {"--param_date": date_ymd, "--param_schema_tbl": ",".join(schema_inst2)}
job_args_t0_inst3 = {"--param_date": date_ymd, "--param_schema_tbl": ",".join(schema_inst3)}

job_args_es_keyin_emp  = {"--param_date": date_ymd} 

job_args_t1_emp  =    {"--param_date": date_ymd, "--param_sql_file": ",".join(t1_emp_sql)}

job_args_t2_bef_aft     = {"--param_date": date_ymd, "--param_sql_filename":"T2_BEF_AFT.sql"}

job_args_t2_emp_center  = {"--param_date": date_ymd}

job_args_t2_feat_f01   = {"--param_date": date_ymd}
job_args_t2_feat_f02   = {"--param_date": date_ymd}
job_args_t3_feature    = {"--param_date": date_ymd}

job_args_t4_emp_center   = {"--param_date": date_ymd}
job_args_t4_rcmd_emp     = {"--param_date": date_ymd}

job_args_t5_mng = {"--param_date": date_ymd}


with DAG(
    dag_id=dag_name,
    catchup=False,
    default_args=default_args,
    schedule_interval = '30 7 * * 1-5'
) as dag:

    with TaskGroup(group_id='t0_daily') as t0_daily:
        task_t0_inst1 = get_task_by_schedule_type(task_id="task_t0_s3_hrtpr_inst1", job_name=JOB_BTLoadS3ToT0, job_arguments=job_args_t0_inst1)
        task_t0_inst2 = get_task_by_schedule_type(task_id="task_t0_s3_hrtpr_inst2", job_name=JOB_BTLoadS3ToT0, job_arguments=job_args_t0_inst2)
        task_t0_inst3 = get_task_by_schedule_type(task_id="task_t0_s3_hrtpr_inst3", job_name=JOB_BTLoadS3ToT0, job_arguments=job_args_t0_inst3)

    with TaskGroup(group_id='t1_daily') as t1_daily:
        task_es_emp_keyin  = get_task_by_schedule_type(task_id="task_t1_es_emp_keyin",  job_name=JOB_BTEsRvrsEmpKeyin,  job_arguments=job_args_es_keyin_emp)

        task_t1_emp      = get_task_by_schedule_type(task_id="task_t1_emp",        job_name=JOB_BTLoadRsT1,        job_arguments=job_args_t1_emp)
        
        # Task dependency
        task_t1_emp

    with TaskGroup(group_id="t2") as t2:
        with TaskGroup(group_id='t2_daily') as t2_daily:
            task_t2_emp_center  = get_task_by_schedule_type(task_id="task_t2_emp_center",   job_name=JOB_BTLoadRsT2Emp,       job_arguments=job_args_t2_emp_center, numberofworkers=20)

        with TaskGroup(group_id='t2_weekly') as t2_weekly:
            task_t2_bef_aft     = get_task_by_schedule_type(task_id="task_t2_bef_aft",     job_name=JOB_BTLoadRsT2EmpBefAft, job_arguments=job_args_t2_bef_aft)

        # Inner Task group dependency
        t2_daily >> t2_weekly

    with TaskGroup(group_id='t2t3_weekly_feature') as t2t3_weekly_feature:
        task_t2_feat_f01   = get_task_by_schedule_type(task_id="task_t2_feat_f01",     job_name=JOB_BTFeatureF01,   job_arguments=job_args_t2_feat_f01,   numberofworkers=10)
        task_t2_feat_f02   = get_task_by_schedule_type(task_id="task_t2_feat_f02",     job_name=JOB_BTFeatureF02,   job_arguments=job_args_t2_feat_f02,   numberofworkers=10)

        task_t3_feature    = get_task_by_schedule_type(task_id="task_t3_feature",     job_name=JOB_BTDtyMetrics,   job_arguments=job_args_t3_feature, numberofworkers=10)
        
        # Task dependency
        task_t2_feat_f01 >> task_t3_feature
        task_t2_feat_f02 >> task_t3_feature

    with TaskGroup(group_id='t4_es_index') as t4_es_index:
        with TaskGroup(group_id='t4_daily') as t4_daily:
            task_t4_emp_center   = get_task_by_schedule_type(task_id="task_t4_emp_center",   job_name=JOB_BTEsEmpCenterIndex,   job_arguments=job_args_t4_emp_center)

        with TaskGroup(group_id='t4_weekly') as t4_weekly:
            task_t4_rcmd_emp     = get_task_by_schedule_type(task_id="task_t4_rcmd_emp",     job_name=JOB_BTEsDtyRcmdEmpIndex,  job_arguments=job_args_t4_rcmd_emp, WorkerType="G.2X")
    
    with TaskGroup(group_id='t5') as t5:
            t5_mng       = get_task_by_schedule_type(task_id="task_t5_mng",       job_name=JOB_BTEsMngIndex,       job_arguments=job_args_t5_mng)


# TaskGroup dependency
t0_daily >> t1_daily >> t2 >> t2t3_weekly_feature >> t4_es_index >> t5