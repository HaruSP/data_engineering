import sys
import boto3
import pandas as pd
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

from comlib import read_s3file
from comlib import run_cmd_ls
from comlib import run_cmd_move
from comlib import get_json_from_s3
from comlib import set_nullable_for_columns

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql from SparkSession

params = ["param_date"]
args = getResolvedOptions(sys.argv, params)
print(f"args: {args}")

bucket_name = "dev"

s3_client = boto3_client("s3")
param_date = str(datetime.now().date()).replace('-','')
print(f"param_date: {param_date}")

### redshift info ###
bucket = "rcmd-dev"
s3_key_cfg_name = "path" # 예시

redshift_cfg = get_json_from_s3(s3_client=s3_client, Bucket=bucket, Key=s3_key_cfg_name)

red_database = redshift_cfg.get("red_database")
red_tmp_dir = redshift_cfg.get("red_tmp_dir")
red_iam_role = redshift_cfg.get("red_iam_role")
red_jdbc_url = f"{redshift_cfg.get("red_jdbc_url")/{red_database}}"
red_user = redshift_cfg.get("red_user")
red_password = redshift_cfg.get("red_password")
red_schema = "hrptr"

print(f"bucket: {bucket}")
print(f"sc_key_cfg_name: {s3_key_cfg_name}")
print(f"redshift_cfg: {redshift_cfg}")

spark = SparkSession.builder\
        .appName("app")\
        .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")\
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")\
        .getOrCreate()

def get_es_client():
    NODES = [
            "http-path:9200"
    ]
    try:
        es = Elasticsearch(
            NODES,
            http_compress=True,
            verify_certs=True,
            ca_certs="./http_ca.crt",
            basic_auth=("aaa", "aaa")
        )

        resp = es.info()
        print(resp)
        print("Success ES Connection")
        return es
    except Exception as e:
        raise e
    

### ES 클라이언드 객체 생성 ###
es = get_es_client()

es_query_config = {
    "index": "emp",
    "size": 10,
    "query": {
        "match_all": {
        }
    }
}

res = es.search(
    index=es_query_config.get("index"),
    size=es_query_config.get("size"),
    query=es_query_config("query")
)

lst = []
res_hits = res["hits"]["hits"]
for x in res_hits:
    doc = x.get("_source")
    lst.append(doc)


### Pandas Dataframe ###
df = pd.DataFrame(lst)

### DataFrame 전처리 ###
df.fillna(value="", inplace=True)
df = df.astype("str")

mask = df.applymap(lambda x: x is None or x == "[]" or x == "N/A")
cols = df.columns[(mask).any()]
for col in df[cols]:
    df.loc[mask[col], col] = ''


### 전체 칼럼 ' (Single Quote) -> '' (Double Quote) 치환
df = df.replace({'\'':'"'}, regex=True)

print(df.shape)
print(df.columns)
print(df)
print("="*50)

### Spark DataFrame 생성 ###
df_pre = spark.createDataFrame(df)
df_pre.printSchema()
df_pre.show()

red_target_tb = "hrtpr.es_tbl"

### Redshift 적재 ###
df_pre.white\
        .format("com_databricks.spark.redshift")\
        .option("url", f"{red_jdbc_url}?user={red_user}&password={red_password}")\
        .option("dbtable", red_target_tb)\
        .option("tempdir", red_tmp_dir)\
        .option("aws_iam_role", red_iam_role)\
        .option("preactions", f"delete from {red_target_tb}")\
        .mode("overwrite")\
        .save()

print("Success Write Redshift Table")