import os
import openai
import json
import re
from elasticsearch import Elasticsearch
from django.shortcuts import render

# Elasticsearch 호스트 및 포트 설정
host = "https://aaaaaa.iptime.org:49202"
username = "elastic"
password = "elastic"

openai_api_key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
openai.api_key = openai_api_key


def jsonData(fileName):
    mapping_dir = "./mapping/"

    with open(mapping_dir + fileName, "r", encoding="utf-8") as f:
        json_data = json.load(f)

    returnData = json.dumps(json_data, ensure_ascii=False)

    return returnData


client = Elasticsearch(hosts=host, http_auth=(username, password), verify_certs=False)

trin_index = """
{인덱스설명:직원 인덱스,index:hr_emp_center_20230301,type:index}
"""
trin_data = """
{이름:취미,필드명:array_ki_hbby,속성:array,데이터:[콘서트관람]}
{이름:나의보유스킬,필드명:array_ki_hold_skll,속성:array,데이터:[경쟁사 분석]}
{이름:하고싶은 업무,필드명:array_ki_prefer_nm,속성:array,데이터:[IT전략계획]}
{이름:현소속전입일,필드명:date_info_crnt_dvsn_str_dt,속성:date,데이터:[99991231]}
{이름:현직급승격일,필드명:date_info_curnt_jobcl_prss_dd,속성:date,데이터:[99991231]}
{이름:입행일자,필드명:date_info_enbnk_dt,속성:date,데이터:[99991231]}
{이름:최초입행일자,필드명:date_info_frst_enbnk_dt,속성:date,데이터:[99991231]}
{이름:직원번호,필드명:emp_no,속성:string,데이터:[1500391]}
{이름:부서명,필드명:info_brn_name,속성:string,데이터:[HR부]}
{이름:직무명,필드명:info_dty_nm,속성:string,데이터:[PB고객담당]}
{이름:직원명,필드명:info_emp_nm_kr,속성:string,데이터:[아**]}
{이름:성별,필드명:info_gndr_dstic_nm,속성:string,데이터:[여자, 남자]}
{이름:자택우편주소,필드명:info_home_zip_addr,속성:string,데이터:[서울]}
{이름:직급명,필드명:info_jobcl_nm,속성:string,데이터:[L2]}
{이름:직위명,필드명:info_jobtl_nm,속성:string,데이터:[팀장]}
{이름:직군명,필드명:info_jobty_nm,속성:string,데이터:[영업]}
{이름:최종학과명,필드명:info_last_crculm_nm,속성:string,데이터:[경영학]}
{이름:직무이력,필드명:nested_duty,속성:nested,데이터:[{이름:직원상태구분명,필드명:duty_emp_stus_dstic_nm,속성:string,데이터:[현업]},{이름:직무 시작일자,필드명:date_duty_str_dt,속성:date,데이터:[20080820]},이름:직무수행 부서명,필드명:duty_brn_nm,타입:string,데이터:[HR부]},{이름:직무 종료일자,필드명:date_duty_end_dt,속성:date,데이터:[20100726]},{이름:직무이력 직무명,필드명:duty_dty_nm,속성:string,데이터:[PB고객담당]},{이름:직무이력 직급,필드명:duty_jobcl_nm,속성:string,데이터:[L1]},{이름:직무이력 직위,필드명:duty_jobtl_nm,속성:string,데이터:[팀원]}]}
{이름:프로젝트 수행이력,필드명:nested_inqu,속성:nested,데이터:[{이름:프로젝트 주직무명,필드명:inqu_mn_dty_nm,속성:string,데이터:[서버업무개발]},{이름:프로젝트 시작일,필드명:date_inqu_dvlp_str_dt,속성:date,데이터:[20171206]},{이름:프로젝트 수행업무,필드명:inqu_job_desc,속성:string,데이터[SWIFT GPI송금서비스 구축]},{이름:프로젝트명,필드명:inqu_prj_nm,속성:date,데이터:[SWIFT GPI 송금 서비스 구축]},{이름:프로젝트 종료일,필드명:date_inqu_dvlp_end_dt,속성:date,데이터:[20180705]},{이름:프로젝트 부서명,필드명:inqu_dvsn_nm,속성:string,데이터:[외환마케팅부]}]}
{이름:자격증,필드명:nested_lcns,속성:nested,데이터:[{이름:취득일,필드명:date_lcns_qlcr_acqsi_dd,속성:date,데이터:[20030621]},{이름:자격증명,필드명:lcns_qlcr_nm,속성:string,데이터:[손해보험대리점]}]}
{이름:연수,필드명:nested_trin,속성:nested,데이터:[{이름:연수 시작일자,필드명:date_trin_str_dt,속성:date,데이터:[20070910]},{이름:KB과정명,필드명:trin_kb_course_nm,속성:date,데이터:[은행실무종합과정]},{이름:연수 종료일자,필드명:date_trin_end_dt,속성:date,데이터:[20071110]},{이름:차수명,필드명:trin_dgre_nm,속성:string,데이터:[직원실무평가(2007)]},{이름:연수수료여부,필드명:trin_cmplt_yn,속성:string,데이터:[Y]},{이름:연수주관처,필드명:trin_mgmt_cmpy,속성:string,데이터:[인재개발부]},{이름:연수과정명,필드명:trin_course_nm,속성:string,데이터:[직원실무평가(2007)]}]}
"""
query_example = """
{
  "query": {
    "bool": {
      "must": [],
      "filter": [],
      "must_not": [],
      "should": []
    }
  }
}
,"aggs": {}
}"""


def reqChatGPT(request):
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        temperature=0,
        max_tokens=500,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0,
        messages=[
            {"role": "assistant", "content": "Index Infomation" + trin_index},
            {"role": "assistant", "content": "Mapping Infomation" + trin_data},
            {"role": "system", "content": "Query Fomatting" + query_example},
            {"role": "user", "content": "Train Info assistant model."},
            {
                "role": "user",
                "content": "The request should only contain the index name and query without any expected results.",
            },
            {
                "role": "system",
                "content": "Please provide the query in the desired Query Fomatting.",
            },
            {
                "role": "system",
                "content": "You need to remove ['.keyword' and '.text'] from the field names.",
            },
            {
                "role": "user",
                "content": "The request should only contain the index name and query without any expected results.",
            },
            {
                "role": "user",
                "content": "Only include the index name and query without any accompanying explanation.",
            },
            {"role": "user", "content": "Please provide the exact query"},
            {
                "role": "system",
                "content": "You need to remove ['.keyword' and '.text'] from the field names.",
            },
            {
                "role": "user",
                "content": 'Please provide the answer in the format of {"인덱스": "", "쿼리": "", "타입":"차트", "검색", "차트":종류}.',
            },
            {"role": "user", "content": "Please provide only one query."},
            {
                "role": "user",
                "content": "Check if it is a chart type or a search result display.",
            },
            {
                "role": "user",
                "content": 'Write the bool query using the format {"query": {"bool": {}}}',
            },
            {
                "role": "user",
                "content": 'type:nested is use format {"path", "query"}.',
            },
            {
                "role": "user",
                "content": "Please avoid using fields that do not exist.",
            },
            {"role": "user", "content": request},
        ],
    )

    response = completion.choices[0].message.content.replace(".keyword", "")

    data = json.loads(response)
    json_data = json.dumps(data, indent=4, ensure_ascii=False)

    response = completion.choices[0].message.content
    print(
        "====================================== response ======================================\n",
        response,
    )
    print(
        "====================================== json_data ======================================\n",
        json_data,
    )
    print(
        "====================================== tempDict ======================================\n",
        data,
    )

    index = data["인덱스"]
    query = data["쿼리"]
    searchType = data["타입"]

    return index, query, searchType


def returnQuery(inputText):
    qry = {
        "query": {
            "query_string": {
                "fields": ["intro_question", "recommend_question", "keyword_question"],
                "query": inputText,
            }
        }
    }

    return qry


def empInfoQuery(empNo):
    qry = {
        "_source": [
            "emp_no",
            "info_emp_nm_kr",
            "info_gndr_dstic_nm",
            "info_home_zip_addr",
            "info_last_crculm_nm",
            "info_jobtl_nm",
            "info_dsgt_nm",
            "info_drct_grp_nm",
            "info_drct_hdqt_nm",
            "info_brn_name",
            "info_jobty_nm",
            "info_jbln_nm",
            "info_dty_nm",
            "array_srch_duty_dty_nm",
            "array_srch_lngt_lngts_exam_nm",
            "array_srch_trin_dgre_nm",
            "array_srch_lcns_qlcr_nm",
            "array_srch_inqu_prj_nm",
        ],
        "query": {"match": {"emp_no": empNo}},
    }

    return qry


def index(request):
    input_text = ""
    result = ""
    context = {}

    # if request.method == "POST":
    #     input_text = request.POST.get("input_text", "")

    client_ip = request.META.get("REMOTE_ADDR")
    print(client_ip)

    return render(request, "index.html", context)


def demo1_view(request):
    input_text = ""
    result = ""

    if request.method == "POST":
        input_text = request.POST.get("input_text", "")

        if "option3" in request.POST:
            try:
                es_response = client.search(
                    index="chatgpt_question_answer", body=returnQuery(input_text)
                )

                if len(es_response["hits"]["hits"]) > 0:
                    empInfo = client.search(
                        index="hr_emp_center_20230301",
                        body=empInfoQuery(
                            es_response["hits"]["hits"][0]["_source"]["emp_no"]
                        ),
                    )

                    es_response["hits"]["hits"][0]["_source"]["emp_no"] = empInfo[
                        "hits"
                    ]["hits"][0]["_source"]["emp_no"]
                    es_response["hits"]["hits"][0]["_source"][
                        "info_emp_nm_kr"
                    ] = empInfo["hits"]["hits"][0]["_source"]["info_emp_nm_kr"]
                    es_response["hits"]["hits"][0]["_source"][
                        "info_gndr_dstic_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"]["info_gndr_dstic_nm"]
                    es_response["hits"]["hits"][0]["_source"][
                        "info_home_zip_addr"
                    ] = empInfo["hits"]["hits"][0]["_source"]["info_home_zip_addr"]
                    es_response["hits"]["hits"][0]["_source"][
                        "info_last_crculm_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"]["info_last_crculm_nm"]
                    es_response["hits"]["hits"][0]["_source"][
                        "info_jobtl_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"]["info_jobtl_nm"]
                    es_response["hits"]["hits"][0]["_source"]["info_dsgt_nm"] = empInfo[
                        "hits"
                    ]["hits"][0]["_source"]["info_dsgt_nm"]
                    es_response["hits"]["hits"][0]["_source"][
                        "info_drct_grp_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"]["info_drct_grp_nm"]
                    es_response["hits"]["hits"][0]["_source"][
                        "info_drct_hdqt_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"]["info_drct_hdqt_nm"]
                    es_response["hits"]["hits"][0]["_source"][
                        "info_brn_name"
                    ] = empInfo["hits"]["hits"][0]["_source"]["info_brn_name"]
                    es_response["hits"]["hits"][0]["_source"][
                        "info_jobty_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"]["info_jobty_nm"]
                    es_response["hits"]["hits"][0]["_source"]["info_jbln_nm"] = empInfo[
                        "hits"
                    ]["hits"][0]["_source"]["info_jbln_nm"]
                    es_response["hits"]["hits"][0]["_source"]["info_dty_nm"] = empInfo[
                        "hits"
                    ]["hits"][0]["_source"]["info_dty_nm"]
                    es_response["hits"]["hits"][0]["_source"][
                        "array_srch_duty_dty_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"]["array_srch_duty_dty_nm"]
                    es_response["hits"]["hits"][0]["_source"][
                        "array_srch_lngt_lngts_exam_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"][
                        "array_srch_lngt_lngts_exam_nm"
                    ]
                    es_response["hits"]["hits"][0]["_source"][
                        "array_srch_trin_dgre_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"]["array_srch_trin_dgre_nm"]
                    es_response["hits"]["hits"][0]["_source"][
                        "array_srch_lcns_qlcr_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"]["array_srch_lcns_qlcr_nm"]
                    es_response["hits"]["hits"][0]["_source"][
                        "array_srch_inqu_prj_nm"
                    ] = empInfo["hits"]["hits"][0]["_source"]["array_srch_inqu_prj_nm"]
                    es_response["hits"]["hits"][0]["_source"]["type"] = "option3"

                    result = json.dumps(
                        es_response["hits"]["hits"][0]["_source"],
                        indent=4,
                        ensure_ascii=False,
                    )
                else:
                    result = json.dumps({"type": "option3"})
            except Exception as e:
                print(e)
                result = e
        else:
            pass

    context = {
        "input_text": input_text,
        "result": result,
    }

    return render(request, "demo1.html", context)


def demo2_view(request):
    input_text = ""
    result = ""

    if request.method == "POST":
        input_text = request.POST.get("input_text", "")

        if "option4" in request.POST:
            try:
                index, query, searchType = reqChatGPT(input_text)

                es_response = client.search(index=index, body=query)
                es_data = es_response.body
                tempResult = {
                    "result": es_data,
                    "type": "option4",
                    "index": index,
                    "query": query,
                    "searchType": searchType,
                }

                result = (
                    json.dumps(tempResult, indent=4, ensure_ascii=False)
                    .encode("utf-8")
                    .decode("utf-8")
                )

                print(result)
            except Exception as e:
                print("Error : ", e)

                error = {"error": str(e), "type": "option4"}
                result = json.dumps(error)
        else:
            pass

    context = {
        "input_text": input_text,
        "result": result,
    }

    return render(request, "demo2.html", context)