import os
import openai
import json
import re
from elasticsearch import Elasticsearch
from django.shortcuts import render

# Elasticsearch 호스트 및 포트 설정
host = "https://imguru2.iptime.org:49202"
username = "elastic"
password = "elastic"

openai_api_key = "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"
openai.api_key = openai_api_key
mapping_dir = "./mapping/"

with open(mapping_dir + "index.json", "r", encoding="utf-8") as f:
    json_data = json.load(f)

index_mapping = json.dumps(json_data, ensure_ascii=False)

with open(mapping_dir + "sample.json", "r", encoding="utf-8") as f:
    json_data = json.load(f)

emp_mapping = json.dumps(json_data, ensure_ascii=False)

# Elasticsearch 클라이언트 초기화
client = Elasticsearch(
    hosts=host,
    http_auth=(username, password),
    verify_certs=False
)

def reqChatGPT(request):
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        temperature=0,
        max_tokens=500,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0,
        messages=[ 
            {"role": "assistant", "content": "인덱스정보" + index_mapping},
            {"role": "assistant", "content": "필드 맵핑" + emp_mapping},
            {"role": "user", "content": "엘라스틱 인덱스 assistant로 정보 학습"},
            {"role": "user", "content": request},
            {"role": "user", "content": "엘라스틱서치 결과는 보여주지 말고 인덱스명과 쿼리만 짜줘"}
        ],
    )

    response = completion.choices[0].message.content
    
    index_name_start = response.find("인덱스명: ") + len("인덱스명: ")
    index_name_end = response.find("\n", index_name_start)
    index = response[index_name_start:index_name_end]
    
    query_start = response.find("{", response.find("쿼리:")) 
    query_end = response.rfind("}") + 1
    query_string = response[query_start:query_end]

    query = json.loads(query_string)
    
    return index, query

def index(request):
    input_text = ''
    result = ''

    if request.method == 'POST':
        input_text = request.POST.get('input_text', '')
        
        if 'submit' in request.POST:
            try:
                index, query = reqChatGPT(input_text)

                es_response = client.search(index=index, body=query)
                es_data = es_response.body
                json_str = json.dumps(es_data, indent=4, ensure_ascii=False).encode('utf-8')
                
                result = json_str.decode('utf-8')

                # contents = {
                #     "index":index,
                #     "query":query
                # }

                # result = json.dumps(contents, indent=4, ensure_ascii=False)

                # print(type(result))
            except Exception as e:
                print(e)
                result = e
        else:
            pass

    context = {
        'input_text': input_text,
        'result': result,
    }

    return render(request, 'index.html', context)