-- ABAP 언어로 소스데이터 수집하여 GCP Bigquery에 적재하는 예제
REPORT Z_LOAD_CUSTOMER_DATA_TO_GCP.

DATA: lt_customer_data TYPE TABLE OF customer_table,
      lt_processed_data TYPE TABLE OF processed_customer_table,
      lv_json_payload TYPE string.

* 1. 소스테이블 고객 데이터 수집
SELECT *
  FROM customer_table
  INTO TABLE @lt_customer_data.

* 2. 고객 테이블 전처리
LOOP AT lt_customer_data INTO DATA(ls_customer_data).
  DATA(ls_processed_data) TYPE processed_customer_table.

  ls_processed_data-customer_id = ls_customer_data-customer_id.
  ls_processed_data-customer_name = ls_customer_data-customer_name.
  ls_processed_data-email = ls_customer_data-email.
  ls_processed_data-processed_address = CONCATENATE(ls_customer_data-address_line1, ', ', ls_customer_data-address_line2, ', ', ls_customer_data-city, ', ', ls_customer_data-state, ', ', ls_customer_data-country).
  ls_processed_data-phone_number = ls_customer_data-phone_number.

  APPEND ls_processed_data TO lt_processed_data.
ENDLOOP.

* 3. 고객 데이터를 JSON 포맷으로 변경
lv_json_payload = cl_abap_conv_out_ce=>create( lt_processed_data )->to_json_string( ).

* 4. GCP BigQuery의 HTTP Client 세팅
DATA(lo_http_client) = cl_http_client=>create( ).
lo_http_client->request->set_uri( 'https://www.googleapis.com/bigquery/v2/projects/YOUR_PROJECT_ID/datasets/YOUR_DATASET/tables/YOUR_TABLE_ID/insertAll' ).
lo_http_client->request->set_method( 'POST' ).
lo_http_client->request->set_content_type( 'application/json' ).
lo_http_client->request->set_cdata( lv_json_payload ).

* 5. GCP BigQuery에 데이터 로드
lo_http_client->send( ).

IF lo_http_client->response->status_code = '200'.
  WRITE: 'Data loaded to GCP successfully.'.
ELSE.
  WRITE: 'Failed to load data to GCP.'.
ENDIF.