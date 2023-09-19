from kafka import KafkaConsumer

# Kafka Consumer 설정
consumer = KafkaConsumer(
    'topic_name',                       # 구독할 Topic 이름
    bootstrap_servers='localhost:9092', # Kafka브로커의 호스트와 포트
    enable_auto_commit=False,           # 커밋 자동 처리 비활성화
    group_id='consumer_group_id',       # Consumer 그룹 ID
    isolation_level='read_committed'    # Isolation Level을 read_committed로 설정
)

try:
    for message in consumer:
        process_message(message) # 메시지 처리 로직
        consumer.commit() # 커밋 수행
except:
    consumer.close()