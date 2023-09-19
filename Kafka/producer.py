from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    enable_idempotence=True,    # 멱등성 활성화
    transactional_id='prod-1'   # 트랜잭션 ID 설정
)

try:
    # 메시지 전송
    producer.send(topic, key=key, value=value)
    producer.flush()

except Exception as e:
    # 예외 처리 로직
    print("메시지 전송 오류:", e)

finally:
    # 프로듀서 종료
    producer.close()