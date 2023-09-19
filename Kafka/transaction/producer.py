from confluent_kafka import Producer, Consumer, KafkaError

bootstrap_servers = "data3.cdh.com:19092"
topic = "test_jhpark"

producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'transactional.id': 'my-transaction-id',  	# 트랜잭션 ID 설정
    'enable.idempotence': True					# 멱등성(idempotence)
}
producer = Producer(producer_config)

producer.init_transactions() # 트랜잭션 초기화
producer.begin_transaction() # 트랜잭션 시작

try:
	producer.produce(topic=topic_name, key='key1', value='message1'.encode('utf-8')) # Process
	
	producer.commit_transaction() # 트랜잭션 커밋
except:
	producer.abort_transaction() # 롤백
finally:
    producer.close() # 프로듀서 종료