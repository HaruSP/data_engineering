from confluent_kafka import Producer, Consumer, KafkaError

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'my-consumer-group',
    'enable.auto.commit': False,
    'isolation.level': 'read_committed', # 컨슈머가 트랜잭션으로 커밋된 메시지만 읽을 수 있는 옵션
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)

topic_name = 'test'
consumer.subscribe([topic_name])

try:
    while True:
        msg = consumer.poll(1.0)  # 1초마다 메시지 폴링

        if msg is None:
            print("메시지가 도착하지 않았습니다.")
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Reached end of partition, offset: {msg.offset()}')
            else:
                print(f'Error occurred: {msg.error().str()}')
        else:
            # 메시지 처리 로직
            print(f'Received message: {msg.value().decode()}')
except KeyboardInterrupt:
    consumer.close()