from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda m: dumps(m).encode('ascii')
)

for e in range(1000):
    data = {'number' : e}
    print(data)
    producer.send('test', value=data)
    sleep(5)
