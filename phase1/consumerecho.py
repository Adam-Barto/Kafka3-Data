from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'bank-customer-events',
     bootstrap_servers=['127.0.0.1:9092'],
     value_deserializer=lambda m: loads(m.decode('ascii')))

for message in consumer:
    print(message)
    message = message.value
    print('{} found'.format(message))
