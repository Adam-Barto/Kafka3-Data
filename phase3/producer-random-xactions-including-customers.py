from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random


class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                                      value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit(self):
        data = {'custid': random.randint(50, 56),
                'type': self.depOrWth(),
                'date': int(time.time()),
                'amt': random.randint(10, 101) * 100,
                }
        return data

    def depOrWth(self):
        return 'dep' if (random.randint(0, 2) == 0) else 'wth'

    def gen_bankid(self):
        return random.randint(0, 3)

    def generateRandomXactions(self, n=1000):
        for _ in range(n):
            bank_id = self.gen_bankid()
            data = self.emit()
            print('sent', data, 'Bank Id:', bank_id)
            self.producer.send('bank-customer-new', value=data, partition=bank_id)
            sleep(1)
# Next on the docket is to figure out HOW to view the tables created and recreate the one for bank-customer-new

# kafka-topics --create --bootstrap-server 127.0.0.1:9092 --replication-factor 1 --partitions 4 --topic bank-customer-new
# kafka-topics --bootstrap-server 127.0.0.1:9092 --describe
# kafka-topics --bootstrap-server 127.0.0.1:9092 --topic bank-customer-new --delete

if __name__ == "__main__":
    p = Producer()
    p.generateRandomXactions(n=20)
