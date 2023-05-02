from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random


class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                                      value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit(self, cust=55, type="dep"):
        data = {'custid': random.randint(50, 56),
                'type': self.depOrWth(),
                'date': int(time.time()),
                'amt': random.randint(10, 101) * 100,
                }
        return data

    def depOrWth(self):
        return 'dep' if (random.randint(0, 2) == 0) else 'wth'

    def generateRandomXactions(self, n=1000):
        for _ in range(n):
            data = self.emit()
            print('sent', data)
            self.producer.send('bank-customer-events', value=data)
            sleep(1)


if __name__ == "__main__":
    p = Producer()
    p.generateRandomXactions(n=20)
