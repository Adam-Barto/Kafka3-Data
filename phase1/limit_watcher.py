from kafka import KafkaConsumer
from json import loads
import pandas as pd
# from configure_database import db, engine, session, trans_schema, Transaction

class LimitWatcher:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['127.0.0.1:9092'],
                                      auto_offset_reset='earliest',
                                      # enable_auto_commit=True,
                                      value_deserializer=lambda m: loads(m.decode('ascii')))

    def handleMessages(self):
        # With data stuff
        for message in self.consumer:
            if message.value.get('amt') < -5000:
                print(f'\033[31m Customer with Id {message.value.get("custid")} has an amount under 5000 \033[00m')


if __name__ == "__main__":
    c = LimitWatcher()
    c.handleMessages()
