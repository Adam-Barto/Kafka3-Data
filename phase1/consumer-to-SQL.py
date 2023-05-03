from kafka import KafkaConsumer, TopicPartition
from json import loads
from configure_database import db, engine


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['127.0.0.1:9092'],
                                      # auto_offset_reset='earliest',
                                      enable_auto_commit=True,
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.connection = engine.connect()
        self.metadata = db.MetaData()
        self.table = db.Table('transaction',
                              self.metadata,
                              db.Column('custid', db.Integer()),
                              db.Column('type', db.String(4)),
                              db.Column('date', db.Integer()),
                              db.Column('amt', db.Integer()),
                              )
        # Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL using SQLalchemy
            query = db.insert(self.table).values(message)
            self.connection.execute(query)  # this is throwing the error
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)
        self.connection.close()


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
