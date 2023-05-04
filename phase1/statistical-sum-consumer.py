from kafka import KafkaConsumer
from json import loads
import pandas as pd
# from configure_database import db, engine, session, trans_schema, Transaction

class SumConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['127.0.0.1:9092'],
                                      auto_offset_reset='earliest',
                                      # enable_auto_commit=True,
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

        # self.table = db.Table('transaction',
        #                         self.meta,
        #                         db.Column('custid', db.Integer()),
        #                         db.Column('type', db.String(4)),
        #                         db.Column('date', db.Integer()),
        #                         db.Column('amt', db.Integer()),
        #                         )
        self.table = pd.DataFrame(index=['custid', 'type', 'date', 'amt'])
        # db.MetaData().create_all(engine)
        # self.connection.begin()
        # Go back to the readme.

    def handleMessages(self):
        # With data stuff
        for message in self.consumer:
            add_too = pd.DataFrame(message.value,index=['custid', 'type', 'date', 'amt'])
            self.table = pd.concat([self.table, add_too])
            print(self.table.describe())

        # query = db.select(Transaction)
        # data = self.connection.execute(query).fetchall()
        # data = session.scalars(statement).all


        # self.connection.close()


if __name__ == "__main__":
    c = SumConsumer()
    c.handleMessages()
