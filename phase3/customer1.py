from kafka import KafkaConsumer, TopicPartition
from json import loads
from configure_database import session, trans_schema, custo_schema, Customer

from random import randint

# This will be used to generate the customer names
name_list = ['John', 'William', 'Alice', 'Bell', 'Harly', 'Apple', 'Mango', 'Doctor', 'Marcel', 'Melon', 'Bread',
             'Cheese']


class XactionConsumer:
    def __init__(self, partition_id):
        self.consumer = KafkaConsumer(bootstrap_servers=['127.0.0.1:9092'],
                                      auto_offset_reset='earliest',
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        partitions = self.consumer.partitions_for_topic('bank-customer-new')
        print(partitions)
        partition = TopicPartition('bank-customer-new', partition_id)
        print(partition)
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        self.bankid = partition_id
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.consumer.assign([partition])  # This connects it to the partition. How cool.
        self.connection = session

    def generate_customer(self, custid=0, date=0):
        data = {'custid': custid,
                'createdate': date,
                'fname': name_list[randint(0, len(name_list) - 1)],
                'lname': name_list[randint(0, len(name_list) - 1)],
                'bankid': self.bankid
                }
        return data

    def handleMessages(self):
        print(self.consumer.assignment())
        for msg in self.consumer:
            message = msg.value
            print('{} received'.format(message))
            get_customers_in_database = self.connection.query(Customer.custid, Customer.bankid).all()
            print(get_customers_in_database)
            self.ledger[message['custid']] = message
            check_list = [person[0] for person in get_customers_in_database if message['custid'] is person[0] and person[1] == self.bankid]
            # print(check_list)
            #  Says there is a connection when all connections should have been closed already.
            # self.connection.begin()
            if message['custid'] not in check_list:
                customer = self.generate_customer(message['custid'], message['date'])
                new_customer = custo_schema.load(customer, session=session)
                self.connection.add(new_customer)
                print('Customer Adding')
                self.connection.commit()
            new_message = trans_schema.load(message, session=session)
            self.connection.add(new_message)
            self.connection.commit()
            self.connection.close()
            self.ledger[message['custid']] = message
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']


if __name__ == "__main__":
    c = XactionConsumer(1)  # The id of the partition is located here
    c.handleMessages()
