from kafka import KafkaConsumer
from json import loads


# LimitConsumer should keep track of the customer ids that have current balances greater or equal to the limit
# supplied to the constructor. The intro suggests -5000 for example, but you should be able set that with a parameter
# to the class' Constructor

class LimitWatcher:
    def __init__(self, limit_value=-5000):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['127.0.0.1:9092'],
                                      auto_offset_reset='earliest',
                                      # enable_auto_commit=True,
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        self.limit_value = limit_value
        self.consumer_limit = set()
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            self.ledger[message['custid']] = message

            if message['custid'] not in self.custBalances: # This just fills out the ledger and custBalances
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']

            if self.custBalances[message['custid']] <= self.limit_value:
                self.consumer_limit.add(message['custid'])
            else:
                self.consumer_limit.discard(message['custid'])

            print(self.consumer_limit) #This is the accounts that are under the value.

if __name__ == "__main__":
    c = LimitWatcher(-5000)
    c.handleMessages()