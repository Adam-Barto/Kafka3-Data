from kafka import KafkaConsumer
from json import loads
import pandas as pd


# SummaryConsumer should produce a list of outputs, the status of the mean (avg) deposits and mean withdrawals across
# all customers. You should also print the standard deviation of the distribution for both deposits and withdrawals.
# As each transaction comes in, print a new status of the numerical summaries.


class SummaryConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['127.0.0.1:9092'],
                                      auto_offset_reset='earliest',
                                      # enable_auto_commit=True,
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        self.table = pd.DataFrame(columns=['custid', 'type', 'date', 'amt'])
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}

    # SummaryConsumer should produce a list of outputs, the status of the mean (avg) deposits and mean withdrawals across
    # all customers. You should also print the standard deviation of the distribution for both deposits and withdrawals.
    # As each transaction comes in, print a new status of the numerical summaries.
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

            add_too = pd.DataFrame(message, columns=['custid', 'type', 'date', 'amt'], index=['dropme'])
            self.table = pd.concat([self.table, add_too])
            deposits = self.table.loc[self.table['type'] == 'dep', 'amt']
            withdrawls = self.table.loc[self.table['type'] == 'wth', 'amt']

            print(f'\033[31m Avg Deposits:\033[33m {deposits.mean()}\033[00m \n'
                  f'\033[31m Avg Withdrawls:\033[33m {withdrawls.mean()}\033[00m \n'
                  f'\033[31m Standard Deviation of Deposits:\033[33m {deposits.std()}\033[00m \n'
                  f'\033[31m Standard Deviation of Withdrawls:\033[33m {withdrawls.std()}\033[00m \n')
            # print(type(message.values))
            # print(self.table)


if __name__ == "__main__":
    c = SummaryConsumer()
    c.handleMessages()
