import pathlib
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

key_file = open('/Users/adamb/Projects/Week 10/Kafka3-Data/phase1/env', 'r')
token = key_file.read()  # Let's grab that Key here, and NOT upload it to Github.
key_file.close()

basedir = pathlib.Path(__file__).parent.resolve()

# Let's make an engine
secret = token.splitlines()
#
# engine = db.create_engine(f"mysql+mysqlconnector://{secret[0]}:{secret[1]}@localhost:3306/{basedir / 'database.db'}", pool_pre_ping=True)
engine = db.create_engine(f"sqlite:////{basedir / 'database.db'}")
metadata = db.MetaData()


# connection = engine.connect()

def check_hookup_data():
    create_table = db.Table('transaction',
                            metadata,
                            db.Column('custid', db.Integer()),
                            db.Column('type', db.String(4)),
                            db.Column('date', db.Integer()),
                            db.Column('amt', db.Integer()),
                            )
    #  So this line was throwing the error when I use mysql, but works with Sqlite
    metadata.create_all(engine, checkfirst=True)


#     return create_table
#
#
# table = check_hookup_data()
check_hookup_data()
# engine.begin()
