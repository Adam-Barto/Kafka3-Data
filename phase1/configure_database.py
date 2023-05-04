import pathlib
import sqlalchemy as db
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Session, declarative_base
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

# key_file = open('pathtosecret', 'r')
# token = key_file.read()  # Let's grab that Key here, and NOT upload it to Github.
# key_file.close()

basedir = pathlib.Path(__file__).parent.resolve()

# Let's make an engine
# secret = token.splitlines()
#
# engine = db.create_engine(f"mysql+mysqlconnector://{secret[0]}:{secret[1]}@localhost:3306/{basedir / 'database.db'}", pool_pre_ping=True)
engine = db.create_engine(f"sqlite:////{basedir / 'database.db'}")
metadata = db.MetaData()
session = Session(engine)
Base = declarative_base()


class Transaction(Base):
    __tablename__ = 'transaction'
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)


class TransactionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Transaction
        load_instance = True
        sqla_session = session


# create_table = db.Table('transaction',
#                         metadata,
#                         db.Column('custid', db.Integer()),
#                         db.Column('type', db.String(4)),
#                         db.Column('date', db.Integer()),
#                         db.Column('amt', db.Integer()),
#                         )

# connection = engine.connect()

def check_hookup_data():
    #  So this line was throwing the error when I use mysql, but works with Sqlite
    metadata.create_all(engine, checkfirst=True)


#     return create_table
#
#
# table = check_hookup_data()
check_hookup_data()
metadata.create_all(engine)
# session.begin()
trans_schema = TransactionSchema()
