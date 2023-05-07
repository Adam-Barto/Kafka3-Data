import pathlib
import sqlalchemy as db
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import Session, declarative_base, relationship
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema, fields

basedir = pathlib.Path(__file__).parent.resolve()
# Let's make an engine
engine = db.create_engine(f"sqlite:////{basedir / 'database.db'}")
session = Session(engine)
Base = declarative_base()
metadata = Base.metadata


class Transaction(Base):
    __tablename__ = 'transaction'
    # __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True, nullable=True)
    custid = Column(Integer, ForeignKey('customer.custid'))
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)


class Customer(Base):
    __tablename__ = 'customer'
    id = Column(Integer, primary_key=True, autoincrement=True)
    custid = Column(Integer)
    createdate = Column(Integer)
    fname = Column(String(250), nullable=False)
    lname = Column(String(250), nullable=False)
    transactions = relationship(Transaction, backref="customer")


class TransactionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Transaction
        load_instance = True
        sqla_session = session
        include_fk = True


class CustomerSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Customer
        load_instance = True
        sqla_session = session
        include_relationships = True

    transactions = fields.Nested(TransactionSchema, many=True)


def check_hookup_data():
    #  So this line was throwing the error when I use mysql, but works with Sqlite
    metadata.create_all(engine, checkfirst=True)


trans_schema = TransactionSchema()
custo_schema = CustomerSchema()
check_hookup_data()
