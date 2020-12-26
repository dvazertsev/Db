import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,String,Integer,ForeignKey
from sqlalchemy.orm import relationship ,sessionmaker


DATABASE = 'postgres+psycopg2://postgres:@localhost:1572/Database'
engine = create_engine(DATABASE)
Session = sessionmaker(bind=engine)

Base = declarative_base()

shopper_product = Table('shopper_product',Base.metadata,Column('shopper_data',String,ForeignKey('shopper.shopper_data')),Column('product_type',String,ForeignKey('product.product_type')))

class Seller(Base):
    __tablename__ = 'seller'
    seller_data = Column(String,primary_key = True)
    experience = Column(String)
    cash_register_num = Column(Integer)
    def __init__(self,seller_data,experience,cash_register_num):
        self.seller_data = seller_data
        self.experience = experience
        self.cash_register_num = cash_register_num

class Shopper(Base):
    __tablename__ = 'shopper'
    shopper_data = Column(String,primary_key = True)
    bonus_card = Column(String)
    seller_data = Column(String)
    def __init__(self,shopper_data,bonus_card,seller_data):
        self.shopper_data = shopper_data
        self.bonus_card = bonus_card
        self.seller_data = seseller_data

class Product(Base):
    __tablename__ = 'product'
    product_type = Column(String,primary_key = True)
    price = Column(Integer)
    weight = Column(String)
    def __init__ (self,product_type,price,weight):
        self.product_type = product_type
        self.price = price
        self.weight = weight

class Price(Base):
    __tablename__ = 'price'
    price = Column(Integer,ForeignKey('product.price'),primary_key = True)
    new_price= Column(Integer)
    def __init__(self,price,new_price):
        self.price = price
        self.new_price = new_price