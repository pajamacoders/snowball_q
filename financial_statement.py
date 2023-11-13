from typing import List
from sqlalchemy import Integer, String, DateTime, ForeignKey, BigInteger
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql.schema import Column
from pydantic import BaseModel

Base = declarative_base()

class Company(Base):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True, autoincrement=True)
    corp_code = Column(String, unique=True) 
    stock_code = Column(String, unique=True) 
    corp_name = Column(String, nullable=False)
    sector = Column(String, nullable=False, server_default='NULL')
    market = Column(String, nullable=False)
    listed_year = Column(Integer, nullable=True)
    delisting_year = Column(Integer, nullable=True, server_default='NULL')
    
    financial_statements: Mapped[List["FStatements"]] = relationship()

class FStatements(Base):
    __tablename__ = 'financial_statements'
    id = Column(Integer, primary_key=True)
    stock_code = Column(String, ForeignKey('company.stock_code')) 
    year = Column(Integer, nullable=False) # 2011
    quater = Column(String, nullable=False) # value='1q', '2q', '3q', '4q'
    current_assets = Column(BigInteger, nullable=False) # 유동자산
    noncurrent_assets = Column(BigInteger, nullable=False) # 비유동자산
    full_assets = Column(BigInteger, nullable=False) # 총자산
    current_liabilities = Column(BigInteger, nullable=False) # 유동부채
    noncurrent_liabilities = Column(BigInteger, nullable=False) #비유동부채
    full_liabilities = Column(BigInteger, nullable=False) # 총부채 
    issued_capital = Column(BigInteger, nullable=False) # 자본금
    retain_ernings = Column(BigInteger, nullable=False) # 이익잉여금
    full_equity = Column(BigInteger, nullable=False) # 자본총계
    
    
    