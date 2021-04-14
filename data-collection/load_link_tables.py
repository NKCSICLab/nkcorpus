from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

class comcrawl_load(declarative_base()):
    __tablename__='comcrawl_load'
    id = Column(Integer, primary_key=True)
    link_position = Column(String(150), nullable=False)
    size_M = Column(Integer, nullable=False, default=0)
    date = Column(DateTime())
    is_handle = Column(Integer, default=0)
    is_load = Column(Integer, default=0)
    server_id = Column(Integer)
    year_month = Column(String(30))
