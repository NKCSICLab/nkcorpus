from sqlalchemy import *
from load_link_tables import comcrawl_load
from sqlalchemy.orm import Session

#数据库url
sql_type = "mysql+pymysql"
user = "wangzhihao"
password = "Wangzhihao_2021"
server = "10.10.1.217"
port = "3306"
database = "comcrawl_data"
try:
    engine = create_engine('%s://%s:%s@%s:%s/%s'%(sql_type, user, password, server, port, database))
except Exception as e:
    print(str(e))
    print('连接数据库失败')
metadata = MetaData()

#建表
comcrawl_load_table = Table('comcrawl_load', metadata,
    Column('id', Integer, primary_key=True),
    Column('link_position', String(150), nullable=False),
    Column('size_M', Integer, nullable=False, default=0),
    Column('date', DateTime()),
    Column('is_handle', Integer, default=0),
    Column('is_load', Integer, default=0),
    Column('server_id', Integer),
    Column('year_month',String(30))
)
comcrawl_load_table.create(engine, checkfirst=True)

#插入数据
print("--输入文件路径--")
path_p = input()
print("--输入文件年月--(例2000-01)")
year_month = input()
try:
    with open(path_p, 'r') as lines:
        with Session(engine) as session, session.begin():
            print('--准备插入--')
            for line in lines:
                line = line.replace('\n', '')
                session.add(comcrawl_load(link_position=line, year_month=year_month))
except Exception as e:
    print(str(e))
    print('--插入失败--')

else:
    print('--插入完成--')

        
