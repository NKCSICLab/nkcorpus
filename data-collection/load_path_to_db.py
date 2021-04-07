import mysql.connector
import sys
from connect_to_db import connect_to_db

mydb = connect_to_db()
mycursor = mydb.cursor()
sql_seq = "insert into comcrawl_load(link_position) values(%s)"
num = 10000
value = []
with open(sys.argv[1],'r') as lines:
    for line in lines:
        value.append((line,))
        num-=1
        if(num<0):
            mycursor.executemany(sql_seq, value)
            mydb.commit()
            value=[]
            num=10000
mycursor.executemany(sql_seq, value)
mydb.commit()
mydb.close()


