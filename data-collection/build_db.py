import mysql.connector

password = "172155763aaa"
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd=password,
  auth_plugin='mysql_native_password',
  database='load_link'
)

mycursor = mydb.cursor()

mycursor.execute("create database if not exists load_link")
mycursor.execute("""create table if not exists comcrawl_load(
    id int primary key auto_increment,
    link_position varchar(255), 
    size_M int not null default 0,
    date datetime,
    is_load bit default 0,
    server varchar(40),
    is_langdetect bit default 0,
    langdetect_size_M int default 0)
    """)
mydb.close()
