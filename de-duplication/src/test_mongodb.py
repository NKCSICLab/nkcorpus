import pymongo
from pymongo.database import Database

# replace this with your MongoDB connection string
conn_str = "mongodb://lidongwen:Lidongwen_2021@10.10.1.217:27017/ldw_test"
# set a 5-second connection timeout
client: pymongo.MongoClient = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
try:
    db: Database = client.ldw_test
    print(db.ldw_test.insert_many([{"x": 1, "y": 2},{"x": 3, "y": 4}]))
    result = db.ldw_test.find({"x": 55})
    if result != None:
        for i in result:
            print(i)
        print("exits ok")
    else:
        print("can not find")
except Exception:
    print("Unable to connect to the server.")
