import MySQLdb
import db
import configs

CONFIG_PATH = 'configs'

config = configs.config(CONFIG_PATH)
DB_CONF = db.get_database_config(config)

db = MySQLdb.connect(DB_CONF.host,
                     DB_CONF.username,
                     DB_CONF.password,
                     DB_CONF.database,
                     charset='utf8')
cursor = db.cursor()

with open('out.txt', 'r') as f:
    lines = f.readlines()
    id_list = ', '.join([line.strip() for line in lines])
    sql = 'delete from storage where id in (' + id_list + ')'
    print(f'{cursor.execute(sql)} rows affected')
    db.commit()
    print('Transaction committed')
