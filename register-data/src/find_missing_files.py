import os
import MySQLdb
import db
import utils
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
sql = 'select id, out_path from storage where prefix = "original" and id_device = 2'
cursor.execute(sql)
result = cursor.fetchall()
with open('out.txt', 'w') as out:
    progbar = utils.ProgBar(target=len(result), stateful_metrics=['missing'])
    missing = 0
    for row in result:
        _id = row[0]
        out_path = row[1]
        if not os.path.exists(f'/mnt/2020-10/original/{out_path}'):
            missing += 1
            out.write(f'{_id}\n')
        progbar.add(1, [('missing', missing)])
