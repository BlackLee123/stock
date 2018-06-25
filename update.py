# -*- coding: utf-8 -*-

import pymysql
import time

db = pymysql.connect(host='47.96.12.103', user='root', password='123456', db='stock', port=3306)
cur = db.cursor()

try:
    backdate = time.strftime('%Y%m%d', time.localtime(time.time()))
    backdate = '20180301'
    selsql = "select code from stockback where date='%s' order by code" % backdate
    cur.execute(selsql)
    stocks = cur.fetchall()
    for stock in stocks:
        print(stock[0])
        cur.execute("select max(sequence) from stockback where code = '%s'" % stock[0])
        sequence = cur.fetchone()
        if not sequence[0]:
            sequence = 1
        else:
            sequence = sequence[0] + 1
        updatesql = "update stockback set sequence = %d where code = '%s' and date = '%s'" % (sequence, stock[0], backdate)
        cur.execute(updatesql)
except Exception as e:
    raise e
finally:
    db.commit()
    db.close()
