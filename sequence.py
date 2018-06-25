# -*- coding: utf-8 -*-

import datetime
import pymysql


db = pymysql.connect(host='47.96.12.103', user='root', password='123456', db='stock', port=3306)
cur = db.cursor()

try:
    stocksql = "select distinct code from stockback order by code"
    cur.execute(stocksql)
    stocks = cur.fetchall()
    for code in stocks:
        with open('./file/{code}.sql'.format(code=code), 'w') as f:
            print(code)
            selectsql = "select date from stockback where code = '%s' order by date" % code[0]
            cur.execute(selectsql)
            dates = cur.fetchall()
            count = 1
            updatesql = ""
            args = []
            for date in dates:
                updatesql += "update stockback set sequence = %d where code = '%s' and date = '%s';\n" % (count, code[0], datetime.datetime.strptime(str(date[0]), '%Y-%m-%d').strftime('%Y-%m-%d'))
                count += 1
            f.write(updatesql)
except Exception as e:
    raise e
finally:
    db.close()
