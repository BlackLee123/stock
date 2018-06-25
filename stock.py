#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import pymysql
import os
import time

db = pymysql.connect(host='localhost', user='root', password='123456', db='stock', port=3306)
cur = db.cursor()

path = '/var/lib/mysql/stock/stock_data'

try:
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.splitext(file_path)[1] == '.csv':
            print(file_path)
            data = '''load data infile '{filename}'
into table stock
fields terminated by ','  optionally enclosed by ' ' escaped by '\\\\'
lines terminated by '\\n'
IGNORE 1 LINES(
`code`,
`date`,
`open`,
`high`,
`low`,
`close`,
`change`,
`volume`,
`money`,
`traded_market_value`,
`market_value`,
`turnover`,
`adjust_price`,
`report_type`,
`report_date`,
`PE_TTM`,
`PS_TTM`,
`PC_TTM`,
`PB`,
`adjust_price_f`
    );'''.format(filename=file_path)
            cur.execute(data)
            backdate = time.strftime('%Y%m%d', time.localtime(time.time()))
            backupsql = "insert into stockback(`id`,`code`,`date`,`open`,`high`,`low`,`close`,`change`,`volume`,`money`,`traded_market_value`,`market_value`,`turnover`,`adjust_price`,`report_type`,`report_date`,`PE_TTM`,`PS_TTM`,`PC_TTM`,`PB`,`adjust_price_f`) select `id`,`code`,`date`,`open`,`high`,`low`,`close`,`change`,`volume`,`money`,`traded_market_value`,`market_value`,`turnover`,`adjust_price`,`report_type`,`report_date`,`PE_TTM`,`PS_TTM`,`PC_TTM`,`PB`,`adjust_price_f` from stock where date='%s'" % backdate
            cur.execute(backupsql)
            os.system('rm -rf "{filename}"'.format(filename=file_path))
except Exception as e:
    raise e
finally:
    db.commit()
    db.close()
