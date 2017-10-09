#encoding=utf-8
import urllib2
import json
import MySQLdb
import logging
import sys
reload(sys)
sys.setdefaultencoding( "utf8" )

def getInforList(date):
    conn = MySQLdb.connect(
        host='localhost',
        user='root',
        passwd='pzj123',
        db='futurestrading',
        charset="utf8",
    )
    cur = conn.cursor()
    try:
        response=urllib2.urlopen('http://www.shfe.com.cn/data/dailydata/kx/pm%s.dat'%date)
        logging.info(date)
        falg='true'
    except:
        falg='false'
    if falg=='true':
        resp=response.read()[12:-148]
        array = json.loads(resp)
        for item in array:
            company1=item['PARTICIPANTABBR1'].strip()
            cj1 = item["CJ1"]
            company2 = item['PARTICIPANTABBR2'].strip()
            cj2 = item["CJ2"]
            company3 = item['PARTICIPANTABBR3'].strip()
            cj3 = item["CJ3"]

            sqlv = "INSERT INTO volume  (date,volumecompany,volume)VALUES (" + "'" + date + "','" + company1 + "','" + str(cj1)  + "'" + ");"
            logging.info(sqlv)
            print sqlv
            cur.execute(sqlv)
            conn.commit()
            sql = "INSERT INTO tradinginfor  (date,buycompany,buy,sellcompany,sell)VALUES (" + "'" +date+"','" + company2 + "','" + str(cj2) + "','" + company3 + "','" + str(cj3) + "'" + ");"
            logging.info(sql)
            print sql
            cur.execute(sql)
            conn.commit()
        cur.close()
        pass
    else:
        print '法定节假日,没有交易信息!'
        logging.info( "法定节假日,没有交易信息!")
#根据txt文件里面获取时间来获取某天数据
f=open(r'C:\tradingdate\date.txt')
for i in f.readlines():
    getInforList(i.strip())
pass