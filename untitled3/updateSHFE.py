#encoding=utf-8
import urllib2
import json
import MySQLdb
import logging
from datetime import date, time, datetime, timedelta
import sys
reload(sys)
sys.setdefaultencoding( "utf8" )

#获取实时更新数据

def getInforList(date):
    conn = MySQLdb.connect(
        host='localhost',
        user='root',
        passwd='pzj123',
        db='futurestrading',
        charset="utf8",
    )
    cur = conn.cursor()
    dateti = datetime.now()
    datetim = dateti.strftime('%Y%m%d%H%M%S').strip()
    try:
        response=urllib2.urlopen('http://www.shfe.com.cn/data/dailydata/kx/pm%s.dat'%date)
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

            sqlv = "INSERT INTO volume  (date,volumecompany,volume)VALUES (" + "'" + datetim + "','" + company1 + "','" + str(cj1)  + "'" + ");"
            logging.info(sqlv)
            print sqlv
            cur.execute(sqlv)
            conn.commit()
            sql = "INSERT INTO tradinginfor  (date,buycompany,buy,sellcompany,sell)VALUES (" + "'" +datetim+"','" + company2 + "','" + str(cj2) + "','" + company3 + "','" + str(cj3) + "'" + ");"
            logging.info(sql)
            print sql
            cur.execute(sql)
            conn.commit()
        cur.close()
        pass
    else:
        logging.info("法定节假日,没有交易信息!")
        print '法定节假日,没有交易信息!'

def runTask(day=0, hour=0, min=0, second=0):
        now = datetime.now()
        strnow = now.strftime('%Y%m%d%H%M%S').strip()

        logging.info("now:"+strnow)
        print "now:", strnow
        period = timedelta(days=day, hours=hour, minutes=min, seconds=second)
        next_time = now + period
        strnext_time = next_time.strftime('%Y-%m-%d %H:%M:%S')
        logging .info("next run:"+ strnext_time)
        print "next run:", strnext_time
        while True:
            iter_now = datetime.now()
            iter_now_time = iter_now.strftime('%Y-%m-%d %H:%M:%S')
            dati = iter_now.strftime('%Y%m%d%H%M%S').strip()
            da = dati[0:8]
            ti = dati[8:]
            if  int(ti)==120000:
                startcheck()
            if str(iter_now_time) == str(strnext_time):
                logging.info("start work: %s" % iter_now_time)
                print "start work: %s" % iter_now_time
                if 180000 >=int(ti) >=164000:
                    getInforList(da)
                iter_time = iter_now + period
                strnext_time = iter_time.strftime('%Y-%m-%d %H:%M:%S')
                print "next_iter: %s" % strnext_time
                continue

def startcheck():
    conn = MySQLdb.connect(
        host='localhost',
        user='root',
        passwd='pzj123',
        db='futurestrading',
        charset="utf8",
    )
    cur = conn.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM volume ORDER BY Id DESC LIMIT 1")
    rows = cur.fetchall()
    for item in rows:
        date = item['date'][0:8].strip()
    logging.info("检验昨天数据"+date)
    try:
        response = urllib2.urlopen('http://www.shfe.com.cn/data/dailydata/kx/pm%s.dat' %date)
        resp = response.read()[12:-148]
        print resp
    except:
        logging.info("昨天没有交易数据-_-!")
        print "昨天没有交易数据-_-!"

runTask(day=0, hour=0, min=1, second=0)
pass