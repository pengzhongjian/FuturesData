# coding=utf8
import urllib
import requests
from bs4 import BeautifulSoup
import MySQLdb
import logging
from datetime import date, time, datetime, timedelta
import sys
reload(sys)

#获取每天时时更新数据

def getListInfor():
    sys.setdefaultencoding('utf-8')
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
    year=datetim[0:4]
    day=datetim[6:8]
    month=int(datetim[4:6])-1
    jyurl='http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html'
    #修改日期,月份减一
    value={
        'memberDealPosiQuotes.variety': 'a',
        'memberDealPosiQuotes.trade_type':'0',
        'year':year,
        'day':day,
        'month':month,
        'contract.contract_id':'all',
        'contract.variety_id':'a'
    }
    data=urllib.urlencode(value)
    reslut=requests.get(jyurl,data)
    soup=BeautifulSoup(reslut.text,'lxml')
    qi=soup.select('td')
    x=14
    n=len(qi)
    cp1s=[]
    turnovers=[]
    cp2s=[]
    buys=[]
    cp3s=[]
    sells=[]
    while x < n-1:
        if (x-14)% 12 == 1:
            cp1s.append(qi[x].get_text())
        elif (x-14)% 12 == 2:
            turnovers.append(qi[x].get_text())
        elif (x-14)% 12 == 5:
            cp2s.append(qi[x].get_text())
        elif (x-14)% 12 == 6:
            buys.append(qi[x].get_text())
        elif (x-14)% 12 == 9:
            cp3s.append(qi[x].get_text())
        elif (x-14)% 12 == 10:
            sells.append(qi[x].get_text())
        x=x+1
    for cp1,turnover,cp2,buy,cp3,sell in zip(cp1s,turnovers,cp2s,buys,cp3s,sells):
        data = {
                'cp1':cp1,
                'turnover': turnover,
                'cp2':cp2,
                'buy': buy,
                'cp3':cp3,
                'sell': sell
            }
        sqlv = "INSERT INTO volume  (date,volumecompany,volume)VALUES (" + "'" + datetim + "','" + cp1 + "','" + str(turnover) + "'" + ");"
        logging.info(sqlv)
        print sqlv
        cur.execute(sqlv)
        conn.commit()
        sql = "INSERT INTO tradinginfor  (date,buycompany,buy,sellcompany,sell)VALUES (" + "'" + datetim + "','" + cp2 + "','" + str(buy) + "','" + cp3 + "','" + str(sell) + "'" + ");"
        logging.info(sql)
        print sql
        cur.execute(sql)
        conn.commit()
    cur.close()


def runTask(day=0, hour=0, min=0, second=0):
    now = datetime.now()
    strnow = now.strftime('%Y%m%d%H%M%S').strip()
    logging.info("now:"+ strnow)
    print "now:", strnow
    period = timedelta(days=day, hours=hour, minutes=min, seconds=second)
    next_time = now + period
    strnext_time = next_time.strftime('%Y-%m-%d %H:%M:%S')
    logging.info("next run:"+ strnext_time)
    print "next run:", strnext_time
    while True:
        iter_now = datetime.now()
        iter_now_time = iter_now.strftime('%Y-%m-%d %H:%M:%S')
        dati = iter_now.strftime('%Y%m%d%H%M%S').strip()
        da = dati[0:8]
        ti = dati[8:]
        if int(ti) == 120000:
            startcheck()
        if str(iter_now_time) == str(strnext_time):
            logging.info("start work: %s" % iter_now_time)
            print "start work: %s" % iter_now_time
            if 180000 >= int(ti) >= 164000:
                getListInfor()
            iter_time = iter_now + period
            strnext_time = iter_time.strftime('%Y-%m-%d %H:%M:%S')
            logging.info("next_iter: %s" % strnext_time)
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
        year = date[0:4]
        day = date[6:8]
        month = int(date[4:6])-1
    logging.info('检验昨天数据!')
    print '检验昨天数据!',date
    jyurl = 'http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html'
    # 修改日期,月份减一
    value = {
        'memberDealPosiQuotes.variety': 'a',
        'memberDealPosiQuotes.trade_type': '0',
        'year': year,
        'day': day,
        'month': month,
        'contract.contract_id': 'all',
        'contract.variety_id': 'a'
    }
    data = urllib.urlencode(value)
    reslut = requests.get(jyurl, data)
    soup = BeautifulSoup(reslut.text, 'lxml')
    qi = soup.select('td')
    logging.info(qi)
    print qi

runTask(day=0, hour=0, min=1, second=0)
pass