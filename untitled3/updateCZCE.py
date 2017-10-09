# coding=utf8
from bs4 import BeautifulSoup
import requests
import MySQLdb
from datetime import date, time, datetime, timedelta
import urllib2
import logging

#每天获取实施更新数据

def getListInfor(datetim):
    conn = MySQLdb.connect(
        host='localhost',
        user='root',
        passwd='pzj123',
        db='futurestrading',
        charset="utf8",
    )
    cur = conn.cursor()
    year=datetim[0:4]
    print datetim,year
    response = requests.get('http://www.czce.com.cn/portal/DFSStaticFiles/Future/%s/%s/FutureDataHolding.htm' % (year, datetim))
    soup=BeautifulSoup(response.text,'lxml')
    cp=soup.select('.td-center')
    c=0
    cp1s=[]
    cp2s=[]
    cp3s=[]
    while c <len(cp):
        if c % 3 == 0:
            cp1s.append(cp[c].get_text())
        elif c % 3 == 1:
            cp2s.append(cp[c].get_text())
        else:
            cp3s.append(cp[c].get_text())
        c = c + 1
    tr=soup.select('.td-right')[::2]
    items=[]
    for item in tr:
        items.append(str(item.get_text()))
    x=0
    n=len(items)
    turnover=[]
    buy=[]
    sell=[]
    while x < n-1:
        if x % 3 == 0:
            turnover.append(items[x])
        elif x % 3 == 1:
            buy.append(items[x])
        else:
            sell.append(items[x])
        x = x + 1
    for cp1,turnover,cp2,buy,cp3,sell in zip(cp1s,turnover,cp2s,buy,cp3s,sell):
                data={
                    'turnover':turnover,
                    'buy':buy,
                    'sell':sell
                }
                if cp1.strip().strip('-')=='':
                     data1={
                         'cp1':cp1
                     }
                else:
                     data1={
                         'cp1': cp1.encode('iso-8859-1').decode('gbk')
                     }
                if cp2.strip().strip('-') == '':
                    data2 = {
                        'cp2': cp2
                    }
                else:
                    data2 = {
                        'cp2': cp2.encode('iso-8859-1').decode('gbk')
                    }
                if cp3.strip().strip('-') == '':
                    data3 = {
                        'cp3': cp3
                    }
                else:
                    data3 = {
                        'cp3': cp3.encode('iso-8859-1').decode('gbk')
                    }
                sqlv = "INSERT INTO volume  (date,volumecompany,volume)VALUES (" + "'" + datetim + "','" + data1['cp1'] + "','" + str(data['turnover']) + "'" + ");"
                logging.info(sqlv)
                print sqlv
                cur.execute(sqlv)
                conn.commit()
                sql = "INSERT INTO tradinginfor  (date,buycompany,buy,sellcompany,sell)VALUES (" + "'" + datetim + "','" + data2['cp2'] + "','" + str(
                    data['buy']) + "','" + data3['cp3'] + "','" + str(data['sell']) + "'" + ");"
                logging.info(sql)
                print sql
                cur.execute(sql)
                conn.commit()
    cur.close()



def runTask(day=0, hour=0, min=0, second=0):
    now = datetime.now()
    strnow = now.strftime('%Y%m%d%H%M%S').strip()

    logging.info("now:" + strnow)
    print "now:", strnow
    period = timedelta(days=day, hours=hour, minutes=min, seconds=second)
    next_time = now + period
    strnext_time = next_time.strftime('%Y-%m-%d %H:%M:%S')
    logging.info("next run:" + strnext_time)
    print "next run:", strnext_time
    while True:
        iter_now = datetime.now()
        iter_now_time = iter_now.strftime('%Y-%m-%d %H:%M:%S')
        dati = iter_now.strftime('%Y%m%d%H%M%S').strip()
        datetim = dati[0:8]
        ti = dati[8:]
        if int(ti) == 120000:
            startcheck()
        if str(iter_now_time) == str(strnext_time):
            logging.info("start work: %s" % iter_now_time)
            print "start work: %s" % iter_now_time
            if 180000 >= int(ti) >= 164000:
                getListInfor(datetim)
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
        print item
        datetim= item['date'][0:8].strip()
        year = item['date'][0:4].strip()
    logging.info("检验昨天数据")
    response =requests.get('http://www.czce.com.cn/portal/DFSStaticFiles/Future/%s/%s/FutureDataHolding.htm'%(year,datetim))
    soup = BeautifulSoup(response.text, 'lxml')
    cp = soup.select('.td-center')
    c = 0
    cp1s = []
    cp2s = []
    cp3s = []
    while c < len(cp):
        if c % 3 == 0:
            cp1s.append(cp[c].get_text())
        elif c % 3 == 1:
            cp2s.append(cp[c].get_text())
        else:
            cp3s.append(cp[c].get_text())
        c = c + 1
    tr = soup.select('.td-right')[::2]
    items = []
    for item in tr:
        items.append(str(item.get_text()))
    x = 0
    n = len(items)
    turnover = []
    buy = []
    sell = []
    while x < n - 1:
        if x % 3 == 0:
            turnover.append(items[x])
        elif x % 3 == 1:
            buy.append(items[x])
        else:
            sell.append(items[x])
        x = x + 1
    for cp1, turnover, cp2, buy, cp3, sell in zip(cp1s, turnover, cp2s, buy, cp3s, sell):
        data = {
            'turnover': turnover,
            'buy': buy,
            'sell': sell
        }
        if cp1.strip().strip('-') == '':
            data1 = {
                'cp1': cp1
            }
        else:
            data1 = {
                'cp1': cp1.encode('iso-8859-1').decode('gbk')
            }
        if cp2.strip().strip('-') == '':
            data2 = {
                'cp2': cp2
            }
        else:
            data2 = {
                'cp2': cp2.encode('iso-8859-1').decode('gbk')
            }
        if cp3.strip().strip('-') == '':
            data3 = {
                'cp3': cp3
            }
        else:
            data3 = {
                'cp3': cp3.encode('iso-8859-1').decode('gbk')
            }
        print data1['cp1'],data['turnover'],data2['cp2'],data['buy'],data3['cp3'],data['sell']
        logging.info(data1['cp1']+data['turnover']+data2['cp2']+data['buy']+data3['cp3']+data['sell'])

runTask(day=0, hour=0, min=1, second=0)
pass