# coding=utf8
import urllib
import requests
from bs4 import BeautifulSoup
import MySQLdb
import logging
from datetime import date, time, datetime, timedelta
import sys
reload(sys)


#根据date.txt里面日期获取历史数据

def getListInfor(datetim):
    sys.setdefaultencoding('utf-8')
    conn = MySQLdb.connect(
        host='localhost',
        user='root',
        passwd='pzj123',
        db='futurestrading',
        charset="utf8",
    )
    cur = conn.cursor()
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

f=open(r'C:\tradingdate\date.txt')
for i in f.readlines():
    getListInfor(i.strip())
pass