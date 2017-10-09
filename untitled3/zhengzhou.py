# coding=utf8
from bs4 import BeautifulSoup
import requests
import MySQLdb
import logging

#根据date.txt里面日期来获取历史数据

def getListInfor(datetime):
    conn = MySQLdb.connect(
        host='localhost',
        user='root',
        passwd='pzj123',
        db='futurestrading',
        charset="utf8",
    )
    cur = conn.cursor()
    year=datetime[0:4]
    response=requests.get('http://www.czce.com.cn/portal/DFSStaticFiles/Future/%s/%s/FutureDataHolding.htm'%(year,datetime))
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
                sqlv = "INSERT INTO volume  (date,volumecompany,volume)VALUES (" + "'" + datetime + "','" + data1['cp1'] + "','" + str(data['turnover']) + "'" + ");"
                logging.info(sqlv)
                print sqlv
                cur.execute(sqlv)
                conn.commit()
                sql = "INSERT INTO tradinginfor  (date,buycompany,buy,sellcompany,sell)VALUES (" + "'" + datetime + "','" + data2['cp2'] + "','" + str(
                    data['buy']) + "','" + data3['cp3'] + "','" + str(data['sell']) + "'" + ");"
                logging.info(sql)
                print sql
                cur.execute(sql)
                conn.commit()
    cur.close()

#获取文件数据
f=open(r'C:\tradingdate\date.txt')
for i in f.readlines():
    getListInfor(i.strip())
pass