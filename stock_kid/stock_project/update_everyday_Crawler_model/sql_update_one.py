# -*- encoding: utf8 -*-
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import re
from random import randint
import pandas as pd
from selenium.webdriver import Chrome
from selenium.webdriver.support.ui import Select
import numpy as np
import time
import random
import os
import datetime
import json
import pymysql

#西元改民國
def turnyear(A):
    y = str( int( A[0:4] ) - 1911 )
    m = A[4:6]
    d = A[6:8]
    B = y + '/' + m + '/' + d
    return B
def create_assist_date(datestart=None, dateend=None):
    if datestart is None:
        datestart = '20161003'
    if dateend is None:
        dateend = datetime.datetime.now().strftime( '20200521' )

    datestart = datetime.datetime.strptime( datestart, '%Y%m%d' )
    dateend = datetime.datetime.strptime( dateend, '%Y%m%d' )
    date_list = []
    date_list.append( datestart.strftime( '%Y%m%d' ) )
    while datestart < dateend:
        datestart += datetime.timedelta( days=+1 )
        date_list.append( datestart.strftime( '%Y%m%d' ) )
    return date_list
def sql_update(date_1):
    tmp_list = Stockiid.values()
    stock_iids = []
    for i in tmp_list:
        i = i.replace(' ', '')
        stock_iids.append(i)
    aa = stock_iids

    # https://stock.capital.com.tw/z/zc/zcl/zcl.djhtm?a=1101&c=2018-1-1&d=2020-4-28
    # https://stock.capital.com.tw/z/zc/zcl/zcl.djhtm?a=1101
    headerlist = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36 OPR/43.0.2442.991",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36 OPR/42.0.2393.94",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36 OPR/47.0.2631.39",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
        "Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko"]
    db = pymysql.connect(host='127.0.0.1', port=3306, user='dbuser6', passwd='aabb1234', db='Project_test')
    cur = db.cursor()
    user_agent = random.choice(headerlist)
    headers = {'User-Agent': user_agent}

    tt = turnyear(date_1) # 轉換成民國
    url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=%s&type=ALL'%date_1
    # time.sleep(random.randint(0, 5))
    res = requests.get(url, headers=headers)
    # res.encoding = 'Big5'
    # soup = BeautifulSoup(res.text, 'html.parser')
    jdata = json.loads(res.text, encoding='utf-8')

    url = 'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&o=json&d=%s&se=EW&s=0,asc,0'%tt
    # time.sleep(random.randint(0, 5))
    res = requests.get(url, headers=headers)
    # res.encoding = 'Big5'
    # soup = BeautifulSoup(res.text, 'html.parser')
    jdata2 = json.loads(res.text, encoding='utf-8')
    data_2 = pd.DataFrame(pd.DataFrame(jdata2['aaData']),columns=[0,4,5,6,2,7])

    if len(jdata['stat'])!=14:
        try:
            data_1 = pd.DataFrame(pd.DataFrame(jdata['data9']),columns=[0,5,6,7,8,2])
        except:
            data_1 = pd.DataFrame(pd.DataFrame(jdata['data8']), columns=[0, 5, 6, 7, 8, 2])
        # time_tra = pd.to_datetime(time)
        timeTuple = time.strptime(date_1, "%Y%m%d")
        time_tra = time.strftime("%Y-%m-%d", timeTuple)
        """
        # debug
        if date_1 == '20180117':
            stock_iids = stock_iids2
        else:
            stock_iids = aa
        """
        for id in stock_iids:
            tmp_data_1 = data_1[data_1[0] == id].drop([0], axis=1)
            tmp_data_2 = data_2[data_2[0] == id].drop([0], axis=1)
            if sum(data_1[0] == id)>0:
                # tmp_data_1 = pd.concat([tmp_data_1, pd.DataFrame([time_tra])], axis=1)
                tmp_data_1.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                try:
                    a1 = float(tmp_data_1.iloc[[0], [0]].values[0][0].replace(',', '').replace('--','0').replace('0-','0'))
                except:
                    a1 = float(tmp_data_1.iloc[[0], [0]].values[0][0])
                try:
                    a2 = float(tmp_data_1.iloc[[0], [1]].values[0][0].replace(',', '').replace('--','0').replace('0-','0'))
                except:
                    a2 = float(tmp_data_1.iloc[[0], [1]].values[0][0])
                try:
                    a3 = float(tmp_data_1.iloc[[0], [2]].values[0][0].replace(',', '').replace('--','0').replace('0-','0'))
                except:
                    a3 = float(tmp_data_1.iloc[[0], [2]].values[0][0])
                try:
                    a4 = float(tmp_data_1.iloc[[0], [3]].values[0][0].replace(',', '').replace('--','0').replace('0-','0'))
                except:
                    a4 = float(tmp_data_1.iloc[[0], [3]].values[0][0])
                try:
                    a5 = int(tmp_data_1.iloc[[0], [4]].values[0][0].replace(',', '').replace('--','0').replace('0-','0'))
                except:
                    a5 = int(tmp_data_1.iloc[[0], [4]].values[0][0])
                tmp_data_1 = pd.DataFrame([[time_tra, a1, a2, a3, a4, a5]],
                                          columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                if (a1+a2+a3+a4)==0:
                    print('%s,%s無開高收低，去除' % (date_1,id))
                    continue

            if sum(data_2[0] == id)>0:
                # tmp_data_2 = pd.concat([tmp_data_2, pd.DataFrame([time_tra])], axis=1)
                tmp_data_2.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                try:
                    a1 = float(tmp_data_2.iloc[[0], [0]].values[0][0].replace(',', '').replace('--','0').replace('0-','0'))
                except:
                    a1 = float(tmp_data_2.iloc[[0], [0]].values[0][0])
                try:
                    a2 = float(tmp_data_2.iloc[[0], [1]].values[0][0].replace(',', '').replace('--','0').replace('0-','0'))
                except:
                    a2 = float(tmp_data_2.iloc[[0], [1]].values[0][0])
                try:
                    a3 = float(tmp_data_2.iloc[[0], [2]].values[0][0].replace(',', '').replace('--','0').replace('0-','0'))
                except:
                    a3 = float(tmp_data_2.iloc[[0], [2]].values[0][0])
                try:
                    a4 = float(tmp_data_2.iloc[[0], [3]].values[0][0].replace(',', '').replace('--','0').replace('0-','0'))
                except:
                    a4 = float(tmp_data_2.iloc[[0], [3]].values[0][0])
                try:
                    a5 = int(tmp_data_2.iloc[[0], [4]].values[0][0].replace(',', '').replace('--','0').replace('0-','0'))
                except:
                    a5 = int(tmp_data_2.iloc[[0], [4]].values[0][0])
                tmp_data_2 = pd.DataFrame([[time_tra, a1, a2, a3, a4, a5]],
                                          columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                if (a1+a2+a3+a4)==0:
                    print('%s,%s無開高收低，去除' % (date_1,id))
                    continue
            if sum(data_1[0] == id)>0:
                sql = 'INSERT INTO daily_trade_tw VALUES({}, \'{}\', {}, {}, {},{},{});'.format(int(id), time_tra, a1, a2, a3, a4, a5)
                print(sql)
                #這邊編輯sql每列餵入資料
                cur.execute(sql)
                db.commit()
            if sum(data_2[0] == id)>0:
                sql = 'INSERT INTO daily_trade_two VALUES({}, \'{}\', {}, {}, {},{},{});'.format(int(id), time_tra, a1, a2, a3, a4, a5)
                print(sql)
                #這邊編輯sql每列餵入資料
                cur.execute(sql)
                db.commit()
            """
            if sum(data_1[0] == id) > 0 or sum(data_2[0] == id) > 0:
                if len(tmp_data_1)>0 :
                    print('上市')
                    try:
                        tmp1 = pd.read_csv("E:/股票資料/各股日資料2/%s.csv"%id)
                    except:
                        print('222222222222222222222222222222')
                        pd.DataFrame([], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume']).to_csv("E:/股票資料/各股日資料2/%s.csv"%id,index=False)
                        tmp1 = pd.read_csv("E:/股票資料/各股日資料2/%s.csv" % id)

                    data1_out = pd.concat([tmp1, tmp_data_1], axis=0).to_csv("E:/股票資料/各股日資料2/%s.csv"%id,index=False)


                if len(tmp_data_2)>0 :
                    print('上櫃')
                    try:
                        tmp2 = pd.read_csv("E:/股票資料/各股日資料2/%s.csv"%id)
                    except:
                        pd.DataFrame([], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume']).to_csv("E:/股票資料/各股日資料2/%s.csv"%id,index=False)
                        tmp2 = pd.read_csv("E:/股票資料/各股日資料2/%s.csv" % id)

                    data2_out = pd.concat([tmp2, tmp_data_2], axis=0).to_csv("E:/股票資料/各股日資料2/%s.csv"%id,index=False)
                print('%s,%s 已塞入資料csv'%(id,time_tra))
            """
        print('##########################%s已塞入########################'%time_tra)
        print(date_1,'is ok, have data')
        s = date_1+' is ok, have data'
        cur.close()
        db.close()
        return s
    else:
        print(date_1, 'is ok, but not data')
        s = date_1+' is ok, but not data'
        cur.close()
        db.close()
        return s


