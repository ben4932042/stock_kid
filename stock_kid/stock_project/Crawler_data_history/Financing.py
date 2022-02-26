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

# 取得所有股
tmp_list = Stockiid.values()
stock_iids = []
for i in tmp_list:
    i=i.replace(' ','')
    stock_iids.append(i)

# https://stock.capital.com.tw/z/zc/zcl/zcl.djhtm?a=1101&c=2018-1-1&d=2020-4-28
# https://stock.capital.com.tw/z/zc/zcl/zcl.djhtm?a=1101
headerlist = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
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
# url = 'https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=20200430&selectType=STOCK&_=1589002919516'
# url = 'https: // www.twse.com.tw / fund / T86?response = json & date = 20200504 & selectType = ALLBUT0999'
time_all = ['20200429','20200430']

for time_tmp in time_all:
        user_agent = random.choice(headerlist)
        headers = {'User-Agent': user_agent}

        # 融資融劵   上市
        url = 'https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=20200430&selectType=STOCK&_=1589002919516'
        # time.sleep(random.randint(0, 5))
        res = requests.get(url, headers=headers)
        # res.encoding = 'Big5'
        # soup = BeautifulSoup(res.text, 'html.parser')
        jdata = json.loads(res.text, encoding='utf-8')
        d1 = pd.DataFrame(jdata['data'])
        d1 = d1.drop([0], axis=0)
        d1.columns = ["股票代號", "股票名稱", "買進", "賣出", "現金償還", "前日餘額", "今日餘額", "限額", "買進", "賣出", "現券償還", "前日餘額", "今日餘額",
                      "限額", "資券互抵", "註記"]
        # 融資融劵   上櫃
        url = 'https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d=109/05/19&s=0,asc'
        # time.sleep(random.randint(0, 5))
        res = requests.get(url, headers=headers)
        res.encoding = 'utf-8'
        #soup = BeautifulSoup(res.text, 'html.parser')
        # res.encoding = 'Big5'
        # soup = BeautifulSoup(res.text, 'html.parser')
        jdata = json.loads(res.text, encoding='utf-8')
        d3 = pd.DataFrame(jdata['aaData'])
        d3.columns = ["股票代號", "股票名稱", "融資買進", "融資賣出", "融資現金償還", "融資前日餘額", "融資今日餘額", "融資限額", "融劵買進", "融劵賣出", "融劵現券償還", "融劵前日餘額", "融劵今日餘額",
                      "融劵限額", "資券互抵", "註記"]


        # 三大法人    上市
        url = 'https://www.twse.com.tw/fund/T86?response=json&date=20200430&selectType=ALLBUT0999'
        res = requests.get(url, headers=headers)
        jdata = json.loads(res.text, encoding='utf-8')
        d2 = pd.DataFrame(jdata['data'])
        d2.columns = ["股票代號", "證券名稱", "外陸資買進股數(不含外資自營商)", "外陸資賣出股數(不含外資自營商)", "外陸資買賣超股數(不含外資自營商)", "外資自營商買進股數",
                      "外資自營商賣出股數", "外資自營商買賣超股數", "投信買進股數", "投信賣出股數", "投信買賣超股數", "自營商買賣超股數", "自營商買進股數(自行買賣)",
                      "自營商賣出股數(自行買賣)", "自營商買賣超股數(自行買賣)", "自營商買進股數(避險)", "自營商賣出股數(避險)", "自營商買賣超股數(避險)", "三大法人買賣超股數"]
        df_combind = pd.concat([d1, d2], axis=1, join='inner')
        # time_tra = pd.to_datetime(time)
        timeTuple = time.strptime(time_tmp, "%Y%m%d")
        time_tra = time.strftime("%Y-%m-%d", timeTuple)
        j=0
        for cp in range(0,len(df_combind)):
            # 股票代號
            a1 = np.int(df_combind.iloc[[cp], [0]].values[0][0].replace(',',''))
            # 交易日期
            # time_tra
            # 融資買進
            a2 = np.int(df_combind.iloc[[cp], [2]].values[0][0].replace(',', ''))
            # 融資賣出
            a3 = np.int(df_combind.iloc[[cp], [3]].values[0][0].replace(',', ''))
            # 融資餘額
            a4 = np.int(df_combind.iloc[[cp], [6]].values[0][0].replace(',', ''))
            # 融資限額
            a5 = np.int(df_combind.iloc[[cp], [7]].values[0][0].replace(',', ''))
            # 融劵買進
            a6 = np.int(df_combind.iloc[[cp], [8]].values[0][0].replace(',', ''))
            # 融劵賣出
            a7 = np.int(df_combind.iloc[[cp], [9]].values[0][0].replace(',', ''))
            # 融劵餘額
            a8 = np.int(df_combind.iloc[[cp], [12]].values[0][0].replace(',', ''))
            # 融劵限額
            a9 = np.int(df_combind.iloc[[cp], [13]].values[0][0].replace(',', ''))
            # 外資買賣超股數
            a10 = np.int(df_combind.iloc[[cp], [21]].values[0][0].replace(',', ''))
            # 投信買賣超股數
            a11 = np.int(df_combind.iloc[[cp], [27]].values[0][0].replace(',', ''))
            # 自營商買賣超股數
            a12 = np.int(df_combind.iloc[[cp], [28]].values[0][0].replace(',', ''))
            sql = 'INSERT INTO margin_trading_short_selling VALUES({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});'.format(a1, time_tra, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12)
            j = j + 1
            print(j, sql)


