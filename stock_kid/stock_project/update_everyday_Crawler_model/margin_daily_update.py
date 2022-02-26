# -*- encoding: utf8 -*-
import requests
import pandas as pd
import numpy as np
import time
import random
import os
import datetime
import json
import pymysql


def get_margin_daily(atime):
    # 要改
    db = pymysql.connect(host='localhost',port=3306,user='dbuser6',passwd='aabb1234',db='Project_test')
    cur = db.cursor()

:    # 取得所有股票代碼
    tmp_list = Stockiid.values()
    stock_iids = []
    for i in tmp_list:
        i = i.replace(' ', '')
        stock_iids.append(i)

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
            dateend = datetime.datetime.now().strftime( '20200525' )

        datestart = datetime.datetime.strptime( datestart, '%Y%m%d' )
        dateend = datetime.datetime.strptime( dateend, '%Y%m%d' )
        date_list = []
        date_list.append( datestart.strftime( '%Y%m%d' ) )
        while datestart < dateend:
            datestart += datetime.timedelta( days=+1 )
            date_list.append( datestart.strftime( '%Y%m%d' ) )
        return date_list
    time_all = create_assist_date(datestart = '{}'.format(atime), dateend = '{}'.format(atime))

    for time_tmp in time_all:
        tt = turnyear(time_tmp)
        user_agent = random.choice(headerlist)
        headers = {'User-Agent': user_agent}
        # 融資融劵   上市
        try:
            url = 'https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=%s&selectType=STOCK&_=1589002919516' %time_tmp
            # time.sleep(random.randint(0, 5))
            res = requests.get(url, headers=headers)
            # res.encoding = 'Big5'
            # soup = BeautifulSoup(res.text, 'html.parser')
            jdata = json.loads(res.text, encoding='utf-8')
            d1 = pd.DataFrame(jdata['data'])
            d1 = d1.drop([0], axis=0)
            d1.columns = ["股票代號", "股票名稱", "融資買進", "融資賣出", "融資現金償還", "融資前日餘額", "融資今日餘額", "融資限額", "融劵買進", "融劵賣出", "融劵現券償還", "融劵前日餘額", "融劵今日餘額",
                          "融劵限額", "資券互抵", "註記"]
        except:
            continue
        # 融資融劵   上櫃
        url = 'https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d=%s&s=0,asc' %tt
        # time.sleep(random.randint(0, 5))
        res = requests.get(url, headers=headers)
        res.encoding = 'utf-8'
        #soup = BeautifulSoup(res.text, 'html.parser')
        # res.encoding = 'Big5'
        # soup = BeautifulSoup(res.text, 'html.parser')
        jdata = json.loads(res.text, encoding='utf-8')
        d3 = pd.DataFrame(jdata['aaData'])
        d3.columns = ["股票代號","股票名稱","融資前日餘額","融資買進","融資賣出","融資現金償還","融資今日餘額","融資證金","資使用率","融資限額","融劵前日餘額","融劵買進","融劵賣出","融劵現金償還","融劵今日餘額","融劵證金","劵使用率","融劵限額","資劵相抵","註記"]
        # 融資融劵 合併
        d1_d3_cob = pd.concat([d1, d3], axis=0, join='inner', ignore_index=True)
        d1_d3_cob_sel = d1_d3_cob[d1_d3_cob["股票代號"].isin(stock_iids)]#############################
        # 三大法人    上市
        url = 'https://www.twse.com.tw/fund/T86?response=json&date=%s&selectType=ALLBUT0999' %time_tmp
        res = requests.get(url, headers=headers)
        jdata = json.loads(res.text, encoding='utf-8')
        d2 = pd.DataFrame(jdata['data'])
        try:
            d2.columns = ["股票代號", "證券名稱", "外陸資買進股數(不含外資自營商)", "外陸資賣出股數(不含外資自營商)", "外陸資買賣超股數(不含外資自營商)", "外資自營商買進股數",
                          "外資自營商賣出股數", "外資自營商買賣超股數", "投信買進股數", "投信賣出股數", "投信買賣超股數", "自營商買賣超股數", "自營商買進股數(自行買賣)",
                          "自營商賣出股數(自行買賣)", "自營商買賣超股數(自行買賣)", "自營商買進股數(避險)", "自營商賣出股數(避險)", "自營商買賣超股數(避險)", "三大法人買賣超股數"]
        except:
            d2.columns = ["股票代號","證券名稱","外陸資買進股數(不含外資自營商)", "外陸資賣出股數(不含外資自營商)", "外陸資買賣超股數(不含外資自營商)","投信買進股數","投信賣出股數","投信買賣超股數",
                          "自營商買賣超股數","自營商買進股數(自行買賣)","自營商賣出股數(自行買賣)","自營商買賣超股數(自行買賣)","自營商買進股數(避險)","自營商賣出股數(避險)",
                          "自營商買賣超股數(避險)","三大法人買賣超股數"]
        # 三大法人    上櫃
        url = 'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=json&se=EW&t=D&d=%s&s=0,asc' %tt
        res = requests.get(url, headers=headers)
        jdata = json.loads(res.text, encoding='utf-8')
        d4 = pd.DataFrame(jdata['aaData'])
        try:
            d4.columns = ["股票代號", "證券名稱", "外陸資買進股數(不含外資自營商)", "外陸資賣出股數(不含外資自營商)", "外陸資買賣超股數(不含外資自營商)", "外資自營商買進股數",
                          "外資自營商賣出股數", "外資自營商買賣超股數", "外陸買進", "外陸賣出", "外陸買賣超", "投信買進股數", "投信賣出股數", "投信買賣超股數", "自營商買進股數(自行買賣)",
                          "自營商賣出股數(自行買賣)", "自營商買賣超股數(自行買賣)", "自營商買進股數(避險)", "自營商賣出股數(避險)", "自營商買賣超股數(避險)", "自營商買進", "自營商賣出",
                          "自營商買賣超股數", "三大法人買賣超股數", "備註"]
        except:
            d4.columns = ["股票代號", "證券名稱", "外陸資買進股數(不含外資自營商)", "外陸資賣出股數(不含外資自營商)", "外陸資買賣超股數(不含外資自營商)",
                          "投信買進股數", "投信賣出股數", "投信買賣超股數","自營商買賣超股數","自營商買進股數(自行買賣)",
                          "自營商賣出股數(自行買賣)", "自營商買賣超股數(自行買賣)","自營商買進股數(避險)", "自營商賣出股數(避險)", "自營商買賣超股數(避險)",
                          "三大法人買賣超股數", "備註"]

        # 三大法人合併
        d2_d4_cob = pd.concat([d2, d4], axis=0, join='inner', ignore_index=True)
        d2_d4_cob_sel = d2_d4_cob[d2_d4_cob["股票代號"].isin(stock_iids)]


        # 三大法人和融資融劵合併
        df_combind = pd.merge(d1_d3_cob_sel, d2_d4_cob_sel, on='股票代號', how='outer')
        df_combind_no_na = df_combind.fillna(0)

        # time_tra = pd.to_datetime(time)
        timeTuple = time.strptime(time_tmp, "%Y%m%d")
        time_tra = time.strftime("%Y-%m-%d", timeTuple)
        j = 0
        for cp in range(0, len(df_combind_no_na)):
            # 股票代號
            try:
                a1 = np.int(df_combind_no_na.loc[[cp], ['股票代號']].values[0][0].replace(',',''))
            except:
                a1 = df_combind_no_na.loc[[cp], ['股票代號']].values[0][0]
            # 交易日期
            # time_tra
            # 融資買進
            try:
                a2 = np.int(df_combind_no_na.loc[[cp], ['融資買進']].values[0][0].replace(',', ''))
            except:
                a2 = df_combind_no_na.loc[[cp], ['融資買進']].values[0][0]
            # 融資賣出
            try:
                a3 = np.int(df_combind_no_na.loc[[cp], ['融資賣出']].values[0][0].replace(',', ''))
            except:
                a3 = df_combind_no_na.loc[[cp], ['融資賣出']].values[0][0]
            # 融資餘額
            try:
                a4 = np.int(df_combind_no_na.loc[[cp], ['融資今日餘額']].values[0][0].replace(',', ''))
            except:
                a4 = df_combind_no_na.loc[[cp], ['融資今日餘額']].values[0][0]
            # 融資限額
            try:
                a5 = np.int(df_combind_no_na.loc[[cp], ['融資限額']].values[0][0].replace(',', ''))
            except:
                a5 = df_combind_no_na.loc[[cp], ['融資限額']].values[0][0]
            # 融劵買進
            try:
                a6 = np.int(df_combind_no_na.loc[[cp], ['融劵買進']].values[0][0].replace(',', ''))
            except:
                a6 = df_combind_no_na.loc[[cp], ['融劵買進']].values[0][0]
            # 融劵賣出
            try:
                a7 = np.int(df_combind_no_na.loc[[cp], ['融劵賣出']].values[0][0].replace(',', ''))
            except:
                a7 = df_combind_no_na.loc[[cp], ['融劵賣出']].values[0][0]
            # 融劵餘額
            try:
                a8 = np.int(df_combind_no_na.loc[[cp], ['融劵今日餘額']].values[0][0].replace(',', ''))
            except:
                a8 = df_combind_no_na.loc[[cp], ['融劵今日餘額']].values[0][0]
            # 融劵限額
            try:
                a9 = np.int(df_combind_no_na.loc[[cp], ['融劵限額']].values[0][0].replace(',', ''))
            except:
                a9 = df_combind_no_na.loc[[cp], ['融劵限額']].values[0][0]
            # 外資買賣超股數
            try:
                a10 = np.int(df_combind_no_na.loc[[cp], ['外陸資買賣超股數(不含外資自營商)']].values[0][0].replace(',', ''))
            except:
                a10 = df_combind_no_na.loc[[cp], ['外陸資買賣超股數(不含外資自營商)']].values[0][0]
            # 投信買賣超股數
            try:
                a11 = np.int(df_combind_no_na.loc[[cp], ['投信買賣超股數']].values[0][0].replace(',', ''))
            except:
                a11 = df_combind_no_na.loc[[cp], ['投信買賣超股數']].values[0][0]
            # 自營商買賣超股數
            try:
                a12 = np.int(df_combind_no_na.loc[[cp], ['自營商買賣超股數']].values[0][0].replace(',', ''))
            except:
                a12 = df_combind_no_na.loc[[cp], ['自營商買賣超股數']].values[0][0]
            s = [a1, time_tra, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12]
            sql = 'INSERT INTO margin_trading_short_selling VALUES({}, \'{}\', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});'.format(a1, str(time_tra), a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12)
            j = j + 1
            print(j, sql)
            cur.execute(sql)
            db.commit()
    cur.close()
    db.close()
    return(atime)
