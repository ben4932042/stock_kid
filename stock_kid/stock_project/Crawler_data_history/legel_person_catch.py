# -*- encoding: utf8-*-
import threading
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import random
import stockid
import os

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
threads = []
Stockiid_broker = stockid.Stock_Broker['證券商代號']

Stockiid = stockid.Stockiid
# 取得所有股票代碼
tmp_list = Stockiid.values()
stock_iids = []
for i in tmp_list:
    stock_iids.append(i)

proxies = {
    "http": "http://167.99.54.39:8888",
    "http": "http://123.1.170.138:3128",
    "http": "http://217.112.83.235:3128",
    "http": "http://36.91.207.109:59362",
    "http": "http://41.207.54.154:443",
    "http": "http://51.159.52.158:5836",
    "http": "163.172.204.28:59362:5836",
    "http": "14.140.131.82:3128"
}




def broker_thread(start_num, end_num, iter):
    l = len(stock_iids[start_num:end_num]) * len(Stockiid_broker)
    ll = 1
    for company_id in stock_iids[start_num:end_num]:  # :1721
        try:
            os.mkdir('E:/分點券商/%s' % company_id)
        except:
            print(company_id,'pass')
            continue
        brocker_in = []
        for broker in Stockiid_broker:
            user_agent = random.choice(headerlist)
            headers = {'User-Agent': user_agent}
            url = 'https://jdata.yuanta.com.tw/z/zc/zco/zco0/zco0.djhtm?A=%s&BHID=%s&b=%s&D=2016-1-4&E=2020-5-8' % (
                company_id, broker, broker)
            res = requests.get(url, headers=headers, proxies=proxies)
            res.encoding = 'big5'
            soup = BeautifulSoup(res.text, 'html.parser')
            a = soup.select('table[id="oMainTable"]')

            if a != []:
                df_list = pd.read_html(str(a[0]))
                df = pd.DataFrame(df_list[0])
                df.to_json('E:/分點券商/%s/%s.json' % (company_id, broker),
                           orient='index', force_ascii=False)
                """
                df.columns = ['日期', '買進(張)', '賣出(張)', '買賣總額(張)', '買賣超(張)']
                df = df.drop([0], axis=0)
                df.set_index("日期", inplace=True)
                df.to_json('E:/分點券商/%s/%s2.json' % (company_id, broker),
                           orient='index', force_ascii=False)
                """
                brocker_in = brocker_in + [broker]
            aaa = ll / l
            print('mainjob:%d,' %iter, company_id, 'and', broker, 'is ok,整體完整率:', '%.10f' % aaa,
                  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            ll = ll + 1
            time.sleep(random.randint(0, 1))
        brocker_in_pd = pd.DataFrame(brocker_in)
        brocker_in_pd.to_csv('E:/分點券商/%s/%s投資券商名冊.csv' % (company_id, company_id),
                             index=1, encoding='utf_8_sig')


def main():
    # x 奴隸數 a 奴隸工作範圍
    x = 8
    a = 215
    for i in range(x):
        # 建立子執行緒
        threads.append(threading.Thread(target=broker_thread, args=(i * a, i * a + a - 1, i)))
        # 執行該子執行緒
        threads[i].start()

    for i in range(x):
        threads[i].join()
    print("全部結束")


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print("CPU Time: ", end - start)