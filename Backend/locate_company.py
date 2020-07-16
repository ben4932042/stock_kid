import requests
import pandas as pd
from bs4 import BeautifulSoup
import stockid
import random
import datetime



Stockid = stockid.Stockiid.values()

def get_html_one4(id):
    id = int(id)
    user_agent = ''
    headers = {'User-Agent': user_agent}
    #proxy = random.choice(proxylist)
    #proxies = {'http': 'http://' + proxy, 'https': 'https://' + proxy}
    url = 'https://goodinfo.tw/StockInfo/BasicInfo.asp?STOCK_ID=%d' % id
    res = requests.get(url, headers=headers)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    a = soup.select('table[class="solid_1_padding_4_6_tbl"]')
    # print(a[0])
    df_list = pd.read_html(str(a[0]))
    df = pd.DataFrame(df_list[0])
    json = df.to_dict()
    locate =  json[1][20]
    return locate

Dic ={}
for stock in Stockid :
    Dic[stock] =  get_html_one4(stock)
    print(stock)



with open('locate.json','w') as outfile:
    outfile.write(Dic)
