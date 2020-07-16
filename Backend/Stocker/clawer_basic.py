import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import stockid


undo_List = []
path = './basic_csv'
if not os.path.exists( path ):
    os.mkdir( path )
def catch_basic(id):
    id = str(id)
    url = 'https://goodinfo.tw/StockInfo/BasicInfo.asp?RPT_CAT=BS_M_QUAR&STOCK_ID={}'.format(id)
    res = requests.get(url=url, headers=headers)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    a = soup.select('table[class="solid_1_padding_4_6_tbl"]')
    df_list = pd.read_html( str( a[0] ) )
    df = pd.DataFrame( df_list[0] )
    df.to_csv('./basic_csv/{}.csv'.format(id))

undo_List = [x for x in stockid.Stockiid.values()]
c = 1
while True:
    n,y = 0,0

    if len(undo_List) == 0:
        print('finish')
        break

    for id in undo_List:
        try:
            catch_basic( id )
            print(id,'完成')
            y +=1
            undo_List.remove(id)
        except:
            n +=1
    c +=1

    print('第',c,'輪  -------','完成：',y,'   未完成：',n)
    print('剩下',len(undo_List),'支要抓取。')
    print('-'*80)


