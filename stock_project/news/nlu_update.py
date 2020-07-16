import datetime
import time
import pandas as pd
from google.cloud import storage
import test_NLU3_main_gcs_final
import joblib
import redis
import news_list_produce

r = redis.Redis(host='10.120.35.200', port=6379)
rf = joblib.load('C:/Users/Big data/Desktop/nlu_result/nlu_model.pkl')
# data = pd.read_csv("C:/Users/Big data/Desktop/nlu_result/ori_data2.csv")
data = pd.read_csv("C:/Users/Big data/Desktop/nlu_result/ori_data2.csv", chunksize=1)
for df in data:
    data = df.drop(['Unnamed: 0'], axis=1)
    if len(df) > 0:
        break


data_log = pd.DataFrame([])
DAY = 1
while True:
    if DAY ==1 :

        d1 = datetime.datetime.now()
        d2 = datetime.datetime(2020, 7, 15)# 啟動隔天日期
        time.sleep((d2 - d1).seconds-10800) # 前一天10中前抓取 11.00

        #time.sleep(60)
        #date_1 = datetime.datetime.now().strftime('%Y%m%d')
        #debug
        date_1 = '20200714'#當天
        #..............
        start_time = datetime.datetime.now()
        a0 = news_list_produce.news_list_produce(date_1,'104.199.192.228','9092')
        a = test_NLU3_main_gcs_final.nlu_update(date_1,rf,data,r)


        #b=date_1
        DAY = DAY+1
        with open('C:/Users/Big data/Desktop/update_nlu.txt', 'a') as f:
            f.write('catch_news:%s, '%a0+ 'nlu:%s'%a+ '/n')
        datetime_log = (datetime.datetime.today()).strftime("%Y-%m-%d_%H")

        storage_client = storage.Client.from_service_account_json('credential.json')
        bucket = storage_client.get_bucket('stock_daily_logs')
        blob = bucket.blob('logs/{}.txt'.format(str(datetime_log)+'nlu'))
        blob.upload_from_string('catch_news:%s, '%a0+ 'nlu:%s'%a)
        end_time = datetime.datetime.now()
        dif_time = (end_time - start_time).seconds
        print(datetime.datetime.now())
    else:
        time.sleep(86400-dif_time)
        start_time = datetime.datetime.now()
        #time.sleep(60)
        date_1 = datetime.datetime.now().strftime('%Y%m%d')
        a0 = news_list_produce.news_list_produce(date_1, '104.199.192.228', '9092')
        a = test_NLU3_main_gcs_final.nlu_update(date_1,rf,data,r)

        end_time = datetime.datetime.now()
        dif_time = (end_time-start_time).seconds
        #b = date_1
        DAY = DAY + 1
        with open('C:/Users/Big data/Desktop/update_nlu.txt', 'a') as f:
            f.write('catch_news:%s, '%a0+ 'nlu:%s'%a+ '/n')

        datetime_log = (datetime.datetime.today()).strftime("%Y-%m-%d_%H")
        storage_client = storage.Client.from_service_account_json('credential.json')
        bucket = storage_client.get_bucket('stock_daily_logs')
        blob = bucket.blob('logs/{}.txt'.format(str(datetime_log)+'nlu'))
        blob.upload_from_string('catch_news:%s, '%a0+ 'nlu:%s'%a)
    print(DAY)
    print(datetime.datetime.now())
