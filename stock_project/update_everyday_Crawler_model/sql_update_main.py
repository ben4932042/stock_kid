import sql_update_one
import datetime
import time
import pandas as pd
from predict_random import rf_main
import margin_daily_update
import forei_score, Invers_score, Local_score
import redis_to_kafka
from google.cloud import storage


data_log = pd.DataFrame([])
DAY = 1
while True:
    if DAY ==1 :

        d1 = datetime.datetime.now()
        d2 = datetime.datetime(2020, 7, 16)# 啟動隔天日期
        time.sleep((d2 - d1).seconds-600) # 前一天10中前抓取 11.50

        #time.sleep(60)
        #date_1 = datetime.datetime.now().strftime('%Y%m%d')
        #debug
        date_1 = '20200715'#當天
        #..............
        start_time = datetime.datetime.now()
        a = sql_update_one.sql_update(date_1)
        b = rf_main.redis_rf_score_update(date_1)
        c = margin_daily_update.get_margin_daily(date_1)
        d = forei_score.forei_score(date_1)
        e = Invers_score.Invers_score(date_1)
        f = Local_score.Local_score(date_1)
        g = redis_to_kafka.redis_to_kafka(date_1)

        #b=date_1
        DAY = DAY+1
        with open('C:/Users/Big data/Desktop/upadate_log.txt', 'a') as f:
            f.write('mysql_5:%s;rf:%s;margin:%s;forei_score:%s;Invers_score:%s;Local_score:%s;redis_to_kafka:%s'%(a,b,c,d,e,f,g)+'\n')
        datetime_log = (datetime.datetime.today()).strftime("%Y-%m-%d_%H")

        storage_client = storage.Client.from_service_account_json('credential.json')
        bucket = storage_client.get_bucket('stock_daily_logs')
        blob = bucket.blob('logs/{}.txt'.format(str(datetime_log)))
        blob.upload_from_string('mysql_5:%s;rf:%s;margin:%s;forei_score:%s;Invers_score:%s;Local_score:%s;redis_to_kafka:%s'%(a,b,c,d,e,f,g))
        end_time = datetime.datetime.now()
        dif_time = (end_time - start_time).seconds
        print(datetime.datetime.now())
    else:
        time.sleep(86400-dif_time)
        start_time = datetime.datetime.now()
        #time.sleep(60)
        date_1 = datetime.datetime.now().strftime('%Y%m%d')
        a = sql_update_one.sql_update(date_1)
        b = rf_main.redis_rf_score_update(date_1)
        c = margin_daily_update.get_margin_daily(date_1)
        d = forei_score.forei_score(date_1)
        e = Invers_score.Invers_score(date_1)
        f = Local_score.Local_score(date_1)
        g = redis_to_kafka.redis_to_kafka(date_1)
        end_time = datetime.datetime.now()
        dif_time = (end_time-start_time).seconds
        #b = date_1
        DAY = DAY + 1
        with open('C:/Users/Big data/Desktop/upadate_log.txt', 'a') as f:
            f.write('mysql_5:%s;rf:%s;margin:%s;forei_score:%s;Invers_score:%s;Local_score:%s;redis_to_kafka:%s'%(a,b,c,d,e,f,g)+'\n')

        datetime_log = (datetime.datetime.today()).strftime("%Y-%m-%d_%H")
        storage_client = storage.Client.from_service_account_json('credential.json')
        bucket = storage_client.get_bucket('stock_daily_logs')
        blob = bucket.blob('logs/{}.txt'.format(str(datetime_log)))
        blob.upload_from_string('mysql_5:%s;rf:%s;margin:%s;forei_score:%s;Invers_score:%s;Local_score:%s;redis_to_kafka:%s'%(a,b,c,d,e,f,g))
    print(DAY)
    print(datetime.datetime.now())


