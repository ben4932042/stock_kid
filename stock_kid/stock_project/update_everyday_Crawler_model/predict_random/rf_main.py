import numpy as np
import pandas as pd
import joblib
import pymysql
import redis
from predict_random import rf_predict_oneday

def redis_rf_score_update(date):

    # 取得所有股票代碼
    tmp_list = Stockiid.values()
    stock_iids = []
    for i in tmp_list:
        i = i.replace(' ', '')
        stock_iids.append(i)

    r = redis.Redis(host='10.120.35.200', port=6379)
    for id in stock_iids:
        id = int(id)
        score = rf_predict_oneday.rf_predict_one(id)
        print(id , " is ok")
        r.hset('STOCK_%d'%id, 'model', score)
        print('%d'%id,' is ok,redis')
    return date
