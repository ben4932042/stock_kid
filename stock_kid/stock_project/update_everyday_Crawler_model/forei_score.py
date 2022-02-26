import pymysql
import pandas as pd
import numpy as np
import redis
def forei_score(date):
    All = [i for i in Stockiid.values()]

    r = redis.Redis(host='10.120.35.200', port=6379)
    db = pymysql.connect(host='10.120.35.27',port=3306,user='dbuser4',passwd='aabb1234',db='Project_test')
    for stockiid in All:
        cur = db.cursor()
        cur.execute("""select `date`, `foreign_investment` from margin_trading_short_selling 
                        where stockiid={} 
                        order by date desc limit 3;""".format(stockiid))
        Data = [i for i in cur.fetchall()]
        judge=0
        for data in Data:
            if data[1] <0:
                judge+=1
        if judge > 0:
            r.hset('STOCK_{}'.format(stockiid), 'foreign3', 0)
            print('redis_forei', stockiid,0)
        else:
            r.hset('STOCK_{}'.format(stockiid), 'foreign3', 1)
            print('redis_forei', stockiid,1)
    return date
