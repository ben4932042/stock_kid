# -*- encoding: utf8 -*-
import redis
import numpy as np
import pandas as pd
from confluent_kafka import Producer
import datetime
import sys
import time
def redis_to_kafka(date):
    type1 = np.array([0.1, 0.4/6, 0.2, 0.1, 0.1, 0.1])
    type2 = np.array([0.5, 0.1/6, 0.2, 0.1, 0.05, 0.05])
    type3 = np.array([0.6, 0/6, 0, 0, 0, 0.4])
    type4 = np.array([0.1, 0.2/6, 0.5, 0.1, 0.1, 0])
    type5 = np.array([0.1, 0.1/6, 0.3, 0.2, 0.2, 0.1])
    type6 = np.array([0.2, 0.5/6, 0.1, 0, 0.1, 0.1])
    type7 = np.array([0.7, 0/6, 0.3, 0, 0, 0])
    type8 = np.array([0.8, 0.2/6, 0, 0, 0, 0])
    type9 = np.array([1, 0/6, 0, 0, 0, 0])
    type10 = np.array([0, 0.6/6, 0.1, 0.1, 0.1, 0.1])

    r = redis.Redis(host='10.120.35.200', port=6379,decode_responses=True)
    All = [i for i in Stockiid.values()]
    KAll = [i for i in Stockiid.keys()]  # <---return company name

    Dic_type={}
    for stockiid in All:
        try:
            s_dic = r.hgetall('STOCK_{}'.format(stockiid))
            svector = np.array([float(i) for i in s_dic.values()])
            score1 = np.inner(type1,svector)
            score2 = np.inner(type2,svector)
            score3 = np.inner(type3,svector)
            score4 = np.inner(type4,svector)
            score5 = np.inner(type5,svector)
            score6 = np.inner(type6,svector)
            score7 = np.inner(type7,svector)
            score8 = np.inner(type8,svector)
            score9 = np.inner(type9,svector)
            score10 = np.inner(type10,svector)
            #print(stockiid,score1,score2)
            Dic_type[stockiid] = [score1,score2,score3,score4,score5,score6,score7,score8,score9,score10]
        except ValueError as e:
            pass
    Stockiid_inverse ={}
    for item in KAll:
        Stockiid_inverse[Stockiid[item]]='{}-{}'.format(Stockiid[item],item)

    df = pd.DataFrame(Dic_type).rename(columns=Stockiid_inverse).T.rename(columns={0:'type1',1:'type2',2:'type3',3:'type4',4:'type5',5:'type6',6:'type7',7:'type8',8:'type9',9:'type10'})

    rank_type1 = pd.DataFrame(df.sort_values(['type1'],ascending=False)['type1']).head(10).to_dict()['type1']
    rank_type2 = pd.DataFrame(df.sort_values(['type2'],ascending=False)['type2']).head(10).to_dict()['type2']
    rank_type3 = pd.DataFrame(df.sort_values(['type3'],ascending=False)['type3']).head(10).to_dict()['type3']
    rank_type4 = pd.DataFrame(df.sort_values(['type4'],ascending=False)['type4']).head(10).to_dict()['type4']
    rank_type5 = pd.DataFrame(df.sort_values(['type5'],ascending=False)['type5']).head(10).to_dict()['type5']
    rank_type6 = pd.DataFrame(df.sort_values(['type6'],ascending=False)['type6']).head(10).to_dict()['type6']
    rank_type7 = pd.DataFrame(df.sort_values(['type7'],ascending=False)['type7']).head(10).to_dict()['type7']
    rank_type8 = pd.DataFrame(df.sort_values(['type8'],ascending=False)['type8']).head(10).to_dict()['type8']
    rank_type9 = pd.DataFrame(df.sort_values(['type9'],ascending=False)['type9']).head(10).to_dict()['type9']
    rank_type10 = pd.DataFrame(df.sort_values(['type10'],ascending=False)['type10']).head(10).to_dict()['type10']


    #iproducer = None
    def kafkaproducer(server=KAFKA_HOST,topic='test_request',ID='User_ID',query='test'):
        #global iproducer
        return_value = 0
        def error_cb(err):
            print('Error: %s' % err)
        props = {
            'bootstrap.servers': server,  # <-- 置換成要連接的Kafka集群
            'error_cb': error_cb                    # 設定接收error訊息的callback函數
        }
        # 步驟2. 產生一個Kafka的Producer的實例
        #if iproducer is None:
        iproducer = Producer(props)
        # 步驟3. 指定想要發佈訊息的topic名稱
        topicName = topic
        try:
            # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
            iproducer.produce(topicName, key=ID, value=query)
            iproducer.flush()
            return_value+=1
        except:
            return_value = 0
        iproducer.flush()
        return return_value
    time_tmp = datetime.datetime.now().strftime("%Y%m%d")

    kafkaproducer(server='35.236.145.238',topic='promote_stock',ID='type1_%s'%time_tmp,query=str(rank_type1))
    kafkaproducer(server='35.236.145.238',topic='promote_stock',ID='type2_%s'%time_tmp,query=str(rank_type2))
    kafkaproducer(server='35.236.145.238',topic='promote_stock',ID='type3_%s'%time_tmp,query=str(rank_type3))
    kafkaproducer(server='35.236.145.238',topic='promote_stock',ID='type4_%s'%time_tmp,query=str(rank_type4))
    kafkaproducer(server='35.236.145.238',topic='promote_stock',ID='type5_%s'%time_tmp,query=str(rank_type5))
    kafkaproducer(server='35.236.145.238',topic='promote_stock',ID='type6_%s'%time_tmp,query=str(rank_type6))
    kafkaproducer(server='35.236.145.238',topic='promote_stock',ID='type7_%s'%time_tmp,query=str(rank_type7))
    kafkaproducer(server='35.236.145.238',topic='promote_stock',ID='type8_%s'%time_tmp,query=str(rank_type8))
    kafkaproducer(server='35.236.145.238',topic='promote_stock',ID='type9_%s'%time_tmp,query=str(rank_type9))
    kafkaproducer(server='35.236.145.238',topic='promote_stock',ID='type10_%s'%time_tmp,query=str(rank_type10))
    """
    print(type(aa1))
    print(type(aa2))
    print(type(aa3))
    print(type(aa4))
    print(type(aa5))
    print(type(aa6))
    print(type(aa7))
    print(type(aa8))
    print(type(aa9))
    print(type(aa10))
    """
    print('redis_to_kafka is ok')
    return date
#print(push)
