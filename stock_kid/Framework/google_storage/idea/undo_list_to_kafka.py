import Stockiids
from confluent_kafka import Producer
import sys
import time
import json

stock_dic = Stockiids.Stockiid
stock_list = [i for i in stock_dic.values()]

secretFile = json.load(open("secretFile.txt",'r'))
producer = None
server=secretFile['server']+':'+ secretFile['sever_port']
def kafkaproducer(server,topic,ID,query):
    global producer
    return_value = 0
    def error_cb(err):
        print('Error: %s' % err)
    props = {
        'bootstrap.servers': server,  
        'error_cb': error_cb
    }
    if producer is None:
        producer = Producer(props)
    topicName = topic
    try:
        producer.produce(topicName, key=ID, value=query)
        producer.flush()
        return_value+=1
    except:
        return_value = 0
    producer.flush()
    return return_value
    
for key,value in stock_dic.items():
    data = str({key:value})
    kafkaproducer(server=server,topic='pyETL',ID='stock',query=data)
    print('{} complete!'.format(data))
