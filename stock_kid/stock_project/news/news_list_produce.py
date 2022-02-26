from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka import Producer
import sys
import time
import json


def news_list_produce(date,ip,port):

    stock_dic = Stockiid
    stock_list = [i for i in stock_dic.values()]

    producer = None
    server='%s:%s'%(ip, port)
    def kafkaproducer(server,topic,ID,query,partition):
        # global producer
        return_value = 0
        def error_cb(err):
            print('Error: %s' % err)
        props = {
            'bootstrap.servers': server,
            'error_cb': error_cb
        }

        producer = Producer(props)
        topicName = topic
        try:
            producer.produce(topicName, key=ID, value=query,partition=partition)
            producer.poll(1)
            producer.flush(10)
            return_value+=1
        except:
            return_value = 0
        producer.flush(10)
        return return_value

    index = 0
    for key,value in stock_dic.items():
        data = str({key:value})
        if index%2==0:
            kafkaproducer(server=server,topic='PyETLbeta3',ID='stock',query=data,partition=0)
            print('{} complete insert to partition 0!'.format(data))
        else:
            kafkaproducer(server=server,topic='PyETLbeta3',ID='stock',query=data,partition=1)
            print('{} complete!insert to partition 1!'.format(data))
        index+=1
    return date
