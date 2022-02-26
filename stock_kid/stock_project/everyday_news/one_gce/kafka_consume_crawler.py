import requests
from bs4 import BeautifulSoup
import pandas as pd
from google.cloud import storage
import random
from confluent_kafka import Consumer, KafkaException, KafkaError
storage_client = storage.Client()
bucket = storage_client.get_bucket('stock_news')
consumer = None
secretFile = json.load(open("secretFile.txt",'r'))
server=secretFile['server']+':'+ secretFile['sever_port']
def kafkaconsumer(server=server,groupid ='conseumer',topic='test_request1',ID='User_ID'):
    global consumer
    def try_decode_utf8(data):
        if data:
            return data.decode('utf-8')
        else:
            return None

    def my_assign(consumer_instance, partitions):
        for p in partitions:
            p.offset = 0
        consumer_instance.assign(partitions)

    def error_cb(err):
        pass

    props = {
        'bootstrap.servers': server,
        'group.id': groupid,   
        'auto.offset.reset': 'earliest',    
        'session.timeout.ms': 6000,    
        'error_cb': error_cb
    }
    return_answer ={}
    if consumer is None:
        consumer = Consumer(props)
    topicName = topic
    consumer.subscribe([topicName])
    records = []
    while len(records)==0:
        records = consumer.consume(num_messages=1)
        if records is None:
            continue
        for record in records:
            if record is None:
                continue
            if record.error():
                continue
            else:
                msgKey = try_decode_utf8(record.key())
                msgValue = try_decode_utf8(record.value())
                
                if str(msgKey) != ID:
                    records = []
                else:
                    return_answer[msgKey] = msgValue
                
#     consumer.close()
    return return_answer



# for i in range(200):
while True:
    do_data = kafkaconsumer(server=server,groupid ='testing',topic='pyETL',ID='stock')
    Dict = eval(do_data['stock'])
    stock_name = [i for i in Dict.keys()][0]
    stock_iid = [i for i in Dict.values()][0]

    headerlist = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36 OPR/43.0.2442.991",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36 OPR/42.0.2393.94",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36 OPR/47.0.2631.39",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko"
                ]
    cookies = {
            'GED_PLAYLIST_ACTIVITY':'W3sidSI6ImpjSjciLCJ0c2wiOjE1OTQ1MzUzODEsIm52IjoxLCJ1cHQiOjE1OTQ1MzM2NTQsImx0IjoxNTk0NTM1MzgxfV0.',
            'gliaplayer_ssid':'add53ac0-c403-11ea-a0ee-55725f3cbfcc',
            'gliaplayer_uid':'add4eca0-c403-11ea-a0ee-55725f3cbfcc',
            'gliaplayer_user_info':'{%22city%22:%22shinjuku%20city%22%2C%22ip%22:%222001:b400:e353:4cee:f814:ecb4:cda5:35eb%22%2C%22region%22:%2213%22%2C%22source%22:%22CF%22%2C%22latlong%22:%2235.693825%2C139.703356%22%2C%22country%22:%22TW%22}'
                }
    print('Start to clawer {}-{}'.format(stock_name,stock_iid))
    for page in range(1,3):
        url = "https://ess.api.cnyes.com/search/api/v1/news/keyword?q={}&limit=10&page={}".format(stock_name,page)
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
        res = requests.get(url, headers=headers, cookies=cookies)
        data = pd.read_json(res.text)
        data = data.drop(columns = ['statusCode','message']).T.to_dict()
        for subdata in data['items']['data']:
            user_agent = random.choice(headerlist)
            headers = {'User-Agent': user_agent}
            try:
                news_title = subdata['title'].replace('<mark>','').replace('</mark>','')
                newID = subdata['newsId']
                newsurl = 'https://news.cnyes.com/news/id/{}?exp=a'.format(newID)
                news_summary = subdata['summary']
                article_res = requests.get(newsurl, headers=headers, cookies=cookies)
                article_soup = BeautifulSoup(article_res.text, 'html.parser')
                news_date = article_soup.select('div[id="content"]')[0].time["datetime"].split('T')[0]
                news_content = article_soup.select('div[itemprop="articleBody"]')[0].text
                result = str({'title':news_title,
                         'time':news_date,
                         'summary':news_summary,
                         'content':news_content,
                         'url':newsurl})
                file_name = '{}_{}'.format(news_date,newID)
                blob = bucket.blob('{}/{}.json'.format(stock_iid,file_name))
                blob.upload_from_string(result)
                result = ''
                print('{} success!'.format(news_title))

            except:
                """
                
                kakfa reproduce
                
                """
                break # undefined
    print('='*60)