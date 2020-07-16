from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import redis
import datetime
from google.cloud import storage
storage_client = storage.Client.from_service_account_json('credential.json')
bucket = storage_client.get_bucket('kafka_confluent_logs')
r = redis.Redis(host=sever, port=port,decode_responses=True)
time_now = datetime.datetime.now().hour

Str = ""
type_list = [
'.2-1_10%內可接受範圍',  
'.2-3_50%內可接受範圍',    
'.2-4_100%以上',    
'.3-1_1個月內'   
'.3-4_1年以上',    
'.4-3_每月花費1~2小時',    
'.5-1_高風險高報酬',    
'.6-1_投資一定有風險',
'.7-1_我很積極布局',  
'.7-2_我要觀察一陣子', 
'.7-4_認賠殺出',  
'.8-1_我喜愛賺價差',
'.8-2_我喜愛超高報酬',
]


def error_cb(err):
    print('Error: %s' % err)


def try_decode_utf8(data):
    if data:
        return data.decode('utf-8')
    else:
        return None

def my_assign(consumer_instance, partitions):
    for p in partitions:
        p.offset = 0
    print('assign', partitions)
    consumer_instance.assign(partitions)


if __name__ == '__main__':
    props = {
        'bootstrap.servers': '',       
        'group.id': 'justforben123',                     
        'auto.offset.reset': 'earliest',             
        'session.timeout.ms': 6000,                  
        'error_cb': error_cb                         
    }
    consumer = Consumer(props)
    topicName = "2_resquest"
    consumer.subscribe([topicName], on_assign=my_assign)
    count = 0
    try:
        while True:
            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        sys.stderr.write('%% {} [{}] reached end at offset {} - {}\n'.format(record.topic(),
                                                                                             record.partition(),
                                                                                             record.offset()))

                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())

                    #############################################################################
#                   datetime_log = datetime.datetime.today().strftime("%Y-%m-%d")
                    log = '({}-{}-{} : ({} , {})'.format(topic, partition, offset, msgKey, msgValue)
                    print(log)
#                     with open('./{}-log.txt'.format(str(datetime_log)),'a') as outfile:
#                         outfile.write(str(log))
#                         outfile.write('\n')
                    Str+=log
                    Str+='\n'
                    time_next = datetime.datetime.now().hour
                    if time_now != time_next:
                    
                        datetime_log = (datetime.datetime.today()+ datetime.timedelta(hours = 8)).strftime("%Y-%m-%d_%H")
                        blob = bucket.blob('logs/{}.txt'.format(str(datetime_log)))
                        blob.upload_from_string(Str)
                        Str = ""
                        print('=-=-'*20)
                        print('kafka logs {} success load!'.format(datetime_log))
                        print('=-=-'*20)
                        ime_now = time_next
                   ############################################################################## 
                    
                    
                    if msgValue in type_list:
                        target = str(msgValue)
                        result_chinese=target.replace('.2-1_10%內可接受範圍','極端厭惡風險')\
                             .replace('.2-3_50%內可接受範圍','喜愛以小博大')\
                             .replace('.2-4_100%以上','賭性堅強')\
                             .replace('.3-1_1個月內','萬中選一的股神，或是選擇創業')\
                             .replace('.3-4_1年以上','適合投資ETF')\
                             .replace('.4-3_每月花費1~2小時','適合定存或儲蓄險')\
                             .replace('.5-1_高風險高報酬','萬中選一的股神，或是選擇創業')\
                             .replace('.6-1_投資一定有風險','適合投資ETF')\
                             .replace('.7-1_我很積極布局','適合當沖交易')\
                             .replace('.7-2_我要觀察一陣子','適合短線波段交易')\
                             .replace('.7-4_認賠殺出','適合投資ETF')\
                             .replace('.8-1_我喜愛賺價差','低買高賣')\
                             .replace('.8-2_我喜愛超高報酬','尋找成長10倍的公司')

                        result_num = result_chinese.replace('極端厭惡風險','type1')\
                                    .replace('喜愛以小博大','type2')\
                                    .replace('賭性堅強','type3')\
                                    .replace('萬中選一的股神，或是選擇創業','type4')\
                                    .replace('適合投資ETF' ,'type5')\
                                    .replace('賭性堅強','type3')\
                                    .replace('適合定存或儲蓄險','type6')\
                                    .replace('適合當沖交易','type7')\
                                    .replace('適合短線波段交易','type8')\
                                    .replace('低買高賣','type9')\
                                    .replace('尋找成長10倍的公司','type10')         
                        r.sadd('all_member',msgKey)
                        r.hset('attribute',msgKey,result_num)
                        print('#'*40)
                        print('UID:{}  ,type:{}'.format(msgKey,result_num))
                        print('#'*40)
#                         Dic = r.hgetall('attribute')
#                         pd.Series(Dic,Dic.keys()).to_frame()\
#                                                  .reset_index(drop=False)\
#                                                  .rename(columns={0:'attribute'})\
#                                                  .to_csv('./user_type.csv')
                            
                        

                        
                        
#                         tmp = (msgKey,result_num)
#                         datetime_dt = datetime.datetime.today()
#                         datetime_str = datetime_dt.strftime("%Y/%m/%d %H:%M:%S")
#                         print(str(tmp),datetime_str)
#                         ##############################################
#                         with open('./ID.txt','a') as outfile:
#                             outfile.write(str(tmp))
#                             outfile.write('\n')
                        ##############################################
    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(e)

    finally:
        # 步驟6.關掉Consumer實例的連線
        consumer.close()