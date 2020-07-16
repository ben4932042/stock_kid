import jieba
from PIL import Image
import numpy as np
import pandas as pd
import re
from sklearn.utils import shuffle
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
import pymongo
import joblib


jieba.set_dictionary('C:/Users/User/Desktop/dict.txt')
with open('C:/Users/User/Desktop/stop.txt', 'r', encoding='utf8') as f:  # 中文的停用字，我也忘記從哪裡拿到的，效果還可以，繁體字的資源真的比較少，大家將就一下吧
    stops = f.read().split('\n')



client = pymongo.MongoClient('mongodb://0.tcp.ngrok.io:14077/')
db = client.stock
collection = db.pos_news
result = collection.find()
news = [d for d in result]
postitle = [new['news_content'] for new in news][0:8000]

client = pymongo.MongoClient('mongodb://0.tcp.ngrok.io:14077/')
db = client.stock
collection = db.neg_news
result = collection.find()
news = [d for d in result]
negtitle = [new['news_content'] for new in news][0:8000]
# 去除英文及數字及特殊符號
def remove(text):
    text = re.sub('[A-Za-z]+', '', text)
    text = re.sub('\d+', '', text)
    text = re.sub('[’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~：﹗；]', '', text)
    return text
positive_reviews = []
negative_reviews = []
for i in range(len(postitle)):
    positive_reviews = positive_reviews+[remove(postitle[i])]
for i in range(len(negtitle)):
    negative_reviews = negative_reviews+[remove(negtitle[i])]
positive_reviews = shuffle(positive_reviews)#[0:2000]
negative_reviews = shuffle(negative_reviews)#[0:2000]
#del postitle
#del negtitle
"""
positive_reviews = []
positive_data = pd.read_csv('C:/Users/User/Desktop/positive_news.csv', encoding = 'ANSI')
negative_reviews = []
negative_data = pd.read_csv('C:/Users/User/Desktop/negtive_news.csv', encoding = 'ANSI')
# 去除英文及數字及特殊符號
def remove(text):
    text = re.sub('[A-Za-z]+', '', text)
    text = re.sub('\d+', '', text)
    text = re.sub('[’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~：﹗；]', '', text)
    return text

for i in range(len(positive_data)):
    positive_reviews = positive_reviews+[remove(str(positive_data['新聞內文'][i]))]
for i in range(len(negative_data)):
    negative_reviews = negative_reviews+[remove(str(negative_data['新聞內文'][i]))]
"""

def my_tokenizer(s):
    tokens = [t for t in jieba.cut(s) if t not in stops]
    tokens = [t for t in tokens if len(t) == 2]
    return tokens

word_index_map = {}
current_index = 0
positive_tokenized = []
negative_tokenized = []
orig_reviews = []

# 在做斷字斷詞index 共有幾個不同字
# start to process postive_reviews text
for review in positive_reviews:
    orig_reviews.append(review)
    tokens = my_tokenizer(review)
    positive_tokenized.append(tokens)# 正面文章所有的字詞，包成list
    for token in tokens:
        if token not in word_index_map:
            word_index_map[token] = current_index
            current_index += 1
    #break
#start to process negative_reviews text
for review in negative_reviews:
    orig_reviews.append(review)
    tokens = my_tokenizer(review)
    negative_tokenized.append(tokens)
    for token in tokens:
        if token not in word_index_map:
            word_index_map[token] = current_index
            current_index += 1
    #break
#del negative_reviews
#del positive_reviews
#del news
def tokens_to_vector(tokens, label):
    x = np.zeros(len(word_index_map) + 1) # last element is for the label (1:positive, 0:negative)
    # 這邊在數字詞的數量
    for t in tokens:
        i = word_index_map[t]
        x[i] += 1
    # x = x / x.sum() # normalize it before setting label# 用次數標準化詞向量
    x[-1] = label # 貼標
    return x

# 轉成 文章數量*所有詞量  的矩陣
N = len(positive_tokenized) + len(negative_tokenized)
#data=N x D+1 matrix
data = np.zeros((N, len(word_index_map) + 1))#,dtype='uint8'

# 下面兩個 文章的字詞數次數放入矩陣並且貼標
#process postive_tokenized to matrix
#the last column is 1
j = 0
for tokens in positive_tokenized:
    xy = tokens_to_vector(tokens, 1)
    data[j, :] = xy
    j += 1

#process negative_tokenized to matrix
#the last column is 0
for tokens in negative_tokenized:
    xy = tokens_to_vector(tokens, 0)
    data[j, :] = xy
    j += 1
# 處理資料
data = pd.DataFrame(data)
data = data.dropna(axis=0)
data.columns = list(word_index_map.keys())+['y']
data.loc['sum_row'] = data.apply(lambda x: x.sum())
# data = data.T[data.T['sum_row']].T
data['sum_col'] = data.apply(lambda x: x.sum(), axis=1)
data.to_csv("C:/Users/User/Desktop/nlu_result/ori_data.csv")
data = data[data['sum_col']>1]
data = data.drop(data.columns[len(data.columns)-1], axis=1)
data = data.T.drop(data.T.columns[len(data.T.columns)-1], axis=1).T
data.to_csv("C:/Users/User/Desktop/nlu_result/ori_data2.csv")

data = shuffle(data)
# 決定切割比例為 70%:30%
split_point = int(len(data) * 0.7)
# 切割成學習樣本以及測試樣本
train = data.iloc[:split_point, :].copy()
test = data.iloc[split_point:, :].copy()
################################################
# 訓練樣本再分成目標序列 y 以及因子矩陣 X
train_X = train.drop('y', axis=1)
train_y = train.y
# 測試樣本再分成目標序列 y 以及因子矩陣 X
test_X = test.drop('y', axis=1)
test_y = test.y
#model = LogisticRegression()
model = RandomForestClassifier()
#model = LogisticRegression()
model.fit(train_X, train_y)
print("Train accuracy:", model.score(train_X, train_y))
print("validation Test accuracy:", model.score(test_X, test_y))
joblib.dump(model, 'C:/Users/User/Desktop/nlu_result/nlu_model.pkl')
"""
for i in data.columns:
    print(i)
"""