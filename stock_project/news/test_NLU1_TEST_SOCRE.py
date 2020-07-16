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
import test_NLU2
import jieba
from collections import Counter
from wordcloud import WordCloud
from matplotlib import pyplot as plt
from PIL import Image
import numpy as np
import pymongo
import re
client = pymongo.MongoClient('mongodb://0.tcp.ngrok.io:14077/')
db = client.stock
collection = db.pos_news
rf = joblib.load('C:/Users/User/Desktop/nlu_result/nlu_model.pkl')
data = pd.read_csv("C:/Users/User/Desktop/nlu_result/ori_data2.csv")
# 迴圈取每個股票的新聞
# for
#這邊是先抓有的測試，要換成抓美枝的..........................
result = collection.find()
news = [d for d in result]
postitle = [new['news_content'] for new in news][8000:]
positive_reviews = []
#這邊是先抓有的測試，要換成抓美枝的..........................
word_cloud = []
for i in range(len(postitle)):
    positive_reviews = positive_reviews+[test_NLU2.remove(postitle[i])]
    word_cloud = word_cloud + test_NLU2.my_tokenizer(postitle)
predict_tmp = []
for j in positive_reviews:
    predict_tmp = predict_tmp + [test_NLU2.new_input_one(j, rf, data)]
score_mean = np.mean(predict_tmp)