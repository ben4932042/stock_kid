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
# 結巴設定
jieba.set_dictionary('C:/Users/Big data/Desktop/dict.txt')
with open('C:/Users/Big data/Desktop/stop.txt', 'r', encoding='utf8') as f:  # 中文的停用字，我也忘記從哪裡拿到的，效果還可以，繁體字的資源真的比較少，大家將就一下吧
    stops = f.read().split('\n')

# text = '財訊快報／記者李純君報導】晶圓代工龍頭台積電(2330)今早進行去年第四季股息2.5元的除息，開盤後2分鐘就順利填息，速度甚快；爾後隨大盤上下震盪，股價則在313到315元小幅波動，顯見維持小紅盤；就第三季展望部分，在全力替華為趕工，加上聯發科(2454)、NVIDIA、AMD等客戶追單的前提下，第三季營運將有個位數百分比的季增率。台積電改為季度發放股息，並宣布年度四季合計配息會超過10元，而去年度第四季的現金股利在今天除息，配發2.5元現金股息，而台積電的除息，將影響大盤21.5點。台積電本次配發金額將達到648.25億元，預計7月16日發放。昨日大盤資金明顯集中在防疫概念股，半導體類股疲弱，但外資卻在昨日對台積電買超5115張，顯見外資對台積電的填息看法偏多，也對台積電下半年營運前景並不像先前悲觀。受惠於七奈米訂單強勁，台積電第二季營收目標101億至104億美元，可順利達陣，至於第三季的部分，一來美國加強對華為管制後，台積電先前對華為的接單，以及手上的半成品必須在緩衝期內生產完畢，加上下半年疫情舒緩，安卓陣營與蘋果陣營手機零件都將生產與拉貨，同時AI、高速運算與5G相關的新款晶片訂單強勁，為此，台積電第三季營收將有季增一成內的表現。'
def remove(text):
    text = re.sub('[A-Za-z]+', '', text)
    text = re.sub('\d+', '', text)
    text = re.sub('◆[’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~：﹗；\n]', '', text)
    return text
def my_tokenizer(s):
    tokens = [t for t in jieba.cut(s) if t not in stops]
    tokens = [t for t in tokens if len(t) == 2]
    return tokens
def tokens_to_vector(tokens,word_index_map):
    x = np.zeros(len(word_index_map)) # last element is for the label (1:positive, 0:negative)
    # 邊在數字詞的數量
    for t in tokens:
        try:
            i = int(word_index_map[t])
            x[i] += 1
        except:
            continue
    return x
# text = '個股'
def new_input_one(text, rf, data):
    #rf = joblib.load('C:/Users/User/Desktop/nlu_result/nlu_model.pkl')
    #data = pd.read_csv("C:/Users/User/Desktop/nlu_result/ori_data2.csv")
    # 載入特徵
    word_index_map = list(data.columns)[:-1]
    # 將一篇文章斷字斷詞
    """
    def remove(text):
        text = re.sub('[A-Za-z]+', '', text)
        text = re.sub('\d+', '', text)
        text = re.sub('[’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~：﹗；]', '', text)
        return text
    def my_tokenizer(s):
        tokens = [t for t in jieba.cut(s) if t not in stops]
        tokens = [t for t in tokens if len(t) == 2]
        return tokens
    """
    one_data = remove(text)
    tokens = my_tokenizer(one_data)

    """
    def tokens_to_vector(tokens,word_index_map):
        x = np.zeros(len(word_index_map)) # last element is for the label (1:positive, 0:negative)
        # 這邊在數字詞的數量
        for t in tokens:
            try:
                i = int(word_index_map[t])
                x[i] += 1
            except:
                continue
        return x
    """
    data = np.zeros((1, len(word_index_map)))
    word_index_map = dict(zip(word_index_map,np.arange(0,len(word_index_map))))
    xy = tokens_to_vector(tokens,word_index_map)
    data[0, :] = xy
    answer = int(rf.predict(data[0].reshape(1,-1))[0])
    return answer


