import jieba
from collections import Counter
from wordcloud import WordCloud
from matplotlib import pyplot as plt
from PIL import Image
import numpy as np
import pymongo
import re
import test_NLU2

"""
jieba.set_dictionary('C:/Users/User/Desktop/dict.txt')
with open('C:/Users/User/Desktop/stop.txt', 'r', encoding='utf8') as f:  # 中文的停用字，我也忘記從哪裡拿到的，效果還可以，繁體字的資源真的比較少，大家將就一下吧
    stops = f.read().split('\n')

testStr = """
#理財專欄作者：黃逸強最近的熱門新聞就是美國非裔男子遭白人警察壓制致死，引發全國性示威，暴動場面怵目驚心，很難想像這是標榜民主自由的美國。但這些負面消息都不影響股市的發展，美股依然上揚，還一再創近期新高，這一波的反彈令專家很不解，更別說是一般的散戶投資人。散戶是反向指標？雖然疫情趨緩很多城市開始解封，但要經濟復甦還言之過早，更別說美中貿易談判未解，美國又進一步對華為制裁，再加上街頭暴動猶如雪上加霜；經濟數據更是難看，非農就業人數大減二千多萬人，創二戰以來最慘，美股仍不甩利空，硬是漲逾數百點，真要找一個理由就是「市場把期待押在未來的復甦上。」儘管股市一直漲，散戶投資人反倒是越漲越害怕，美國個人投資者協會發布最新報告顯示，散戶投資人看空情緒升高至52.6%，創2013年來最高；反之，看多情緒23.6%，則是30年來最低水準。如果散戶看法是反指標，是否意味著市場未來向上的機率更高。台灣投資人也是一樣，舉一檔股票為例，看漲的「台灣五十ETF」只有1380張的融資，1萬多張的周均量，而看跌的「台灣五十反向ETF」，卻有近40萬張的融資，12萬張的周均量。股市愈漲做空的人愈多，是一個很奇怪的心態。漲跌不需要理由也有專家認為，最近股市大漲並不是看多的買盤所拉抬，而是空頭回補的力道所推升。依據籌碼分析，通常跌到深處融資斷頭、或漲到創高空頭回補，就是一種反轉訊號，所以有專家提醒投資人，目前這個泡沫應該要留心。其實股市漲跌並不一定要理由，那只是記者在寫稿時需要一些題材，所以上漲就去找利多的理由、下跌就去找利空消息來搪塞，都是事後諸葛無濟於事。過去很多次的上漲是無基之彈，因為實在找不到理由。像2008年的雷曼金融風暴後，股市也是沒人看好，在現金為王的氛圍中走了十年的多頭。所以股市難測，幾百萬人在進行的金錢遊戲，不是幾個數據或幾則新聞就能決定漲跌，專家的分析都是以現在的資訊，去推測未來的發展，猜對了只是運氣好，猜錯是正常。天災人禍不是意外而是無常，在做投資或資產配置時都是必須要納入的風險因子。別聽消息做股票市場很任性，當它要漲時再多的利空壞消息它還是漲，當它要跌再多的好消息也挽不住跌勢。所以回歸技術面，趨勢的力量不可擋，只要順勢操作，不要自作聰明去抓頭部或猜底部。每一個階段採用不同的工具，承平時期用基本面分析找到長線績優股，新冠肺炎把股市打到低點，基本面無用改用技術分析搶反彈；千萬不要聽消息面，新聞有太多雜訊反而會干擾投資人。有人因為漲太多、股價很高所以會怕，其實高是一種感覺，很抽象不能用來操盤。現階段不需要預測高點，拋開理智線勇敢下單，搭上趨勢的順風車，並設好停損點，就不怕懼高症。★延伸閱讀★投資不能一窩蜂！黃金變現二管道炒短恪守三原則！超前佈署以應萬變！沒有意外 只有號外﹗
"""
#stops.append('\n')  ## 我發現我的文章中有許多分行符號，這邊加入停用字中，可以把它拿掉
#stops.append('\n\n')
# terms = [t for t in jieba.cut(testStr, cut_all=True) if t not in stops]
terms = [t for t in jieba.cut(testStr) if t not in stops]


sorted(Counter(terms).items(), key=lambda x:x[1], reverse=True)  ## 這個寫法很常出現在Ｃounter中，他可以排序，list每個item出現的次數。




aa = sorted(Counter(terms).items(), key=lambda x:x[1], reverse=True)
"""
jieba.set_dictionary('C:/Users/User/Desktop/dict.txt')
with open('C:/Users/User/Desktop/stop.txt', 'r', encoding='utf8') as f:  # 中文的停用字，我也忘記從哪裡拿到的，效果還可以，繁體字的資源真的比較少，大家將就一下吧
    stops = f.read().split('\n')

client = pymongo.MongoClient('mongodb://0.tcp.ngrok.io:14077/')
db = client.stock
collection = db.pos_news
result = collection.find()
news = [d for d in result]
postitle = [new['news_content'] for new in news]

client = pymongo.MongoClient('mongodb://0.tcp.ngrok.io:14077/')
db = client.stock
collection = db.neg_news
result = collection.find()
news = [d for d in result]
negtitle = [new['news_content'] for new in news]
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

def my_tokenizer(s):
    tokens = [t for t in jieba.cut(s) if t not in stops]
    tokens = [t for t in tokens if len(t) == 2]
    return tokens


# 在做斷字斷詞index 共有幾個不同字
# start to process postive_reviews text
word_cloud=[]
for review in positive_reviews:
    word_cloud = word_cloud + test_NLU2.my_tokenizer(review)

#start to process negative_reviews text
for review in negative_reviews:
    word_cloud = word_cloud + test_NLU2.my_tokenizer(review)

for i in range(len(word_cloud)):
    word_cloud[i] = str(word_cloud[i])

aa = sorted(Counter(word_cloud).items(), key=lambda x:x[1], reverse=True)





wordcloud = WordCloud(font_path="kaiu.ttf")
wordcloud.generate_from_frequencies(frequencies=Counter(word_cloud))
a1 = wordcloud.to_image().save('C:/Users/User/Desktop/nlu_result/model_full1.jpg')

from scipy.ndimage import gaussian_gradient_magnitude
alice_mask = np.array(Image.open("C:/Users/User/Desktop/test.jpg"))
mask_color = alice_mask[::3, ::3]
mask_image = mask_color.copy()
mask_image[mask_image.sum(axis=2) == 0] = 255
# Edge detection
edges = np.mean([gaussian_gradient_magnitude(mask_color[:, :, i]/255., 2) for i in range(3)], axis=0)
mask_image[edges > .08] = 255
wc = WordCloud(background_color="white", max_words=2000, mask=mask_image, font_path="kaiu.ttf")
wc.generate_from_frequencies(Counter(word_cloud))  ## 請更改Counter(terms)
a2 = wc.to_image().save('C:/Users/User/Desktop/nlu_result/model_full2.jpg')