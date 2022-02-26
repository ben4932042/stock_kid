import numpy as np
import pandas as pd
import talib
from talib import abstract
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import cross_validate
from sklearn import metrics
from sklearn.utils import shuffle
from sklearn.externals import joblib
from sklearn.metrics import confusion_matrix

#####################################################################
df = pd.read_csv('D:/股票資料/各股日資料/2330.csv')
df = df.set_index(['Date'])
df = pd.DataFrame(df, columns=['Open', 'High', 'Low', 'Close', 'Volume'])
df.columns = ['open', 'high', 'low', 'close', 'volume']
df = df.astype('float')
#ta_list = talib.get_functions()
ta_list = ['MACD','RSI','MOM','STOCH']
# ta_list = ['MACD']
for x in ta_list:
    try:
        # x 為技術指標的代碼，透過迴圈填入，再透過 eval 計算出 output
        output = eval('abstract.'+x+'(df)')
        # 如果輸出是一維資料，幫這個指標取名為 x 本身；多維資料則不需命名
        output.name = x.lower() if type(output) == pd.core.series.Series else None
        # 透過 merge 把輸出結果併入 df DataFrame
        df = pd.merge(df, pd.DataFrame(output), left_on = df.index, right_on = output.index)
        df = df.rename(columns={'key_0': 'Index'})
        df = df.set_index('Index')
    except:
        print(x)
#將有意義的放入
df = df.reset_index()
a1 = pd.DataFrame(np.array(1*(df['rsi'] > 50)), columns=['rsi2'])
a2 = pd.DataFrame(1*(df['macd'] > 0)*1*(df['macdsignal'] > 0)*1*(df['macd'].shift(1)<df['macd']), columns=['macd2'])
sma_5 = pd.DataFrame(talib.SMA(np.array(df['close']), 5), columns=['sma_5'])
sma_10 = pd.DataFrame(talib.SMA(np.array(df['close']), 10), columns=['sma_10'])
sma_20 = pd.DataFrame(talib.SMA(np.array(df['close']), 20), columns=['sma_20'])
sma_60 = pd.DataFrame(talib.SMA(np.array(df['close']), 60), columns=['sma_60'])
sma_con = pd.DataFrame(1*(sma_5['sma_5']>sma_10['sma_10'])*1*(sma_10['sma_10']>sma_20['sma_20'])*1*(sma_20['sma_20']>sma_60['sma_60']), columns=['sma_con'])
mom_2 = pd.DataFrame(np.array(1*(df['mom'] > 0)), columns=['mom_2'])

df = pd.concat([df, a1, a2, sma_5, sma_10, sma_20, sma_60,sma_con,mom_2], axis=1)

df = df.set_index('Index')
df = df.astype('float32')
data = df.copy()
# 貼標y
data['week_trend'] = np.where(data.close.shift(-1) > data.close, 1, 0)
# data['week_trend'] = np.where(data.close.rolling(window=30).mean().shift(-29) > data.close, 1, 0)


# 最終取出訓練要的特徵
# data.columns = ['open', 'high', 'low', 'close', 'volume', 'macd', 'macdsignal','macdhist', 'rsi', 'mom', 'slowk', 'slowd', 'rsi2','macd2', 'sma_5', 'sma_10', 'sma_20', 'sma_60','sma_con', 'week_trend']
data = pd.DataFrame(data, columns=['volume', 'macd', 'week_trend', 'slowk', 'slowd' ,'mom'])#, 'sma_con', 'rsi2' ,'mom', 'slowk', 'slowd','macd2','macd2','mom_2'
#, 'sma_con', 'rsi2','macd2'

data = data.set_index(pd.DatetimeIndex(pd.to_datetime(data.index)))


# 最簡單的作法是把有缺值的資料整列拿掉
data = data.replace([np.inf, -np.inf], np.nan)
data.isnull().sum().sum()
data = data.dropna(axis=1, how='all')
data = data.dropna(axis=0)
data.isnull().sum().sum()


#pd.DataFrame(data,columns=['ad','obv'])

data = shuffle(data)
# 決定切割比例為 70%:30%
split_point = int(len(data)*0.7)
# 切割成學習樣本以及測試樣本
train = data.iloc[:split_point,:].copy()
test = data.iloc[split_point:-5,:].copy()
################################################
# 訓練樣本再分成目標序列 y 以及因子矩陣 X
train_X = train.drop('week_trend', axis = 1)
train_y = train.week_trend
# 測試樣本再分成目標序列 y 以及因子矩陣 X
test_X = test.drop('week_trend', axis = 1)
test_y = test.week_trend
"""
param_test1 = {'n_estimators':range(100,1000,100),'min_samples_split':range(100,500,100),'max_depth':range(5,30,5)}
gsearch1 = GridSearchCV(estimator = RandomForestClassifier(min_samples_leaf=20,max_features='auto',random_state=10,criterion='entropy'),
                       param_grid = param_test1, scoring='roc_auc',cv=5)
gsearch1.fit(train_X ,train_y)
# 調參
rf0 = gsearch1.best_estimator_
"""
# 測試 先不調參
rf0 = RandomForestClassifier(bootstrap=True, ccp_alpha=0.0, class_weight=None,
                       criterion='entropy', max_depth=10, max_features='auto',
                       max_leaf_nodes=None, max_samples=None,
                       min_impurity_decrease=0.0, min_impurity_split=None,
                       min_samples_leaf=20, min_samples_split=100,
                       min_weight_fraction_leaf=0.0, n_estimators=100,
                       n_jobs=None, oob_score=False, random_state=10, verbose=0,
                       warm_start=False)
"""
rf0 = RandomForestClassifier(bootstrap=True, ccp_alpha=0.0, class_weight=None,
                       criterion='entropy', max_depth=5, max_features='auto',
                       max_leaf_nodes=None, max_samples=None,
                       min_impurity_decrease=0.0, min_impurity_split=None,
                       min_samples_leaf=20, min_samples_split=100,
                       min_weight_fraction_leaf=0.0, n_estimators=10,
                       n_jobs=None, oob_score=False, random_state=10, verbose=0,
                       warm_start=False)
"""
rf0 = RandomForestClassifier(bootstrap=True, ccp_alpha=0.0, class_weight=None,
                       criterion='entropy', max_depth=15, max_features='auto',
                       max_leaf_nodes=None, max_samples=None,
                       min_impurity_decrease=0.0, min_impurity_split=None,
                       min_samples_leaf=20, min_samples_split=100,
                       min_weight_fraction_leaf=0.0, n_estimators=500,
                       n_jobs=None, oob_score=False, random_state=50, verbose=0,
                       warm_start=False)

rf0.fit(train_X ,train_y)
#print(rf0.oob_score_)
y_predprob = rf0.predict_proba(test_X)[:,1]
prediciton = rf0.predict(test_X)
print("AUC Score (Test): %f" % metrics.roc_auc_score(test_y, y_predprob))
print(confusion_matrix(test_y, prediciton))
print(rf0.score(test_X, test_y))
# 儲存cv資料
joblib.dump(gsearch1, 'C:/Users/User/Desktop/rf_cv.pkl')
# 直接存訓練模型
joblib.dump(rf0, 'C:/Users/User/Desktop/rf_model.pkl')


clf2 = joblib.load('C:/Users/User/Desktop/GS_obj.pkl')



