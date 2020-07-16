#from predict_random import train_5
#from predict_random import train_15
import numpy as np
import pandas as pd
import joblib
import pymysql
import sklearn
def catch_30(num):
    db = pymysql.connect(host='10.120.35.27',port=3306,user='dbuser6',passwd='aabb1234',db='Project_test')
    cur = db.cursor()

    sql = 'select * from daily_trade_tw where stockiid=%d order by date desc limit 36;'%num
    a = cur.execute(sql)
    Data = [i for i in cur.fetchall()]


    g = pd.DataFrame(np.array(Data))
    if len(g) == 0:
        sql = 'select * from daily_trade_two where stockiid=%d;' % num
        a = cur.execute(sql)
        Data = [i for i in cur.fetchall()]

        g = pd.DataFrame(np.array(Data))

    g = g.drop(0, axis=1)
    g.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    g['Open'] = g['Open'].astype('float')
    g['High'] = g['High'].astype('float')
    g['Low'] = g['Low'].astype('float')
    g['Close'] = g['Close'].astype('float')
    g['Volume'] = g['Volume'].astype('float')
    cur.close()
    db.close()
    return g
def general(stockiid='2330', data=pd.DataFrame(), MA=5, MACD_int=9, RSV_int=9, Mean_Volume_int=15):
    stockiid = int(stockiid)
    MA, MACD_int, RSV_int, Mean_Volume_int = int(MA), int(MACD_int), int(RSV_int), int(Mean_Volume_int)
    # CSV =
    daily_trade = data
    daily_trade['Open'] = daily_trade['Open'].astype('float')
    daily_trade['High'] = daily_trade['High'].astype('float')
    daily_trade['Low'] = daily_trade['Low'].astype('float')
    daily_trade['Close'] = daily_trade['Close'].astype('float')
    daily_trade['Volume'] = daily_trade['Volume'].astype('float')
    daily_trade = daily_trade.to_dict()

    Dic_Open = {}
    Dic_High = {}
    Dic_Low = {}
    Dic_Close = {}
    Dic_Volume = {}

    for count in range(len(daily_trade['Date'])):
        Dic_Open[daily_trade['Date'][count]] = daily_trade['Open'][count]
        Dic_High[daily_trade['Date'][count]] = daily_trade['High'][count]
        Dic_Low[daily_trade['Date'][count]] = daily_trade['Low'][count]
        Dic_Close[daily_trade['Date'][count]] = daily_trade['Close'][count]
        Dic_Volume[daily_trade['Date'][count]] = daily_trade['Volume'][count]

    L = [l for l in Dic_Close.values()]
    D = [d for d in daily_trade['Date'].values()]

    def Moving_Average(interval=5):
        Dic_MA = {}
        interval = int(interval)
        for i in range(len(L)):
            if i - interval < 0:
                pass
            else:
                Dic_MA[D[i]] = "{:}".format(sum(L[i - interval:i]) / interval)
        return Dic_MA

    def Bias(days=3, date='2020-02-19'):
        try:
            x = Dic_Close[date]
            y = float(Moving_Average(interval=days)[date])
            Result = (x - y) / y
        except KeyError as f:
            Result = 0
        return Result

    def EMA(day=12):

        Result_List = []
        Result_Dic = {}
        day = int(day)
        for i in range(len(L)):
            denominator_EMA = 0
            fraction_EMA = 0
            if i - day < 0:
                Result_List.append(np.nan)
            else:
                interval_price = L[i - day:i]
                alpha = ((2 / (day + 1)))
                for c in range(day):
                    denominator_EMA += (1 - alpha) ** c
                    fraction_EMA += ((1 - alpha) ** c) * float(interval_price[c])
                Result_List.append(fraction_EMA / denominator_EMA)
                Result_Dic = dict(zip(D, Result_List))
        return Result_Dic

    def DIF(day1=12, day2=26):
        day1 = int(day1)
        day2 = int(day2)
        Result_List = []
        EMA_1 = [e for e in EMA(day=day1).values()]
        EMA_2 = [e for e in EMA(day=day2).values()]
        for c in range(len(EMA_1)):
            try:
                Result_List.append(EMA_1[c] - EMA_2[c])
            except TypeError as e:
                Result_List.append(np.nan)
            Result_Dic = dict(zip(D, Result_List))
        return Result_Dic

    def MACD(day=9):
        mL = [l for l in DIF(day1=12, day2=26).values()]
        mResult_List = []
        mResult_Dic = {}
        day = int(day)
        for i in range(len(L)):
            denominator_EMA = 0
            fraction_EMA = 0
            if i - day < 0:
                mResult_List.append(np.nan)

            else:
                interval_price = mL[i - day:i]
                alpha = ((2 / (day + 1)))
                for c in range(day):
                    denominator_EMA += (1 - alpha) ** c
                    fraction_EMA += ((1 - alpha) ** c) * float(interval_price[c])
                mResult_List.append(fraction_EMA / denominator_EMA)
                mResult_Dic = dict(zip(D, mResult_List))
        return mResult_Dic

    def RSV(n=9):
        Close_List = [l for l in Dic_Close.values()]
        High_List = [h for h in Dic_High.values()]
        Low_list = [low for low in Dic_Low.values()]
        D = [d for d in Dic_Close.keys()]
        day = int(n)
        Result_List = []
        for i in range(len(Close_List)):
            denominator_RSV = 0
            fraction_RSV = 0
            min_price = 0.0
            Max_price = 0.0
            Result = 0.0
            if i - day < 0:
                Result_List.append(np.nan)
            else:
                interval_close_price = Close_List[i - day:i]
                interval_high_price = High_List[i - day:i]
                interval_low_price = Low_list[i - day:i]
                min_price = min(interval_low_price)
                Max_price = max(interval_high_price)

                denominator_RSV = Max_price - min_price
                fraction_RSV = Close_List[i] - min_price
                try:
                    Result = fraction_RSV / denominator_RSV
                except:
                    Result = 0
                Result_List.append(float(Result))
        Result_dict = dict(zip(D, Result_List))
        return Result_dict

    def K_values(dic=RSV()):
        Days = [d for d in dic.keys()][9:]
        Null_list = [np.nan for i in range(9)]
        K_value = 0.0
        K_list = []
        for day in Days:
            K_value = (K_value) * (2 / 3) + float(dic[day]) * (1 / 3)
            K_list.append(K_value)
        K_list = [np.nan for i in range(9)] + K_list
        Result_Dic = dict(zip([d for d in dic.keys()], K_list))
        return Result_Dic

    def D_values():
        Result_Dic = K_values(dic=K_values())
        return Result_Dic

    def Mean_Volume(interval=15):
        L = [l for l in Dic_Volume.values()]
        D = [d for d in Dic_Volume.keys()]
        interval = int(interval)
        Dic_MV = {}
        for i in range(len(L)):
            if i - interval < 0:
                pass
            else:
                Dic_MV[D[i]] = "{}".format(sum(L[i - interval:i]) / interval)
        return Dic_MV

    tmp1 = pd.Series(MACD(day=MACD_int), name='MACD_{}'.format(MACD_int)).to_frame()
    tmp2 = pd.Series(RSV(n=RSV_int), name='RSV_{}'.format(RSV_int)).to_frame()
    tmp3 = pd.Series(K_values(), name='K_values').to_frame()
    tmp4 = pd.Series(D_values(), name='D_values').to_frame()
    tmp5 = pd.Series(Mean_Volume(interval=Mean_Volume_int), name='Mean_Volume_{}'.format(Mean_Volume_int)).to_frame()

    Result_df = pd.Series(Moving_Average(interval=MA), name='MA_{}'.format(MA)).to_frame()
    Result_df['MACD_{}'.format(MACD_int)] = tmp1['MACD_{}'.format(MACD_int)]
    Result_df['RSV_{}'.format(RSV_int)] = tmp2['RSV_{}'.format(RSV_int)]
    Result_df['K_values'] = tmp3['K_values']
    Result_df['D_values'] = tmp4['D_values']
    Result_df['Mean_Volume_{}'.format(Mean_Volume_int)] = tmp5['Mean_Volume_{}'.format(Mean_Volume_int)]

    return Result_df
def rf_predict_one(id):
    # rf = joblib.load('D:/model_save/best_model/%d_best_model.pkl'%id)
    rf = joblib.load('E:/model_save/best_model/%d_best_model.pkl'%id)
    ordata = catch_30(id)
    ordata.sort_values('Date',inplace=True,ignore_index=True)
    ben_table = general(stockiid=str(id),data=ordata)

    df = ordata.copy()
    df = df.set_index(['Date'])
    df = pd.DataFrame(df, columns=['Open', 'High', 'Low', 'Close', 'Volume'])
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    df = df.astype('float')

    df = pd.merge(df,ben_table,left_index = True,right_index = True,how = 'outer')
    df = df.astype('float32')
    data = df.copy()
    # 貼標y
    # data['week_trend'] = np.where(data.close.shift(-1) > data.close, 1, 0)
    #data['week_trend'] = np.where(data.close.rolling(window=5).mean().shift(-4) > data.close, 1, 0)

    # 最終取出訓練要的特徵
    # data.columns = ['open', 'high', 'low', 'close', 'volume', 'macd', 'macdsignal','macdhist', 'rsi', 'mom', 'slowk', 'slowd', 'rsi2','macd2', 'sma_5', 'sma_10', 'sma_20', 'sma_60','sma_con', 'week_trend']
    if len(rf.feature_importances_) == 11:
        data = pd.DataFrame(data, columns=['open', 'high', 'low', 'close', 'volume','MA_5', 'MACD_9', 'RSV_9', 'K_values', 'D_values', 'Mean_Volume_15'])#, 'sma_con', 'rsi2' ,'mom', 'slowk', 'slowd','macd2','macd2','mom_2'
    else:
        data = pd.DataFrame(data, columns=['open', 'high', 'low', 'close', 'volume'])
    #, 'sma_con', 'rsi2','macd2'

    data = data.set_index(pd.DatetimeIndex(pd.to_datetime(data.index)))


    # 最簡單的作法是把有缺值的資料整列拿掉
    data = data.replace([np.inf, -np.inf], np.nan)
    data.isnull().sum().sum()
    # data = data.dropna(axis=1, how='all')
    data = data.dropna(axis=0)
    predict_up_down = int(rf.predict(data)[0])
    predict_up_down = int(np.where(predict_up_down==0,-1,1))
    w = pd.read_csv('E:/model_save/best_model/result_best.csv')
    wight_stock = float(w[w.id == id]['auc'])
    score = wight_stock*predict_up_down
    # print(score)
    return score






