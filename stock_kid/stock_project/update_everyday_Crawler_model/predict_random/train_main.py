from predict_random import train_5
from predict_random import train_15
from predict_random import train_15_tuning
import numpy as np
import pandas as pd
import joblib
from predict_random import mysql_catch
tmp_list = Stockiid.values()
stock_iids = []
for i in tmp_list:
    i = i.replace(' ', '')
    stock_iids.append(i)
a1 = pd.DataFrame([],columns=['id', 'auc', 'acu'])
a2 = pd.DataFrame([],columns=['id', 'auc', 'acu'])
a3 = pd.DataFrame([],columns=['id', 'auc', 'acu'])
a4 = pd.DataFrame([],columns=['id', 'model', 'auc'])
for i in stock_iids:
    i = int(i)
    df = mysql_catch.catch(i)
    rf, auc, acu = train_5.train_5_rf(df)
    result = pd.DataFrame({"id":[i], "auc":[auc], "acu":[acu]})
    if len(a1) == 0:
        a1 = result
    else:
        a1 = pd.concat([a1, result], ignore_index=True)
    a1.to_csv('D:/model_save/model_nocv_5v/result_nocv_5v.csv')
    print(i,':model1 is OK')
    joblib.dump(rf, 'D:/model_save/model_nocv_5v/%d_nocv_5v.pkl'%i)

    rf1, auc1, acu1 = train_15.train_15_rf(df)
    result = pd.DataFrame({"id":[i], "auc1":[auc1], "acu1":[acu1]})
    if len(a2) == 0:
        a2 = result
    else:
        a2 = pd.concat([a2, result], ignore_index=True)
    a2.to_csv('D:/model_save/model_nocv_15v/result_nocv_15v.csv')
    print(i,':model2 is OK')
    joblib.dump(rf, 'D:/model_save/model_nocv_15v/%d_nocv_15v.pkl'%i)

    if auc<0.7 and auc1<0.7:
        rf2, gsearch, auc2, acu2 = train_15_tuning.train_15_tuning_rf(df)
        result = pd.DataFrame({"id": [i], "auc2": [auc2], "acu2": [acu2]})
        if len(a3) == 0:
            a3 = result
        else:
            a3 = pd.concat([a3, result], ignore_index=True)
        a3.to_csv('D:/model_save/model_tuning/result_tuning.csv')
        print(i, ':tuning model3 is OK')
        joblib.dump(gsearch, 'D:/model_save/model_tuning/%d_cv_gsearch.pkl' % i)
        joblib.dump(rf2, 'D:/model_save/model_tuning/%d_cv_model.pkl' % i)
    else:
        auc2=0

    score = np.array([auc,auc1,auc2])
    best = int(np.where(score==np.max(score))[0][0])
    if best == 2:
        joblib.dump(rf1, 'D:/model_save/best_model/%d_best_model.pkl' % i)
    elif best == 3:
        joblib.dump(rf2, 'D:/model_save/best_model/%d_best_model.pkl' % i)
    else:
        joblib.dump(rf, 'D:/model_save/best_model/%d_best_model.pkl' % i)
    result = pd.DataFrame({"id": [i], "model": [best+1], "auc": [score[best]]})
    if len(a4) == 0:
        a4 = result
    else:
        a4 = pd.concat([a4, result], ignore_index=True)
    a4.to_csv('D:/model_save/best_model/result_best.csv')
    print(i, ':best model%d is OK'%(best+1))



