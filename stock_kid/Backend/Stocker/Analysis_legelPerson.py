# Not regularly Demo_code:2330

import json
import numpy
import pandas as pd

def analysis_close():
    df = pd.read_csv (r'./all_stock_price/public/2330.TW.csv')
    all_data_dict = df.to_dict()
    # print(all_data_dict)
    time_to_close_dict = {}
    index_to_time_dict = {}
    times = len(all_data_dict['Date'])

    for time in range(times):
        time_to_close_dict[all_data_dict['Date'][time]] = all_data_dict['Close'][time]
    index_to_time_dict = all_data_dict['Date'].copy()
    # print(index_to_time_dict)
    # print(time_to_close_dict)

    T = [t for t in index_to_time_dict.values()]
    L = [x for x in time_to_close_dict.values()]

    X = []
    for i in range(len(L)):
        try:
            if L[i] <= L[i-1]:
                X.append(-1)
            elif L[i] == L[i-1]:
                X.append(0)
            elif L[i] > L[i-1]:
                X.append(1)
        except IndexError as f:
            X.append(2)
    # print(X)
    # print(len(X))

    index = dict(zip(T,X))
    return index

print('==============================')
# with open('./analysis.json','r') as f:
#     dic_str = f.readline()
#
# dic_str = dic_str.replace('NaN','"xxx"')
#
# dic = eval(dic_str)
# # print(dic)
# times = dic.keys()
# # print('data:',len(times))
# L=[]
# for time in times:
#     for i in range(14):
#         L.append(dic[time]['buy'][str(i)])
# c=0
# for l in L:
#     if l == "xxx":
#         c+=1
# tmp_set = set(l for l in L)
# tmp_set.remove('xxx')
# # print('LegelPerson:',len(tmp_set))
# # print('NaN: ',c)
#
# analysis_buyset = set()
# for i in range(15):
#     update_set = { item+'{}'.format(str(i)) for item in tmp_set }
#     analysis_buyset.update(update_set)
#     del update_set
# # test
# # print(analysis_set)
# # print('analysis_set:',len(analysis_buyset))
# # print(analysis_buyset)
#
#
# analysis_buyList = list(analysis_buyset)
#
# for time in times:
#     t = []
#     # print(dic[time]['buy'])
#
#     for key in dic[time]['buy'].keys():
#         t.append('{}{}'.format(dic[time]['buy'][key],key))
#     dic[time]['buy']=t
#
#
# # print(dic)
#
# # print(analysis_buyList)
#
#
# target_buy_index_Dic = {}
# for time in times:
#     Data_list = []
#     for item in analysis_buyList:
#         if item in dic[time]['buy']:
#             Data_list.append(1)
#         else:
#             Data_list.append(0)
#     target_buy_index_Dic[time]=Data_list
#     # print('===============')
# # print(target_buy_index_Dic)
#
#
# #   save data
# # --------------------------------------------------------- #
# with open('./analysis1.json', 'a' ) as outfile:
#     json.dump( target_buy_index_Dic, outfile, ensure_ascii=False )


# type I
# --------------------------------------------------------- #
# with open('./analysis1.json', 'r' ) as outfile:
#     target_buy_index_Dic = json.loads(outfile.read())
# # type dict
# # target_buy_index_Dic['2016-01-04'] =[int(i) for i in target_buy_index_Dic['2016-01-04']]
# with open('./analysis.json','r') as f:
#     dic_str = f.readline()
# dic_str = dic_str.replace('NaN','"xxx"')
# dic = eval(dic_str)
# times = dic.keys()
# typeI=0
# test_L = []
# for i in range(7380):
#     test_L.append(1)
# test_L = numpy.array(test_L)
# for Denominator in times:
#     result = []
#     target_test_L_Denominator = numpy.array(target_buy_index_Dic[Denominator])
#     all_count_Denominator = numpy.inner(target_test_L_Denominator, test_L)
#
#     for Numerator in times:
#         target_test_L_Numerator = numpy.array(target_buy_index_Dic[Numerator])
#         count_Numerator = numpy.inner(target_test_L_Denominator, target_test_L_Numerator)
#         result.append(count_Numerator/all_count_Denominator)
#     # print(result)
# # """
# # L is the number of the target_stock_legel_person
# # numpy.inner(target_test_L, test_L is the Denominator of the Similarity Measure.
# # """
#     A = []
#     count_result_index = 0
#     for num in result:
#         num = float(num)
#         if num in (0.5,0.9999999999):
#             print('yes')
#             A.append(count_result_index)
#             count_result_index+=1
#         else:
#             count_result_index+=1
#     if len(A)>1:
#       typeI+=1
# print(typeI)

# type II
# ==================================================================== #


with open('./analysis.json','r') as f:
    dic_str = f.readline()

dic_str = dic_str.replace('NaN','"xxx"')

dic = eval(dic_str)
# print(dic)
times = dic.keys()
# print('data:',len(times))
L=[]
for time in times:
    for i in range(14):
        L.append(dic[time]['buy'][str(i)])

c=0
for l in L:
    if l == "xxx":
        c+=1
total_set = set(l for l in L)
total_set.remove('xxx')
total_list = list(total_set)
# print(total_list)

analysis_Dic = {}
for time in times:
    analysis_Dic[time] = list(dic[time]['buy'].values())
# print(analysis_Dic)

target_buy_index_Dic = {}
for time in times:
    Data_list = []
    for item in total_list:
        if item in analysis_Dic[time]:
            Data_list.append(1)
        else:
            Data_list.append(0)
    target_buy_index_Dic[time]=Data_list
# print(target_buy_index_Dic)

# #   save data
# # --------------------------------------------------------- #
# with open('./analysis2.json', 'a' ) as outfile:
#     json.dump( target_buy_index_Dic, outfile, ensure_ascii=False )
#
#
#
# # --------------------------------------------------------- #
# with open('./analysis2.json', 'r',encoding='utf-8' ) as outfile:
#     target_buy_index_Dic = json.loads(outfile.read())
# print(target_buy_index_Dic)
#
# # type dict

with open('./analysis.json','r') as f:
    dic_str = f.readline()
dic_str = dic_str.replace('NaN','"xxx"')
dic = eval(dic_str)
times = dic.keys()
typeII=0
test_L = []
A_Dic = {}
target_time = [t for t in times]
for i in range(492):
    test_L.append(1)
test_L = numpy.array(test_L)
for Denominator in times:
    result = []
    target_test_L_Denominator = numpy.array(target_buy_index_Dic[Denominator])
    all_count_Denominator = numpy.inner(target_test_L_Denominator, test_L)
    for Numerator in times:
        target_test_L_Numerator = numpy.array(target_buy_index_Dic[Numerator])
        count_Numerator = numpy.inner(target_test_L_Denominator, target_test_L_Numerator)
        result.append(count_Numerator/all_count_Denominator)
    # print(result)
# """
# L is the number of the target_stock_legel_person
# numpy.inner(target_test_L, test_L) is the Denominator of the Similarity Measure.
# """

    A_List = []
    B_dict = {}
    count_result_index = 0
    for num in result:
        num = float(num)
        if num >0.6 and num < 1:
            A_List.append(count_result_index)
            B_dict[count_result_index] = num
            count_result_index+=1
        else:
            count_result_index+=1

    if len(A_List)>=1:


        typeII+=1
        try:
            A_Dic[Denominator] = [target_time[max(A_List)+1],B_dict[max(A_List)]]
        except:
            pass
        # form_type = [index,time,similarity]


# print(A_Dic)
# print(typeII)
# typeII-1
# ----------------------------------------------------- #
del A_Dic['2020-04-27']
print(A_Dic)
# print(typeII)
# ----------------------------------------------------- #
A = analysis_close()
r_success = 0
r_fail = 0

for key in A_Dic.keys():
    try:
        if analysis_close()[key] == analysis_close()[A_Dic[key][0]]:
            r_success +=1
        else:
            r_fail+=1
    except KeyError as f:
        # print(key)
        pass

print('======================================')
print('typeII-1')
print('--------------------------------------------------------')
print('success:',r_success)
print('failure:',r_fail)
print('typeII-1 success rate: ',r_success/(r_success+r_fail))
print('--------------------------------------------------------')


#typeII-2
A = analysis_close()
r_success_list =[]
r_fail_list = []

for key in A_Dic.keys():
    try:
        if analysis_close()[key] == analysis_close()[A_Dic[key][0]]:
            r_success_list.append(A_Dic[key][1])
        else:
            r_fail_list.append(A_Dic[key][1])
    except KeyError as f:
        # print(key)
        pass

print('typeII-2')
print('--------------------------------------------------------')
print('success:',sum(r_success_list))
print('failure:',sum(r_fail_list))
print('typeII-2 success rate: ',sum(r_success_list)/(sum(r_success_list)+sum(r_fail_list)))
print('--------------------------------------------------------')

