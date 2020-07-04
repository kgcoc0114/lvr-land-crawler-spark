# encoding: utf-8
"""
Name: land_data_etl_rdd.py
Desc: land_data etl with spark
Note:
"""
# import findspark
# findspark.init()
import sys
import os
from pyspark.sql.types import *
from pyspark import SparkContext
import tools.times as times
import tools.sp_gadget as sp_gadget
from pyspark.sql import SQLContext
_SPARK_PATH = '/'.join(os.path.abspath(__file__).replace('\\','/').split('/')[1:-1])
_FILES = ["a_lvr_land_a.csv", "b_lvr_land_a.csv", "e_lvr_land_a.csv", "f_lvr_land_a.csv", "h_lvr_land_a.csv"]
_OUTPUT_FILE_NUM = 2

def str2datetime(date_str):
    year = int(date_str[0:3]) + 1911
    month = date_str[3:5]
    day = date_str[5:]
    return "{}-{}-{}".format(year,month,day)

def events_trans(x):
    tmp_list = []
    for i in x:
        tmp_dict = {}
        tmp_dict['district'] = i[2]
        tmp_dict['building_state'] = i[3]
        tmp_list.append(tmp_dict)
    return tmp_list

def time_slots_trans(x):
    tmp_list = []
    for i in x:
        tmp_dict = {}
        tmp_dict['date'] = i[1]
        tmp_dict['events'] = i[2]
        tmp_list.append(tmp_dict)
    return tmp_list

def main():
    # 宣告
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    # 讀檔
    rdd = sc.textFile("file:///{}/input/*.csv".format(_SPARK_PATH))
    # 檔案前兩行不需要
    junk = rdd.take(2)
    # 欄位標題
    header = rdd.first().split(",")

    # 前兩行不需要資訊刪除
    rdd = rdd.filter(lambda row : row not in junk).map(lambda line: line.split(","))
    # 篩選條件
    rdd = rdd.filter(lambda row : sp_gadget.number_trans(row[header.index(u"總樓層數")].split(u"層")[0]) >= 13 and u"住宅大樓" in row[header.index(u"建物型態")] and u"住家用" in row[header.index(u"主要用途")])

    # 取出結果欄位
    rdd_trans = rdd.map(lambda row: (row[header.index(u"縣市")], str2datetime(row[header.index(u"交易年月日")]), row[header.index(u"鄉鎮市區")], row[header.index(u"建物型態")]))
    # rdd ------
    # import json
    # # (city, date)
    # rdd_trans = rdd_trans.groupBy(lambda x: (x[0], x[1])).map(lambda x : (x[0], list(x[1])))
    # # city, date, events
    # rdd_trans = rdd_trans.map(lambda x : (x[0][0],x[0][1], events_trans(x[1]))).sortBy(lambda x: x[1])
    # # sortBy date
    # rdd_trans = rdd_trans.sortBy(lambda x: x[1])
    # # city
    # rdd_trans = rdd_trans.groupBy(lambda x: x[0]).map(lambda x : (x[0], list(x[1])))
    # sdf = rdd_trans.map(lambda x : json.dumps({'city':x[0], 'time_slots':time_slots_trans(x[1])}, ensure_ascii=False))
    # rdd ------

    # rdd 轉換成 dataframe
    sdf = sqlContext.createDataFrame(rdd_trans, ['city', 'date', 'district', 'building_state'])
    # 依json_schema轉換
    sdf = sp_gadget.result_json_schema(sdf)
    # 匯出檔案
    output_path="/{}/output".format(_SPARK_PATH)
    # 生成output資料夾
    sp_gadget.create_result_dir(output_path)
    # 匯出檔案
    sp_gadget.export_results(sdf, "file://{}".format(output_path), _OUTPUT_FILE_NUM)
    # 改檔名
    sp_gadget.trans_result(output_path, _OUTPUT_FILE_NUM)

if __name__ == '__main__':
    main()
    
