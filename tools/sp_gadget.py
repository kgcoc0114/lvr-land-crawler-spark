# encoding: utf-8
"""
Name: tools/sp_gadget.py
Desc: spark functions
Note:
"""
import sys
import os
import tools.files as files
import tools.times as times

from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
import pyspark.sql.functions as F

from functools import reduce


# 中文數字轉換
def number_trans(ch_num):
    if ch_num:
        ch_num = ch_num.replace(u'層','')
        digit = {u'一': 1, u'二': 2, u'三': 3, u'四': 4, u'五': 5, u'六': 6, u'七': 7, u'八': 8, u'九': 9}
        num = 0
        if ch_num:
            idx_b, idx_s = ch_num.find(u'百'), ch_num.find(u'十')
        
        if idx_b != -1:
            num  = num + digit[ch_num[idx_b - 1:idx_b]] * 100

        if idx_s != -1:
            num = num + digit.get(ch_num[idx_s - 1:idx_s], 1) * 10
    
        if ch_num[-1] in digit:
            num = num+digit[ch_num[-1]]

        return num
    else:
        return 0

# ------ spark rdd ------
def rdd_events_trans(x):
    tmp_list = []
    for i in x:
        tmp_dict = {}
        tmp_dict['district'] = i[2]
        tmp_dict['building_state'] = i[3]
        tmp_list.append(tmp_dict)
    return tmp_list

def rdd_time_slots_trans(x):
    tmp_list = []
    for i in x:
        tmp_dict = {}
        tmp_dict['date'] = i[1]
        tmp_dict['events'] = i[2]
        tmp_list.append(tmp_dict)
    return tmp_list
# ------ spark rdd ------

# ------ spark dataframe ------
# 讀檔
def sdf_read_file_spark(sqlc, file_path, schema):
    sdf = sqlc.read.format('com.databricks.spark.csv').options(header='False', inferschema='False').schema(schema).load(file_path)
    return sdf

# 格式轉換-中文樓層轉換, 日期轉換
def sdf_trans(sdf):
    floor_trims = F.udf(number_trans, IntegerType())
    str2date = F.udf(times.dt2AD_dt, DateType())
    sdf = sdf.withColumn("total_floor_trans", floor_trims("total_floor"))
    sdf = sdf.withColumn("date", str2date("trans_date"))
    sdf_final = sdf.filter("district is not NULL")
    return sdf_final

# 合併dataframe
def sdf_union(*sdf):
    return reduce(DataFrame.unionAll, *sdf)

# 條件篩選
def sdf_filter(sdf, conditions):
    return sdf.filter(conditions)

# 結果json schema
def result_json_schema(sdf, distinct_judge=False):
    if distinct_judge:
        sdf = sdf.select(
            "city",
            "date",
            "district",
            "building_state"
        ).distinct()
    
    sdf = sdf.select(
        "city",
        "date",
        F.struct(
            F.col("district"),
            F.col("building_state"),
        ).alias("events")
    ).groupBy(
        "city","date"
    ).agg(
        F.collect_list("events").alias("events")
    ).orderBy("date").select(
        "city",
        F.struct(
            F.col("date"),
            F.col("events"),
        ).alias("time_slots")
    ).groupBy(
        "city"
    ).agg(
        F.collect_list("time_slots").alias("time_slots")
    )
    return sdf
# ------ spark dataframe ------

# ------ export result ------
# 生成output資料夾
def create_result_dir(output_path):
    if files.exist_or_not(output_path):
        files.remove_dir(output_path)
        files.create_dir(output_path)
    else:
        files.create_dir(output_path)

# 匯出檔案
def export_results(sdf, output_path, output_file_num):
    rdd_type = False
    if isinstance(sdf, RDD):
        rdd_type = True
    sdf_list = sdf.randomSplit([0.5, 0.5], 1)
    for i in range(output_file_num):
        if rdd_type:
            sdf_list[i].coalesce(1).saveAsTextFile("{}/result{}".format(output_path, i+1))
        else:
            sdf_list[i].coalesce(1).write.json("{}/result{}".format(output_path, i+1))

# 重新命名
def trans_result(output_path, output_file_num):
    for i in range(output_file_num):
        file_list = files.get_all_files("{}/result{}".format(output_path, i+1))
        json_name = list(filter(lambda x: x.startswith("part-"), file_list))[0]
        files.rename_file("{}/result{}/{}".format(output_path, i+1,json_name),
                        "{}/result-part{}.json".format(output_path, i+1))
        files.remove_dir("{}/result{}".format(output_path, i+1))
# ------ export result ------

def get_data(spark, ld):
    import json
    result = None
    if ld.mode == 'DF':
        sdf_list =[]
        for file in ld.file_list:
            input_data = "file:///{}/input/{}".format(ld.proj_root, file)
            sdf = sdf_read_file_spark(spark, input_data, ld.data_schema)
            # add file city
            sdf = sdf.withColumn('city', F.lit(ld.get_file_city(file)))
            sdf_list.append(sdf)
        
        # 合併dataframe
        sdf = sdf_union(sdf_list)
        return sdf
    
    else:
        for file in ld.file_list:
            tmp_rdd = spark.textFile("file:///{}/input/{}".format(ld.proj_root, file))
            # add file city
            file_city = ld.get_file_city(file)
            tmp_rdd = tmp_rdd.map(lambda row: file_city + "," + row)
            # exclude header
            junk = tmp_rdd.take(2)
            tmp_rdd = tmp_rdd.filter(lambda row: row not in junk).map(lambda line: line.split(","))
            # header for getting index
            if not ld.header:
                ld.get_header(junk)
            if not result:
                result = tmp_rdd
            else:
                # union files
                result = result.union(tmp_rdd)

        return result
                
def analyze(result, mode, header=None):
    if mode == "DF":       
        # 格式處理
        result = sdf_trans(result)
        # 條件篩選
        result = sdf_filter(result, u"main_use ='住家用' AND building_state like '住宅大樓%' and total_floor_trans >= 13")
        # 匯出json
        result = result_json_schema(result)
    else:
        result = result.filter(lambda row : number_trans(
                            row[header.index(u"總樓層數")].split(u"層")[0]) >= 13\
                            and u"住宅大樓" in row[header.index(u"建物型態")] \
                            and u"住家用" in row[header.index(u"主要用途")])
    
        result = result.map(lambda row: (row[header.index(u"縣市")], 
                            times.dt2AD(row[header.index(u"交易年月日")]), 
                            row[header.index(u"鄉鎮市區")], 
                            row[header.index(u"建物型態")]))
        import json
        # (city, date)
        result = result.groupBy(lambda x: (x[0], x[1])).map(lambda x : (x[0], list(x[1])))
        # city, date, events
        result = result.map(lambda x : (x[0][0],x[0][1], rdd_events_trans(x[1]))).sortBy(lambda x: x[1])
        # sortBy date
        result = result.sortBy(lambda x: x[1])
        # city
        result = result.groupBy(lambda x: x[0]).map(lambda x : (x[0], list(x[1])))
        result = result.map(lambda x : json.dumps({'city':x[0], 'time_slots':rdd_time_slots_trans(x[1])}, ensure_ascii=False))
    
    return result


def export_result(result, spark_path, output_path, output_file_num):
    # 匯出檔案
    output_path="/{}/output".format(spark_path)
    # 生成output資料夾
    create_result_dir(output_path)
    # 匯出檔案
    export_results(result, "file://{}".format(output_path), output_file_num)
    # 改檔名
    trans_result(output_path, output_file_num)


class LvrData(object):
    proj_root = None
    file_list = None
    mode = None
    header = None
    city_mapping = None
    data_schema = None

    def init(self, spark_path):
        self.proj_root = spark_path
        self.file_list = files.get_all_files("/{}/input/".format(self.proj_root))
        self.file_list.remove("manifest.csv")

    def get_city_map(self, sc):
        city_mapping = sc.textFile("file:///{}/input/manifest.csv".format(self.proj_root))
        self.city_mapping = city_mapping.map(lambda line: line.split(","))
    
    def get_header(self, junk):
        self.header = junk[0].split(",")[1:]
        self.header.insert(0, u"縣市")
    
    def get_file_city(self, file_name):
        city_result = self.city_mapping.filter(lambda row: row[0] == file_name).collect()
        return city_result[0][2][0:3]
