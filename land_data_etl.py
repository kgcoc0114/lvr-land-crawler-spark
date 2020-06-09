# encoding: utf-8
"""
Name: land_data_etl.py
Desc: land_data etl with spark
Note:
"""

# import findspark
# findspark.init()

from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

import sys
import os
import shutil
from functools import reduce
from pathlib import Path
from urllib import parse

import tools.times as times
import tools.files as files
import tools.sp_gadget as sp_gadget

_SPARK_PATH = '/'.join(os.path.abspath(__file__).replace('\\','/').split('/')[1:-1])
_FILES = ["a_lvr_land_a.csv", "b_lvr_land_a.csv", "e_lvr_land_a.csv", "f_lvr_land_a.csv", "h_lvr_land_a.csv"]

_OUTPUT_FILE_NUM = 2

def str2datetime(date_str):
    year = int(date_str[0:3]) + 1911
    month = date_str[3:5]
    day = date_str[5:]
    return times.str2datetime("{}-{}-{}".format(year,month,day))

def read_file_spark(sqlc, file_path, schema):
    sdf = sqlc.read.format('com.databricks.spark.csv').options(header='False', inferschema='False').schema(schema).load(file_path)
    return sdf

def sdf_trans(sdf):
    floor_trims = F.udf(sp_gadget.number_trans, IntegerType())
    str2date = F.udf(str2datetime, DateType())
    sdf = sdf.withColumn("total_floor_trans", floor_trims("total_floor"))
    sdf = sdf.withColumn("date", str2date("trans_date"))
    sdf_final = sdf.filter("district is not NULL")
    return sdf_final

def sdf_union(*sdf):
    return reduce(DataFrame.unionAll, *sdf)

def sdf_filter(sdf, conditions):
    return sdf.filter(conditions)

def main():
    data_schema = StructType([
            StructField('district', StringType(), True), 
            StructField('trans_type', StringType(), True),
            StructField('address', StringType(), True),
            StructField('area', DoubleType(), True),
            StructField('non_metropolis_district', StringType(), True),
            StructField('non_metropolis', StringType(), True),
            StructField('land_use', StringType(), True),
            StructField('trans_date', StringType(), True),
            StructField('trans_pen_num', StringType(), True),
            StructField('shifting_lvl', StringType(), True),
            StructField('total_floor', StringType(), True),
            StructField('building_state', StringType(), True),
            StructField('main_use', StringType(), True),
            StructField('main_materials', StringType(), True),
            StructField('construction_date', StringType(), True),
            StructField('shifting_total_area', DoubleType(), True),
            StructField('building_room', IntegerType(), True),
            StructField('building_hall', IntegerType(), True),
            StructField('building_health', IntegerType(), True),
            StructField('building_compartmented', StringType(), True),
            StructField('manage_org', StringType(), True),
            StructField('total_price_NTD', IntegerType(), True),
            StructField('unit_price_NTD', IntegerType(), True),
            StructField('berth_category', StringType(), True),
            StructField('berth_shifting_total_area', DoubleType(), True),
            StructField('berth_total_price_NTD', IntegerType(), True),
            StructField('note', StringType(), True),
            StructField('serial_num', StringType(), True),
            StructField('city', StringType(), True)])
    
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    sdf_list = []

    for i in _FILES:
        input_data = "file:///{}/input/{}".format(_SPARK_PATH,i)
        sdf = read_file_spark(sqlContext, input_data, data_schema)
        sdf_list.append(sdf)
    
    sdf = sdf_union(sdf_list)
    sdf = sdf_trans(sdf)
    sdf = sdf_filter(sdf, u"main_use ='住家用' AND building_state like '住宅大樓%' and total_floor_trans >= 13")
    sdf = sp_gadget.result_json_schema(sdf)
    output_path="/{}/output".format(_SPARK_PATH)

    sp_gadget.create_result_dir(output_path)
    sp_gadget.export_results(sdf, "file://{}".format(output_path), _OUTPUT_FILE_NUM)
    sp_gadget.trans_result(output_path, _OUTPUT_FILE_NUM)



if __name__ == '__main__':
    main()
    # test()
    