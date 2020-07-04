# encoding: utf-8
"""
Name: land_data_etl.py
Desc: land_data etl with spark
Note:
-- add city column
"""

# import findspark
# findspark.init()

import sys
import os
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import tools.sp_gadget as sp_gadget

_SPARK_PATH = '/'.join(os.path.abspath(__file__).replace('\\','/').split('/')[1:-1])
_FILES = ["a_lvr_land_a.csv", "b_lvr_land_a.csv", "e_lvr_land_a.csv", "f_lvr_land_a.csv", "h_lvr_land_a.csv"]
_OUTPUT_FILE_NUM = 2

def main():
    # initial
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    # lvr data params
    ld = sp_gadget.LvrData()
    ld.mode = 'DF'
    # get file desc
    ld.get_city_map(sc)
    # set schema
    ld.data_schema = StructType([
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
            StructField('serial_num', StringType(), True)])
    
    # get lvr datafeed
    result_df = sp_gadget.get_data(sqlContext, ld)
    # analyze
    result_df = sp_gadget.analyze(result_df, ld.mode)
    # set output folder
    output_path="/{}/output".format(_SPARK_PATH)
    # export result
    # sp_gadget.export_result(result_df, _SPARK_PATH, output_path, _OUTPUT_FILE_NUM)

if __name__ == '__main__':
    main()
    
