# encoding: utf-8
"""
Name: land_data_etl_rdd.py
Desc: land_data etl with spark
Note:
-- add city column
"""
# import findspark
# findspark.init()

import sys
import os
from pyspark import SparkContext
import tools.sp_gadget as sp_gadget

_SPARK_PATH = '/'.join(os.path.abspath(__file__).replace('\\','/').split('/')[1:-1])
_FILES = ["a_lvr_land_a.csv", "b_lvr_land_a.csv", "e_lvr_land_a.csv", "f_lvr_land_a.csv", "h_lvr_land_a.csv"]
_OUTPUT_FILE_NUM = 2

def main():
    # initial
    sc = SparkContext()
    # lvr data params
    ld = sp_gadget.LvrData()
    ld.mode = "RDD"
    # get file desc
    ld.get_city_map(sc)
    
    # get lvr datafeed
    result_rdd = sp_gadget.get_data(sc, ld)
    # analyze
    result_rdd = sp_gadget.analyze(result_rdd, ld.mode, header=ld.header)
    # set output folder
    output_path="/{}/output".format(_SPARK_PATH)
    # export result
    sp_gadget.export_result(result_rdd, _SPARK_PATH, output_path, _OUTPUT_FILE_NUM)
    
if __name__ == '__main__':
    main()
    
