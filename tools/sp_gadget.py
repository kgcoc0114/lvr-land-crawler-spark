# encoding: utf-8
"""
Name: tools/sp_gadget.py
Desc: spark functions
Note:
"""

import pyspark.sql.functions as F
import tools.files as files

# 中文數字轉換
def number_trans(ch_num):
    if ch_num:
        ch_num = ch_num.replace(u'層','')
        digit = {u'一': 1, u'二': 2, u'三': 3, u'四': 4, u'五': 5, u'六': 6, u'七': 7, u'八': 8, u'九': 9}
        num = 0
        if ch_num:
            idx_b, idx_s = ch_num.find('百'), ch_num.find('十')
        
        if idx_b != -1:
            num  = num + digit[ch_num[idx_b - 1:idx_b]] * 100

        if idx_s != -1:
            num = num + digit.get(ch_num[idx_s - 1:idx_s], 1) * 10
    
        if ch_num[-1] in digit:
            num = num+digit[ch_num[-1]]

        return num
    else:
        return 0

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

# 生成output資料夾
def create_result_dir(output_path):
    if files.exist_or_not(output_path):
        files.remove_dir(output_path)
        files.create_dir(output_path)
    else:
        files.create_dir(output_path)

# 匯出檔案
def export_results(sdf, output_path, output_file_num):
    sdf_list = sdf.randomSplit([0.5, 0.5], 1)
    for i in range(output_file_num):
        sdf_list[i].coalesce(1).write.json("{}/result{}".format(output_path, i+1))

# 重新命名
def trans_result(output_path, output_file_num):
    for i in range(output_file_num):
        file_list = files.get_all_files("{}/result{}".format(output_path, i+1))
        json_name = list(filter(lambda x: 'json' in x and '.crc' not in x, file_list))[0]
        files.rename_file("{}/result{}/{}".format(output_path, i+1,json_name),
                        "{}/result-part{}.json".format(output_path, i+1))
        files.remove_dir("{}/result{}".format(output_path, i+1))

