# encoding: utf-8
"""
Name: tools/times.py
Desc: the functions about time
Note:
"""

import time
import datetime

# 取得今天日期
def get_today():
    return datetime.datetime.today()

# datetime轉字串
def datetime2str(datetime_in, dt_format="%Y%m%d"):
    return datetime.datetime.strftime(datetime_in, dt_format)

# 字串轉datetime
def str2datetime(date_str, dt_format="%Y-%m-%d"):
    return datetime.datetime.strptime(date_str, dt_format)