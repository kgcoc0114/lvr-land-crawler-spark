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

def dt2AD(date_str, to_dt_format=None):
    year = int(date_str[0:3]) + 1911
    month = date_str[3:5]
    day = date_str[5:]
    if to_dt_format:
        return year, month, day
    return "{}-{}-{}".format(year, month, day)

def dt2AD_dt(date_str):
    year, month, day = dt2AD(date_str, to_dt_format=True)
    return str2datetime("{}-{}-{}".format(year,month,day))