# encoding: utf-8
"""
Name: tools/request.py
Desc: the functions about request
Note:
"""
import requests
from urllib import parse

# 下載檔案
def download_file(url=None, target_path=None):
    # 取得檔名
    if not target_path:
        params = parse.parse_qs(parse.urlparse(url).query)
        target_path = params["fileName"][0]
    # 下載
    r = requests.get(url)
    with open(target_path, 'wb') as f:
        f.write(r.content)