# encoding: utf-8
"""
Name: land_data_crawler.py
Desc: land crawler 
Note:
"""
import sys
import os
from pathlib import Path
from urllib import parse
import shutil
import pandas as pd

import tools.times as times
import tools.files as files
import tools.request as request

_BASE_PATH = '/'.join(os.path.abspath(__file__).replace('\\','/').split('/')[:-1])
_CURRENT_DATE = times.get_today()
_FILES = ["a_lvr_land_a.csv", "b_lvr_land_a.csv", "e_lvr_land_a.csv", "f_lvr_land_a.csv", "h_lvr_land_a.csv"]

    
class LandData(object):
    def __init__(self, file_list):
        self.upzip_target_dir = ""
        self.zip_file_path = ""
        self.zip_file = ""
        self.file_city = {}
        self.land_dir = "{}/land_data".format(_BASE_PATH)
        self.file_list = file_list
    
    # 判斷資料夾
    def check_land_dir(self):
        files.remove_dir(dir_path=self.land_dir)
        files.dynamic_create_dir(self.land_dir)
    
    # 取得資料
    def get_land_datafeed(self, pub_date=None, year=None, season=None):
        # 當期/非當期/季
        if pub_date and not (year and season):
            url = "https://plvr.land.moi.gov.tw/DownloadHistory?type=history&fileName={}".format(pub_date)
        elif not pub_date and (year and season):
            url = "https://plvr.land.moi.gov.tw//DownloadSeason?season={}S{}&type=zip&fileName=lvr_landcsv.zip".format(year, season)
        else:
            url = "https://plvr.land.moi.gov.tw//Download?type=zip&fileName=lvr_landcsv.zip"
        
        params = parse.parse_qs( parse.urlparse(url).query)
        zip_file_name = 'lvr_landcsv.zip'
        self.zip_file_path = 'lvr_landcsv.zip'
        # 檢查result dir
        self.check_land_dir() 

        self.zip_file_path = "{}/{}".format(self.land_dir, self.zip_file_path)
        self.upzip_target_dir = "{}/{}".format(self.land_dir, zip_file_name.split(".")[0])
        # 下載
        request.download_file(url=url, target_path=Path(self.zip_file_path))
        
        # 解壓縮至資料夾
        files.dynamic_create_dir(Path(self.upzip_target_dir))
        
        # 解壓縮
        files.upzip(Path(self.zip_file_path), Path(self.upzip_target_dir))
    
    # 取得檔案的城市
    def get_file_city(self):
        with open(Path("{}/land_data/lvr_landcsv/manifest.csv".format(_BASE_PATH)), 'r', encoding="utf-8") as f:
            for f in f.readlines():
                tmp_split = f.split(',')
                if tmp_split[0] in self.file_list:
                    self.file_city["{}".format(tmp_split[0])] = tmp_split[2][0:3]
    
    # 標記城市
    def add_city_col(self, file_name, src_path, target_path):
        df = pd.read_csv(src_path)
        df["縣市"] = self.file_city["{}".format(file_name)]
        df["備註"] = df["備註"].replace({',':''}, regex=True)
        df.to_csv(target_path, index=False, encoding='utf_8_sig')

    # 將需要分析檔案移到input中
    def move_files(self, upzip_target_dir=None):
        if not self.upzip_target_dir:
            upzip_target_dir = upzip_target_dir
        else:
            upzip_target_dir = self.upzip_target_dir
        
        input_path = "{}/input".format(_BASE_PATH)
        if files.exist_or_not(file_dir=input_path):
            files.remove_dir(dir_path=input_path)
            files.create_dir(input_path)
        else:
            files.create_dir(input_path)

        for i in self.file_list:
            self.add_city_col(i, "{}/{}".format(upzip_target_dir, i), 
                        "{}/input/{}".format(_BASE_PATH,i))

def main(argv):
    crawler_mode = ""
    year = ""
    season = ""
    pub_date = ""
    if argv[1] == "hist_date":
        crawler_mode = "hist_date"
        pub_date = argv[2]
    elif argv[1] == "hist_season":
        if "S" in argv[2]:
            crawler_mode = "hist_season"
            tmp_s = argv[2].split("S")
            year = tmp_s[0]
            season = int(tmp_s[1])
    elif argv[1] == "curr":
        crawler_mode = "curr"
    
    file_list = list(filter(lambda f: ".csv" in f or ".CSV" in f, argv))
    if not file_list:
        file_list = _FILES
    else:
        file_list = file_list[0].split(",")

    land_data = LandData(file_list)
    if crawler_mode == 'curr':
        land_data.get_land_datafeed()
    elif crawler_mode == 'hist_season':
        land_data.get_land_datafeed(year=year, season=season)
    elif crawler_mode == 'hist_date':
        print(crawler_mode)
        land_data.get_land_datafeed(pub_date=pub_date)

    land_data.get_file_city()
    land_data.move_files(upzip_target_dir="land_data/lvr_landcsv")

if __name__ == '__main__':
    main(sys.argv)
