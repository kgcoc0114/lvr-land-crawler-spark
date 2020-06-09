# encoding: utf-8
"""
Name: tools/files.py
Desc: File Handling 
Note:
"""
import os
import zipfile
import shutil

# 判斷檔案/資料夾是否存在
def exist_or_not(file_dir):
    if not os.path.isdir(file_dir):
        return False
    return True

# 新增資料夾
def create_dir(dir_path):
    os.mkdir(dir_path)

# 移除資料夾
def remove_dir(dir_path):
    shutil.rmtree('{}'.format(dir_path), ignore_errors=True)

# 複製檔案
def copy_file(source, target):
    shutil.copyfile(source, target)

# 移動檔案
def move_file(source, target):
    shutil.move(source, target)

# 重新命名
def rename_file(source, target):
    os.rename(source, target)

# 取得資料夾裡所有檔案
def get_all_files(dir_path):
    return [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]

# 解壓縮
def upzip(zip_file, file_dir):
    with zipfile.ZipFile(zip_file, 'r') as zp:
        for file in zp.namelist():
            zp.extract(file,file_dir)

def dynamic_create_dir(dir_path):
    if not exist_or_not(file_dir=dir_path):
        create_dir(dir_path=dir_path)