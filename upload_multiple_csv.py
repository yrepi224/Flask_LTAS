# -*- coding:utf-8 -*-
import csv
import glob
import os
import sys
from module import upload_db
from pathlib import Path


def upload_multiple_csv(folder_name, file_name):
    raw_data = dict()
    print('Try it Multiple Times')
    name_app = folder_name
    file_name = file_name
    input_path = f'/Users/sungwonryu/Documents/GitHub/Flask_LTAS/csv/{name_app}'
    for input_file in glob.glob(os.path.join(input_path, '*.csv')):
        name_service = Path(input_file).stem
        with open(input_file, 'r', newline='') as csv_in_file:
            filereader = csv.reader(csv_in_file)
            header = next(filereader)
            # 전체 데이터 딕셔너리 host_data[0] = Domain, [1] = IP, [2] = App, [3] = Service, [4] = Filename, [5] = 공통여부 [5] = 날짜
            for row_value in filereader:
                raw_data[name_app+'__'+name_service+'__'+row_value[1]] = raw_data.get(
                    row_value[1], [row_value[1], row_value[2], name_app, name_service, file_name])    

    # 데이터베이스 insert - 전체 데이터
    upload_db(raw_data)
