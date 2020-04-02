# -*- coding:utf-8 -*-
import psycopg2
from openpyxl import Workbook, load_workbook
from openpyxl.styles import NamedStyle, Font, Border, Side


bad_ip = ['192.168.1.1', '172.217.175.101', '172.217.175.101', '172.217.175.10', '161.69.29.185', '35.224.99.156', '172.217.31.138', '172.217.27.78', '172.217.175.67', '172.217.161.67', '52.199.155.35', '13.125.72.55',
          '172.217.161.74', '172.217.24.138', '52.69.18.116', '172.217.31.170', '172.217.26.14', '216.58.197.170', '64.233.189.188', '52.69.13.64', '172.217.25.99', '91.189.89.198', '35.222.85.5', '216.58.197.202',
          '157.240.215.16', '172.217.27.74', '13.35.126.100', '52.26.99.174', '13.225.105.58', '216.58.197.142', '52.36.10.27', '13.227.76.120']


# 데이터베이스 insert
def upload_db(raw_data):
    conn = psycopg2.connect(database="ryu",
                            user="sungwon",
                            host="127.0.0.1",
                            password="7887",
                            port="5432")
    cur = conn.cursor()
    cnt = 0
    fcnt = 0
    for key, value in raw_data.items():
        try:
            cur.execute(
                f"""INSERT INTO public."RawData"("Pkey", "host", "ip", "app", "service", "filename")VALUES({key},{value[0]},{value[1]},{value[2]},{value[3]},{value[4]})""")
            conn.commit()
            cnt = cnt + 1
            print('successfully imported data!         '+str(cnt)+'  '+key)
        except:
            fcnt = fcnt + 1
            print('fail count ('+str(fcnt)+')')
            conn.rollback()
    print('Imported ('+str(cnt)+') lines of All Data!')
    print('Failed ('+str(fcnt)+') lines of All Data!')
    print('------------------------------------------------------------------------------')


# 데이터베이스 all_data
def all_select_data(report):
    highlight = NamedStyle(name="highlight")
    highlight.font = Font(bold=True, size=11)
    bd = Side(style='thick', color="000000")
    highlight.border = Border(left=bd, top=bd, right=bd, bottom=bd)
    connection = psycopg2.connect(database="ryu",
                                  user="sungwon",
                                  host="127.0.0.1",
                                  password="7887",
                                  port="5432")

    try:
        with connection.cursor() as cursor:
            query = f'''SELECT "host", "ip", "app", "service", "duplicate" FROM public."AllData" WHERE "filename" LIKE'%{report}%'ORDER BY "app", "service", "ip"'''
            cursor.execute(query)
            rs = cursor.fetchall()
            wb = Workbook()
            # DB 모든 데이터 엑셀로
            name = 'NULL'
            num = 0
            for row in rs:
                if name != row[2]:
                    wb.create_sheet(index=num, title=row[2])
                    ws = wb.get_sheet_by_name(row[2])
                    name = row[2]
                    num = num + 1
                    ws.append(['Domain', 'IP', 'App', 'Service', '공통여부'])
                ws.append(row)
                if row[4] is True:
                    ws['A'+str(ws.max_row)].style = highlight
            wb.remove(wb['Sheet'])
            wb.save(report+' 계열 보고서.xlsx')
            print(f'Successfully Saved {report}.xlsx')
    finally:
        connection.close()
        wb.close()

# 데이터베이스 Share_data


def share_select_data(report):
    connection = psycopg2.connect(database="ryu",
                                  user="sungwon",
                                  host="127.0.0.1",
                                  password="7887",
                                  port="5432")
    try:
        with connection.cursor() as cursor:
            query = f'''SELECT "host", "ip", "app", "service" FROM public."ShareData" WHERE "filename" LIKE'%{report}%'ORDER BY "ip", "service", "app"'''
            cursor.execute(query)
            rs = cursor.fetchall()
            wb = Workbook()
            ws = wb.active
            wb['Sheet'].title = '공통'
            # 첫행 입력
            ws.append(('Domain', 'IP', 'App', 'Service'))

            # DB 모든 데이터 엑셀로
            for row in rs:
                ws.append(row)
            wb.save(report+' 공통.xlsx')
            print(f'Successfully Saved {report} 공통.xlsx')
    finally:
        connection.close()
        wb.close()


# 데이터베이스 raw_select
def select_raw_data(filename):

    report = filename
    connection = psycopg2.connect(database="ryu",
                                  user="sungwon",
                                  host="127.0.0.1",
                                  password="7887",
                                  port="5432")

    try:
        with connection.cursor() as cursor:
            query = f'''SELECT "host", "ip", "app", "service" FROM public."RawData" WHERE "filename" LIKE'%{report}%'ORDER BY "app", "service"'''
            cursor.execute(query)
            rs = cursor.fetchall()
            wb = Workbook()
            # DB 모든 데이터 엑셀로
            name = 'NULL'
            num = 0
            for row in rs:
                if name != row[2]:
                    wb.create_sheet(index=num, title=row[2])
                    ws = wb.get_sheet_by_name(row[2])
                    name = row[2]
                    num = num + 1
                ws.append(row)
            wb.remove(wb['Sheet'])
            wb.save(report+'.xlsx')
            report_name = (f'{report}.xlsx')
            return report_name
    finally:
        connection.close()
        wb.close()


def upload_sorted_data(raw_data):
    conn = psycopg2.connect(database="ryu",
                            user="sungwon",
                            host="127.0.0.1",
                            password="7887",
                            port="5432")

    cur = conn.cursor()
    host_data = dict()
    # host_data로 데이터 이전 및 호스트명 통일
    for key, value in raw_data.items():
        host_data[value[2]+'__'+value[3]+'__'+value[1]] = [value[0],
                                                           value[1], value[2], value[3], value[4], 'FALSE']
    for key, host_val in host_data.items():
        for key, raw_val in raw_data.items():
            if host_val[1] == raw_val[1] and host_val[0] == host_val[1] and host_val[3]+host_val[4] != raw_val[3]+raw_val[4] and raw_val[0] != raw_val[1]:
                host_data[key] = [raw_val[0], host_val[1],
                                  host_val[2], host_val[3], host_val[4], 'FALSE']
                break

    # 공통 호스트 변경 작업 1
    for key1, value1 in host_data.items():
        for key, value2 in host_data.items():
            if value1[1] == value2[1] and value1[2] != value2[2] or value1[0] == value2[0] and value1[2] != value2[2]:
                host_data[key1] = [value1[0], value1[1],
                                   value1[2], value1[3], value1[4], 'TRUE']

    # 공통 호스트 변경 작업 2
    for key1, value1 in host_data.items():
        for value2 in host_data.items():
            if value1[5] == 'FALSE':
                if value1[0] == value2[0] and value2[5] == 'TRUE':
                    host_data[key1] = [value1[0], value1[1],
                                       value1[2], value1[3], value1[4], 'TRUE']

    # 데이터베이스 insert - 전체 데이터
    cnt = 0
    fcnt = 0
    for key, value in host_data.items():
        try:
            cur.execute(f"""INSERT INTO public."AllData"("Pkey", "host", "ip", "app", "service", "filename", "duplicate")           
            VALUES({key},{value[0]},{value[1]},{value[2]},{value[3]},{value[4]},{value[5]})""")
            conn.commit()
            cnt = cnt + 1
            print('successfully imported data!         '+str(cnt)+'  '+key)
        except:
            fcnt = fcnt + 1
            print('fail count ('+str(fcnt)+')')
            conn.rollback()
    print('Imported ('+str(cnt)+') lines of All Data!')
    print('Failed ('+str(fcnt)+') lines of All Data!')
    print('------------------------------------------------------------------------------')

    # 데이터베이스 insert - 공통데이터
    cnt = 0
    fcnt = 0
    for key, value in host_data.items():
        if value[5] == 'TRUE':
            try:
                cur.execute(f"""INSERT INTO public."ShareData"("Pkey", "host", "ip", "app", "service", "filename")           
                VALUES({key},{value[0]},{value[1]},{value[2]},{value[3]},{value[4]})""")
                conn.commit()
                cnt = cnt + 1
                print('successfully imported data!         '+str(cnt)+'  '+key)
            except:
                fcnt = fcnt + 1
                print('fail count ('+str(fcnt)+')')
                conn.rollback()
    conn.close()
    print('Imported ('+str(cnt)+') lines of Share Data!')
    print('Failed ('+str(fcnt)+') lines of Share Data!')


# def delete_cdn():
#     bad_cdn = ['apple', 'google', 'nrt', 'doubleclick', 'onesignal', 'd3fmvko', 'elasticbeanstalk.com',
#                'android', 'amazonaws.com', 'app-measurement.com', 'cloudfront.net', 'canonical', 'akamai']
#     report = input('"Delete"_Enter report name: (계열이름 LIKE)')
#     connection = psycopg2.connect(database="ryu",
#                                   user="sungwon",
#                                   host="127.0.0.1",
#                                   password="7887",
#                                   port="5432")
#     #del_query = '''SELECT FROM public."AllData" WHERE filename = '''+"'"+report+'''' AND host like '%'''+cdn+"%'"
#     try:
#         with connection.cursor() as cursor:
#             bad_pkey = list()
#             for cdn in bad_cdn:
#                 query = '''select "Pkey" FROM public."RawData" WHERE filename = '''+"'"+report+'''' AND host like '%'''+cdn+"%'"
#                 show_me_the_ip = cursor.execute(query)
#             for pkey in bad_pkey:
#                 cursor.execute('''DELETE FROM public."RawData" WHERE "Pkey" = %s''', (str(pkey),))
#     finally:
#         connection.close()
# delete_cdn()
    # try:
    #     with connection.cursor() as cursor:
    #         query = '''SELECT "host", "ip", "app", "service" FROM public."RawData" WHERE "filename" LIKE'%''' + \
    #             report+'''%'ORDER BY "app", "service"'''
# **개별실행**
# upload_db()
# all_select_data()
# share_select_data()
# select_raw_data()
# upload_sorted_data(raw_data필요)
