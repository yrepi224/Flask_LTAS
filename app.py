from flask import Flask, render_template, request, redirect
import psycopg2
from upload_multiple_csv import upload_multiple_csv
from module import all_select_data, share_select_data, emergency_upload, bad_host
from upload_csv import upload_csv


app = Flask(__name__)

conn = psycopg2.connect(database="ryu", user="sungwonryu", host="127.0.0.1", password="7887", port="5432")
cur = conn.cursor()


@app.route('/')
def main_page():
    """
    메인페이지
    """
    return render_template('main.html')


@app.route('/data/<table>')
def raw_select(table):
    """
    RawData 출력
    """
    try:
        query = f'''SELECT * FROM public."{table}" ORDER BY "filename", "app", "service"'''
        if table == 'ShareData':
            query = f'''SELECT * FROM public."{table}" ORDER BY "filename", "ip", "app", "service"'''
        cur.execute(query)
        rs = cur.fetchall()
        rlen = len(rs[0])
        return render_template('raw.html', rs=rs, rlen=rlen, table=table)
    except:
        return f'There is no table named {table}'


@app.route('/delete/<table>/<Pkey>')
def delete(table, Pkey):
    """
    선택된 row 삭제
    """
    try:
        query = f"""DELETE FROM public."{table}" WHERE "Pkey" = '{Pkey}'"""
        cur.execute(query)
        conn.commit()
        print(f'Deleted {query}')
        return redirect(f"/data/{table}")
    except:
        return 'There was an issue deleting your task'


@app.route('/upload_csv', methods =['POST', 'GET'])
def csv_upload():
    """
    csv파일을 RawData로 push한다.
    """
    if request.method == 'POST':
        try:
            foldername = request.form['folder_name']
            filename = request.form['file_name']
            upload_multiple_csv(foldername, filename)
            return redirect('/upload_csv')
        except:
            return 'upload failed'
    else:
        return render_template('upload_csv.html')


@app.route('/upload_AllData', methods=['POST', 'GET'])
def AllData_upload():
    """
    RawData에서 AllData로 데이터 가공을 하고 push한다.
    """
    if request.method == 'POST':
        try:
            upload_csv(request.form['filename'])
            return redirect('/data/AllData')
        except:
            return '오류'
    else:
        table = 'RawData'
        query = f'''SELECT * FROM public."{table}" ORDER BY "app", "service"'''
        cur.execute(query)
        rs = cur.fetchall()
        rlen = len(rs[0])
        return render_template('raw.html', rs=rs, rlen=rlen, table=table)

@app.route('/report/')
def report_page():
    """
    보고서 페이지로 리다이렉트
    """
    return render_template('report.html')


@app.route('/report/<csv>', methods=['POST', 'GET'])
def Report_excel(csv):
    """
    엑셀파일로 보고서를 출력한다.
    """
    if csv == 'all_data':
        try:
            all_select_data(request.form['all_data'])
            return '성공'
        except:
            return '실패'
    elif csv == 'share_data':
        try:
            share_select_data(request.form['share_data'])
            return '성공'
        except:
            return '실패'


@app.route('/upload/emergency/', methods=['POST','GET'])
def emergency():
    """
    보고서 파일을 데이터베이스에 한번에 입력한다. 
    """
    if request.method == 'POST':
        try:
            foldername = request.form['folder_name']
            filename = request.form['file_name']
            emergency_upload(foldername, filename)
            return redirect('/upload/emergency')
        except:
            return 'upload failed'
    else:
        return render_template('upload_csv.html',emergency = 'emergency')


@app.route('/badhosts', methods= ['POST', 'GET'])
def badhosts():
    """
    임시 Bad Host 보여준다.
    """
    if request.method =='POST':
        try:
            bad_host.extend([request.form['host']])
            return render_template('badhosts.html', bad_host=bad_host, enumerate=enumerate, update=request.form['host'])
        except:
            pass
    else:
        return render_template('badhosts.html', bad_host=bad_host, enumerate=enumerate)


if __name__ == "__main__":
    app.run(debug=False)
