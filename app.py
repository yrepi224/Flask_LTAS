from flask import Flask, render_template, request, redirect
import psycopg2
from upload_multiple_csv import upload_multiple_csv
from module import all_select_data, share_select_data
from upload_csv import upload_csv


app = Flask(__name__)

conn = psycopg2.connect(database="test", user="sungwon", host="127.0.0.1", password="7887",port="5432")
cur = conn.cursor()


@app.route('/')
def main_page():
    return render_template('main.html')


@app.route('/data/<table>', methods=['POST', 'GET'])
def raw_select(table):
    if request.method == 'POST':
        try:
            pass
        except:
            return 'Something wrong'
    else:
        try:
            query = f'''SELECT * FROM public."{table}" ORDER BY "app", "service"'''
            cur.execute(query)
            rs = cur.fetchall()
            rlen = len(rs[0])
            return render_template('raw.html', rs=rs, rlen=rlen, table=table)
        except:
            return f'There is no table named {table}'


@app.route('/delete/<table>/<Pkey>')
def delete(table, Pkey):
    try:
        query = f"""DELETE FROM public."{table}" WHERE "Pkey" = '{Pkey}'"""
        cur.execute(query)
        conn.commit()
        print(f'Deleted {query}')
        return redirect(f"/data/{table}")
    except:
        return 'There was an issue deleteing your task'


@app.route('/upload_csv', methods =['POST', 'GET'])
def csv_upload():
    if request.method == 'POST':
        try:
            foldername = request.form['folder_name']
            filename = request.form['file_name']
            upload_multiple_csv(foldername, filename)
            return render_template('/')
        except:
            return 'upload fail'
    else:
        return render_template('upload_csv.html')


@app.route('/upload_AllData', methods=['POST', 'GET'])
def AllData_upload():
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
    return render_template('report.html')


@app.route('/report/<csv>', methods=['POST', 'GET'])
def Report_excel(csv):
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


if __name__ == "__main__":
    app.run(debug=False)
