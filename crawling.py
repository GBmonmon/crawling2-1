import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import time
import sqlite3


data = {
    'response': 'json',
    'date': '20190312'
}
#res = requests.get('http://www.tse.com.tw/exchangeReport/MI_INDEX', params=data)
res = requests.get('http://www.tse.com.tw/exchangeReport/MI_INDEX?response=json&date=20190312')
#print(res.text)
#loads data to json.text
jres = json.loads(res.text)
jres['stat']
jres
jres['data1']
#print(jres['data1'])
#create a data frame
df_temp = pd.DataFrame(jres['data1'],columns=jres['fields1'])
#print(df_temp)

#build time index
timedelta(days=1)
datetime(2019,3,12) + timedelta(1)
datetime.strftime(datetime(2019,1,30), '%Y%m%d')#datetime formate
#create a empty data frame
column_list = list(df_temp['指數'])

column_list.append('date')
df = pd.DataFrame(columns=column_list)
print(df)

#crawling
crawl_date = datetime(2019,3,12) # start_date

# create database cursor
conn = sqlite3.connect('data.db')
c = conn.cursor()

# create table

c.execute('''CREATE TABLE IF NOT EXISTS data1
             (
                 公司 text,
                 台股萬點 real,
                 htmlCode text,
                 漲跌 real,
                 百分比 real
             )
             ''')
conn.commit()


for i in range(365):
    crawl_date -= timedelta(1)
    crawl_date_str = datetime.strftime(crawl_date, '%Y%m%d')
    res = requests.get('http://www.tse.com.tw/exchangeReport/MI_INDEX?response=json&date=' + crawl_date_str)
    jres = json.loads(res.text)

    # 證交所回覆有資料
    if(jres['stat']=='OK'):
        dataToFetch = jres['data1']

        for index in range(len(dataToFetch)):
            #print(dataToFetch[index],crawl_date_str )
            公司 = dataToFetch[index][0]
            台股萬點 = dataToFetch[index][1]
            htmlCode = dataToFetch[index][2]
            漲跌 = dataToFetch[index][3]
            百分比 = dataToFetch[index][4]
            print('Inserting: ',公司,台股萬點,htmlCode,漲跌,百分比)

            c.execute('''
               INSERT INTO data1 (公司,台股萬點,htmlCode,漲跌,百分比)
                VALUES(?,?,?,?,?)
            ''',(公司,台股萬點,htmlCode,漲跌,百分比))
        conn.commit()



    else:
        print(crawl_date_str, ': no data')

    # sleep 3 sec avoid blocked
    time.sleep(3)
conn.close()
