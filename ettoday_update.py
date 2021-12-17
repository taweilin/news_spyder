# 因有候補關鍵字狀態，故新增此程式
# 可先參考ettoday.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
#from crawlab import save_item
from sqlalchemy import create_engine
import pymssql

URLID=1

def parse_get_keyword(url):
    #print(url)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    # catch no article
    if soup.find('em').getText() != '404錯誤':
        result = soup.find('div', class_ = 'part_tag_1 clearfix')
        if result != None:
            hand_tags = result.find_all('a')
            tags=[]
            for tag in hand_tags:
                tag = tag.getText()
                tag = tag.strip()
                tags.append(tag)
        else:
            tags=[]
    else:
        tags=[]
    #print(tags)
    return tags

def parse_get_keyword2(url):
    #print(url)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    # catch no article
    #print(df_exist.loc[i]['URL'])
    if soup.find('em').getText() != '404錯誤':
        result = soup.find('p', class_ = 'tag')
        if result != None:
            hand_tags = result.find_all('a')
            tags=[]
            for tag in hand_tags:
                tag = tag.getText()
                tag = tag.strip()
                tags.append(tag)
        else:
            tags=[]
    else:
        tags=[]
    #print(tags)
    return tags

TableName = "NewsKeywords"
domain = "https://finance.ettoday.net/news/"
now = datetime.datetime.now()
engine = create_engine('mssql+pymssql://data:data123@192.168.2.11:1433/data')
  
days=[]

i=0
dayn = now + datetime.timedelta(days=-i)
day = (dayn.year, dayn.month, dayn.day)

daynlast = dayn + datetime.timedelta(days=-i+(i+1))
daylast = (daynlast.year, daynlast.month, daynlast.day)

dayn10 = dayn + datetime.timedelta(days=-(i+10))
day10 = (dayn10.year, dayn10.month, dayn10.day)
#print(daylast, day, day10)

#取得關鍵字為空的文章編號
sql = "select PageID from "+ TableName +" where Keywords ='' and PostTime > '"+ str(day10[0]) +"-"+ str(day10[1]) +"-"+ str(day10[2])+"' and right([PageID],3)='"+str(URLID).zfill(3)+"'"

df_blank = pd.read_sql(sql,engine)
#df_blank.sort_values(by=['PageID'], ascending=False,inplace=True)

records=[]
for page_id in df_blank.PageID:
    # 取文章編號
    url = domain+page_id[8:-3]
    keyword = ','.join(parse_get_keyword(url))
    if keyword == '':
        keyword = ','.join(parse_get_keyword2(url))
    if keyword != '':
        records.append({'PageID': page_id,'Keywords': keyword})

# 無法批次處理後再更新，只能一筆筆確認後更新到資料庫
conn = pymssql.connect(server='192.168.2.11:1433', user='data', password='data123', database='data')  
cursor = conn.cursor()
for rec in range(len(records)):
    cursor.execute("update " + TableName + " set Keywords ='"+records[rec]['Keywords']+"' where PageID = '"+records[rec]['PageID']+"'")
    conn.commit()
conn.close()
print(len(records))
