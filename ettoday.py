import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
# 寫入crawlab的monogo使用
#from crawlab import save_item
from sqlalchemy import create_engine

URLID=1

# 解析取得新聞列表後取得新聞關鍵字的處理
def parse_get_keyword(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    # 處理網頁已下架無法解析的判斷
    if soup.find('em').getText() != '404錯誤':
        result = soup.find('div', class_ = 'part_tag_1 clearfix')
        # 判斷無關鍵字的新聞，如無關鍵字時，文章的處理
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
    return tags

# 對應新網站格式處理，解析取得新聞列表後取得新聞關鍵字的處理
def parse_get_keyword2(url):
    #print(url)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    # 處理網頁已下架無法解析的判斷
    if soup.find('em').getText() != '404錯誤':
        result = soup.find('p', class_ = 'tag')
        # 判斷無關鍵字的新聞，如無關鍵字時，文章的處理
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

# 儲存資料庫與網頁定義
TableName = "NewsKeywords"
domain = "https://www.ettoday.net"
# 時間處理
now = datetime.datetime.now()
# DB連線
engine = create_engine('mssql+pymssql://data:data123@192.168.2.11:1433/data')
  
days=[]

for i in range(5):
    # 處理日期格式
    dayn = now + datetime.timedelta(days=-i)
    day = (dayn.year, dayn.month, dayn.day)
    
    daynlast = dayn + datetime.timedelta(days=-i+(i+1))
    daylast = (daynlast.year, daynlast.month, daynlast.day)
    
    dayn10 = dayn + datetime.timedelta(days=-(i+10))
    day10 = (dayn10.year, dayn10.month, dayn10.day)
    #print(daylast, day, day10)

    response = requests.get("https://www.ettoday.net/news/news-list-"+ str(day[0]) +"-"+ str(day[1]) +"-"+ str(day[2]) +"-17.htm")
    # 抓取過去10天編號，與資料庫比對，抓取過的文章不處理
    sql = "select PageID from "+ TableName +" where PostTime >='"+ str(day10[0]) +"-"+ str(day10[1]) +"-"+ str(day10[2]) + "' and PostTime <= '"+ str(daylast[0]) +"-"+ str(daylast[1]) +"-"+ str(daylast[2])+"' and right([PageID],3)='"+str(URLID).zfill(3)+"'"
        
    df_exist = pd.read_sql(sql,engine)

    soup = BeautifulSoup(response.text, "html.parser")
    result = soup.find('div', class_= 'part_list_2')
    articles = result.find_all('h3')

    records=[]
    for article in articles:
        word = article.find('a').get('href')
        word = word.replace('.htm','')
        word = word.split('/')
        # 文章編號為：日期+網站文章編號+抓取網址編號
        page_id = word[2]+word[3]+str(URLID).zfill(3)
        # 判斷文章是否抓過
        if page_id not in df_exist['PageID'].tolist():
            announce_time = article.find('span').getText()
            title = article.find('a').getText()
            #取得新聞編號
            url = domain+article.find('a').get('href')
            # 抓取每篇文章關鍵字
            keyword = ','.join(parse_get_keyword(url))
            if keyword == '':
                # 抓不到關鍵字，使用新網站模式再抓一次
                keyword = ','.join(parse_get_keyword2(url))
            #儲存以批次寫入資料庫
            records.append({'PageID': page_id,
                            'PostTime': announce_time,
                            'Title': title,
                            'URL': url,
                            'Keywords': keyword})

    news = pd.DataFrame(records)
    # 新增進入資料庫
    news.to_sql(TableName, engine, if_exists='append', index=False)
