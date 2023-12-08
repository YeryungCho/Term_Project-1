#버스정류장 기사 크롤링
import requests
from bs4 import BeautifulSoup
from newspaper import Article
import pandas as pd
from newspaper import Config
from datetime import datetime
import os

titles = []
contents = []
timestamps = []

config = Config()
config.verify_ssl = False
config.memoize_articles = True
config.request_timeout = 60

for i in range(1, 1000000, 10):
    link = "https://search.naver.com/search.naver?where=news&sm=tab_pge&query=%EB%B2%84%EC%8A%A4%EC%A0%95%EB%A5%98%EC%9E%A5%20%ED%8E%B8%EC%9D%98%20%EC%A0%91%EA%B7%BC%EC%84%B1%20%ED%8E%B8%EB%A6%AC&sort=1&photo=0&field=0&pd=2&ds=2023.11.08&de=2023.12.08&mynews=0&office_type=0&office_section_code=0&news_office_checked=&office_category=0&service_area=0&nso=so:dd,p:1m,a:all&start=" + str(i)
    response = requests.get(link)
    soup = BeautifulSoup(response.text, 'html.parser')
    url = soup.select('#ct > div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > a')

    news_titles = soup.find_all('a', class_='news_tit')

    if len(news_titles) < 10:
        print(f" - {i//10 + 1}페이지에서 기사 수가 10개 미만이므로 크롤링 중단")
        break

    for title in news_titles:
        news_url = title['href']

        try:
            article = Article(news_url, language='ko', config=config)
            article.download()
            article.parse()

            titles.append(title.get_text(strip=True))
            contents.append(article.text)
            timestamps.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            print(f" - {i//10 + 1}페이지 크롤링 완료: {title.get_text(strip=True)}")
        except Exception as e:
            print(f" - {i//10 + 1}페이지 크롤링 중 오류 발생: {title.get_text(strip=True)} - {str(e)}")

data = {'제목': titles, '기사 내용': contents, '타임스탬프': timestamps}
df = pd.DataFrame(data)
df.to_csv('BusNews.csv', mode='a', index=False)
