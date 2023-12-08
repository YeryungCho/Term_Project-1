# '경찰서 안전' 크롤링
import requests
from bs4 import BeautifulSoup
from newspaper import Article, ArticleException
import pandas as pd
from datetime import datetime

titles = []
contents = []
timestamps = []

for i in range(1, 1000000,10):
    link = 'https://search.naver.com/search.naver?where=news&sm=tab_pge&query=%EA%B2%BD%EC%B0%B0%EC%84%9C%20%EC%95%88%EC%A0%84&sort=1&photo=0&field=0&pd=2&ds=2023.11.08&de=2023.12.08&mynews=0&office_type=0&office_section_code=0&news_office_checked=&office_category=0&service_area=0&nso=so:dd,p:1m,a:all&start=' + str(i)
    response = requests.get(link)
    soup = BeautifulSoup(response.text, 'html.parser')
    original_url_elements = soup.select('#ct > div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > a')
    original_url = original_url_elements[0]['href'] if original_url_elements else None
    
    # Extract news titles and links
    news_titles = soup.find_all('a', class_='news_tit')

    # If less than 10 news titles, stop crawling
    if len(news_titles) < 10:
        print(f" - {i//10 + 1}페이지에서 기사 수가 10개 미만이므로 크롤링 중단")
        break
    
    for title in news_titles:
        news_url = title['href']
        
        if original_url:
            news_url = original_url

        try:
            # Attempt to download and parse the article
            article = Article(news_url, language='ko', fetch_images=False, request_timeout=20)
            article.download()
            article.parse()

            # Append data to lists
            titles.append(title.get_text(strip=True))
            contents.append(article.text)
            timestamps.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            print(f" - {i//10 + 1}페이지 크롤링 완료: {title.get_text(strip=True)}")

        except ArticleException as e:
            # Handle the exception based on the status code
            print(f" - {i//10 + 1}페이지: {title.get_text(strip=True)} - ArticleException (skipping)")


# Create a DataFrame
data = {'제목': titles, '기사 내용': contents, '타임스탬프': timestamps}
df = pd.DataFrame(data)

# Save the DataFrame to a CSV file
df.to_csv('PoliceNews.csv',mode='a', index=False)