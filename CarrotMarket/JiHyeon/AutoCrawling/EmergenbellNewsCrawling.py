# '비상벨 안전' 크롤링 1달

import requests
from bs4 import BeautifulSoup
from newspaper import Article
import pandas as pd

titles = []
contents = []

for i in range(1, 100000,10):
    link = "https://search.naver.com/search.naver?where=news&sm=tab_pge&query=%EB%B9%84%EC%83%81%EB%B2%A8%20%EC%95%88%EC%A0%84&sort=1&photo=0&field=0&pd=2&ds=2023.11.03&de=2023.12.03&mynews=0&office_type=0&office_section_code=0&news_office_checked=&office_category=0&service_area=0&nso=so:dd,p:1m,a:all&start=" + str(i)
    response = requests.get(link)
    soup = BeautifulSoup(response.text, 'html.parser')
    url = soup.select('#ct > div.media_end_head.go_trans > div.media_end_head_info.nv_notrans > div.media_end_head_info_datestamp > a')
    
    # Extract news titles and links
    news_titles = soup.find_all('a', class_='news_tit')

    # If less than 10 news titles, stop crawling
    if len(news_titles) < 10:
        print(f" - {i//10 + 1}페이지에서 기사 수가 10개 미만이므로 크롤링 중단")
        break
    
    for title in news_titles:
        news_url = title['href']

        # Extract article content using newspaper library
        article = Article(news_url, language='ko', fetch_images=False, request_timeout=20)
        article.download()
        article.parse()

        # Append data to lists
        titles.append(title.get_text(strip=True))
        contents.append(article.text)
        
        print(f" - {i//10 + 1}페이지 크롤링 완료: {title.get_text(strip=True)}")

# Create a DataFrame
data = {'제목': titles, '기사 내용': contents}
df = pd.DataFrame(data)

# Save the DataFrame to a CSV file
df.to_csv('emergencybellNews.csv', index=False)