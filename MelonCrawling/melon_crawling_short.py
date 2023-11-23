from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from bs4 import BeautifulSoup
import pandas as pd
import time

def crawl_melon_chart(start_page, end_page):
    chrome_driver_path = r'C:/Users/yeryu/Downloads/chromedriver-win64/chromedriver.exe'  #나의 local directory로 되어있음
    chrome_service = webdriver.chrome.service.Service(chrome_driver_path)
    driver = webdriver.Chrome(service=chrome_service)

    all_titles = []
    all_singers = []
    all_song_ids = []
    
    for page_num in range(start_page, end_page + 1):
        # 각 페이지의 시작 인덱스 계산
        start_index = (page_num - 1) * 50 + 1

        # 최신 음악 페이지의 URL로 이동
        driver.get(f'https://www.melon.com/new/index.htm#params%5BareaFlg%5D=I&po=pageObj&startIndex={start_index}')

        try:
            # 페이지가 로딩될 때까지 대기
            WebDriverWait(driver, 20).until(lambda driver: len(driver.find_elements(By.CLASS_NAME, 'ellipsis.rank01')) > 0)
        except TimeoutException:
            print(f'TimeoutException occurred while waiting for page {page_num}')

        while True:
            try:
                # 새로운 페이지의 데이터가 충분히 로딩되도록 대기 (2초 대기)
                time.sleep(2)
                
                # 페이지 소스를 가져오기
                html = driver.page_source
                parse = BeautifulSoup(html, 'html.parser')

                # 클래스명이나 태그 등을 최신 음악 페이지에 맞게 수정
                titles = parse.find_all("div", {"class": "ellipsis rank01"})
                singers = parse.find_all("div", {"class": "ellipsis rank02"})
                song_ids = parse.select("button.like[data-song-no]")

                title_list = [t.find('a').text for t in titles]
                singer_list = [s.find('span', {"class": "checkEllipsis"}).text for s in singers]
                song_id_list = [int(button['data-song-no']) for button in song_ids]

                # 현재 페이지의 데이터를 전체 리스트에 추가
                all_titles.extend(title_list)
                all_singers.extend(singer_list)
                all_song_ids.extend(song_id_list)

                print(f'Finished crawling page {page_num}')

                # 현재 페이지 크롤링이 완료되면 반복문 탈출
                break
            except StaleElementReferenceException:
                # StaleElementReferenceException이 발생하면 다시 대기
                print("StaleElementReferenceException occurred. Retrying...")
                time.sleep(2)
                continue

    driver.quit()

    # 전체 리스트를 데이터프레임으로 변환
    data = {'Title': all_titles, 'Singer': all_singers, 'Song_ID': all_song_ids}
    df = pd.DataFrame(data)

    # CSV 파일로 저장
    df.to_csv('melon_crawling_id_6month.csv', index=False)
    
if __name__ == "__main__":
    start_page = 1
    end_page = 82  # 82페이지: 2023년 6월 1일까지, 180페이지: 2022년 11월 1일까지
    crawl_melon_chart(start_page, end_page)
