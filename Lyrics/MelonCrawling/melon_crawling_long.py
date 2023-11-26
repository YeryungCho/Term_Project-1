from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

def crawl_song_details(input_csv, output_csv, fail_csv):
    # 기존 CSV 파일 로드
    melon_chart_df = pd.read_csv(input_csv, encoding='utf-8')
    
    # 추가 정보를 저장할 리스트 초기화
    release_dates = []
    genres = []
    like_counts = []
    lyrics_list = []
    fail_to_crawl = []

    # 웹드라이버 설정
    chrome_driver_path = r'C:/Users/yeryu/Downloads/chromedriver-win64/chromedriver.exe'
    chrome_service = webdriver.chrome.service.Service(chrome_driver_path)
    driver = webdriver.Chrome(service=chrome_service)

    try:
        for song_id in melon_chart_df['Song_ID']:
            # 각 노래의 상세 페이지 URL 생성
            song_detail_url = f'https://www.melon.com/song/detail.htm?songId={song_id}'

            # 노래 상세 페이지 방문
            driver.get(song_detail_url)

            try:
                # 페이지 로딩을 기다림
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, 'd_like_count')))

                # 필요한 정보 크롤링
                release_date = driver.find_element(By.XPATH, '//*[@id="downloadfrm"]/div/div/div[2]/div[2]/dl/dd[2]').text
                genre = driver.find_element(By.XPATH, '//*[@id="downloadfrm"]/div/div/div[2]/div[2]/dl/dd[3]').text
                like_count = driver.find_element(By.XPATH, '//*[@id="d_like_count"]').text

                # 가사를 크롤링하기 전에 가사 섹션을 확장
                lyrics_section = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="d_video_summary"]'))
                )
                driver.execute_script("arguments[0].style.height='auto';", lyrics_section)
                time.sleep(2)  # 가사 섹션이 확장되기를 기다림
                lyrics = driver.find_element(By.XPATH, '//*[@id="d_video_summary"]').text

                # 크롤링한 정보를 리스트에 추가
                release_dates.append(release_date)
                genres.append(genre)
                like_counts.append(like_count)
                lyrics_list.append(lyrics)

                # 디버깅 정보 출력
                print(f'{song_id} 번 노래 크롤링 완료')
            except StaleElementReferenceException:
                # StaleElementReferenceException이 발생하면 다시 시도
                continue
            except (TimeoutException, NoSuchElementException) as e:
                # TimeoutException 또는 NoSuchElementException이 발생하면 해당 곡의 크롤링 실패로 간주하고 에러 메시지 출력
                fail_to_crawl.append(song_id)
                print(f'Error: {e}. {song_id} 번 노래 크롤링 실패')
    except KeyboardInterrupt:
        # Ctrl+C로 인터럽트가 발생하면 현재까지 얻은 정보를 저장하고 종료
        print("크롤링이 중단되었습니다. 현재까지 얻은 정보를 저장합니다.")

    finally:
        # 새 정보를 기존 데이터프레임에 추가
        melon_chart_df['Release_Date'] = release_dates + ['N/A'] * (len(melon_chart_df) - len(release_dates))
        melon_chart_df['Genre'] = genres + ['N/A'] * (len(melon_chart_df) - len(genres))
        melon_chart_df['Like_Count'] = like_counts + ['N/A'] * (len(melon_chart_df) - len(like_counts))
        melon_chart_df['Lyrics'] = lyrics_list + ['N/A'] * (len(melon_chart_df) - len(lyrics_list))

        # 실패한 곡들을 fail_csv에 저장
        fail_df = pd.DataFrame({'Song_ID': fail_to_crawl})
        fail_df.to_csv(fail_csv, index=False)

        # 최종 데이터프레임을 새 CSV 파일로 저장
        melon_chart_df.to_csv(output_csv, index=False)

        # 웹드라이버 종료
        driver.quit()

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # 입력 CSV 파일의 경로
    input_csv = "C:\\Users\\yeryu\\Desktop\\빅데프 기말 프로젝트 최종\\Term_Project\\MelonCrawling\\melon_crawling_id_6month.csv"
    output_csv = 'melon_crawling_details_all_finish.csv'
    fail_csv = 'fail_to_crawl_all_finish.csv'
    crawl_song_details(input_csv, output_csv, fail_csv) 