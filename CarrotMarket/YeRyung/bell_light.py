#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd

bell = pd.read_csv("C:/Users/yeryu/Desktop/빅데프 기말 프로젝트 최종/emergency_bell.csv")
light = pd.read_csv("C:/Users/yeryu/Desktop/빅데프 기말 프로젝트 최종/security_light.csv")

bell_df = pd.DataFrame(bell)

# 필요한 열만 추출하여 새로운 데이터프레임 생성
new_bell_df = pd.DataFrame({
    'category': ['emergency_bell'] * len(bell_df),
    'location': bell_df.iloc[:, 1],
    'lat': bell_df.iloc[:, 7],
    'lng': bell_df.iloc[:, 8],
    'img': [''] * len(bell_df),
})

new_bell_df.head()
new_bell_df.to_csv(r"C:\Users\yeryu\Desktop\빅데프 최최종\Term_Project\CarrotMarket\YeRyung\emergency_bell.csv", index=False, encoding='utf-8')

light_df = pd.DataFrame(light)

# 필요한 열만 추출하여 새로운 데이터프레임 생성
new_light_df = pd.DataFrame({
    'category': ['security_light'] * len(light_df),
    'location': light_df.iloc[:, 1],
    'lat': light_df.iloc[:, 5],
    'lng': light_df.iloc[:, 6],
    'img': [''] * len(light_df),
})

new_light_df.head()
new_light_df.to_csv(r"C:\Users\yeryu\Desktop\빅데프 최최종\Term_Project\CarrotMarket\YeRyung\security_light.csv", index=False, encoding='utf-8')

