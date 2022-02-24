from selenium import webdriver
import time
from bs4 import BeautifulSoup

# https://euipo.europa.eu/ohimportal/en/news 


i = 0
go_on = True

with webdriver.Firefox() as driver:
    driver.get("https://euipo.europa.eu/ohimportal/en/news")
    time.sleep(5)

    while go_on:
        # print(driver)
        r_p_html = driver.page_source
        # print(x)
        r_p_soup = BeautifulSoup(r_p_html, features='lxml')