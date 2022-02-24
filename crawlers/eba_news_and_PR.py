from bs4 import BeautifulSoup
from csv import DictWriter
from datetime import datetime
import json
import os, sys
import re
import requests
from selenium import webdriver
import time
from tqdm import tqdm

COUNTER = 0
full_json = []

base_url = 'https://eba.europa.eu/all-news-and-press-releases'

output_folder = '../crawling_files/EBA/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

driver = webdriver.Firefox()
driver.get(base_url)
print('wait for page to load')
time.sleep(2)

print('accept cookies')
try:
    cookiebutton_xpath = '/html/body/div[1]/div[1]/a'
    cookiebutton = driver.find_element_by_xpath(cookiebutton_xpath)
    cookiebutton.click()
except:
    pass


xpaths = []
for i in range(1,11):
    xpaths.append(f'/html/body/div[3]/div[3]/div/section/div/div[2]/div/div[3]/div/div/div[2]/div[{i}]/div/span/a')
    if i == 1:
        assert xpaths[0] == '/html/body/div[3]/div[3]/div/section/div/div[2]/div/div[3]/div/div/div[2]/div[1]/div/span/a'
# print(xpaths)


def get_text_type(elem):
    elemtext = str(elem.get_attribute('innerHTML'))
    if "Press Releases" in elemtext:
        text_type = "PR"
    elif "News" in elemtext:
        text_type = "NEWS"
    else:
        text_type = "NULL"

    return text_type

def process_date(datestr):
    date = datetime.strptime(datestr, "%d %B %Y")
    date = date.strftime('%Y-%m-%d')
    return date

def make_ID(text_type):
    global COUNTER
    COUNTER += 1
    return f"EBA_{text_type}_{COUNTER:04}"

def process_page(url, full_json, text_type):
    response = requests.get(url)
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, features='lxml')

        try:
            title = soup.select('.pane-title')[0].text.strip()
        except:
            title = "NULL"
        print(f"\t- {title}")
        
        try:
            date = soup.select('.SecondaryInfo')[0].text
            date = process_date(date)
        except:
            date = "NULL"
        print(f"\t\t- {date}")

        try:
            text = soup.select('.FreetextArea')[0].text
            text = text.replace('\n', ' ')
            text = re.sub(' +', ' ', text)
        except:
            text = "NULL"

        text_id = make_ID(text_type)

        pagedata = {
                'id':text_id,
                'date':date,
                'text_type':text_type,
                'title':title,
                'text':text,
                'url':url,
            }

        full_json.append(pagedata)

        print(f"\t\t- {text_type}")
    else:
        raise RuntimeError("Bad request")

    pass

def mysleep(seconds):
    for i in tqdm(range(seconds), total=seconds):
        time.sleep(1)

if __name__ == "__main__":
        
    go_on = True
    page_counter = 1
    i = 0
    while go_on:
        print('sleep')
        if i%5 == 0:
            mysleep(5)
        else:
            mysleep(2)
        print(f"Page {i + 1}: {page_counter} - {page_counter + 9}")
        page_counter += 10
        for xpath in xpaths:
            try:
                elem = driver.find_element_by_xpath(xpath)
            except:
                go_on = False
                break

            href = elem.get_attribute('href')

            text_type = get_text_type(elem)
            process_page(href, full_json, text_type)

            
            # print(elemtext)
            # input()
        # print(full_json)
        # print(type(full_json))
        # input()

        # raise NotImplementedError("Click next button")
        if i == 0:
            nextbtn = driver.find_element_by_xpath('/html/body/div[3]/div[3]/div/section/div/div[2]/div/div[3]/div/div/div[3]/ul/li[11]/a')
            nextbtn.click()
        elif i < 5:
            nextbtn = driver.find_element_by_xpath('/html/body/div[3]/div[3]/div/section/div/div[2]/div/div[3]/div/div/div[3]/ul/li[13]/a')
            nextbtn.click()
        elif 5 <= i < 117:
            nextbtn = driver.find_element_by_xpath('/html/body/div[3]/div[3]/div/section/div/div[2]/div/div[3]/div/div/div[3]/ul/li[14]/a')
            nextbtn.click()
        elif i >= 117:
            nextbtn = driver.find_element_by_xpath('/html/body/div[4]/div[3]/div/section/div/div[2]/div/div[3]/div/div/div[3]/ul/li[13]/a')
            nextbtn.click()
        elif i == 120:
            input("no nextbutton found")
            go_on = False

        i += 1
        
        
        '/html/body/div[3]/div[3]/div/section/div/div[2]/div/div[3]/div/div/div[3]/ul/li[13]/a'

        # input()
    driver.close()

    with open(os.path.join(output_folder, 'EBA_NEWS_and_PR.json'), 'wt') as f:
        json.dump(full_json, f)

    # write to csv
    with open(os.path.join(output_folder,'EBA_NEWS_and_PR.csv'), 'wt') as csvfile:
        fieldnames = ['id', 'date', 'text_type', 'title', 'text', 'url']
        writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

        writer.writeheader()
        for item in full_json:
            writer.writerow(item)