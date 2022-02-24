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
output_folder = '/home/hugo/MEGA/work/Agency_Classification/crawling_files/ACER_DOCUMENTS'     # The place where the output files will go
base_url = 'https://www.acer.europa.eu/Media/News/Pages/News.aspx'      # the base url for the news

fulljson = []

driver = webdriver.Firefox()
driver.get(base_url)
print('wait for page to load')
time.sleep(2)

print('accept cookies')
try:
    # cookiebutton_xpath = '/html/body/form/div[13]/div[1]/div[4]/div/div[2]/button[2]'
    # cookiebutton = driver.find_element_by_xpath(cookiebutton_xpath)
    cookiebutton = driver.find_elements_by_class_name('CookieMessageClosebutton')[1]
    print(cookiebutton)
    cookiebutton.click()
    print("cookies accepted")

except Exception as e:
    print(e)
    input()

# xpaths = []
# for i in range(1,6):
#     xpaths.append(f'/html/body/form/div[13]/div[1]/div[2]/div[4]/div[3]/div[2]/div[3]/div[1]/div/div/div/div[1]/div[3]/div/div/div[{i}]/h2/a')
# print( '\n'.join(xpaths))

where = 1
go_on = True
while go_on:
    time.sleep(5)
    elems = driver.find_elements_by_class_name("related_new_button")
    print(len(elems))
    # input()

    for elem in elems:
        html = elem.get_attribute('innerHTML')
        soup = BeautifulSoup(html, features="lxml")
        a_element = soup.select_one('a')
        url = a_element['href']
        # url = soup['href']
        
        response = requests.get(url)
        if response.status_code == 200:
            COUNTER += 1
            html = response.text
            soup = BeautifulSoup(html, features='lxml')

            try:
                title = soup.select_one("h1").text
                # print(title)
            except:
                title = "NULL"
            print(f'\t- {title}')
            # input()
            
            try:
                date = soup.select('.page_date')[0].text
            except:
                pass
                # go_on = False
                # break
            # print(f'\t\t- {date}')
            # input()
            try:
                date=date.strip()
                # print(date)
                day, month, year = date.split('.')
                # print(day, month, year)
                date = datetime(day=int(day), month=int(month), year=int(year))
                date = date.strftime('%Y-%m-%d')
            except:
                date = "NULL"

            print(f'\t\t- {date}')

            try:
                text = soup.select_one('div.section_body').text
                text = ' '.join(text.splitlines())
                # text = re.sub(r'\n', ' ', text)
                text = text.replace('Page Content', ' ')
                text = re.sub(r'{[^}]+}', ' ', text)
                text = re.sub(r' +', ' ', text)
                
            except:
                text = "NULL"
            # print(text)

            pagedata = {
                'id':f"ACER_NEWS_{COUNTER:04}",
                'date':date,
                'url':url,
                'title':title,
                'text':text,
            }
            fulljson.append(pagedata)
            # print('-------------------------------------------------------------------------')

        else:
            pass

        ...
    ...

    try:
        nextbutton_xpath = "//a[@title='Move to next page']"
        nextbutton = driver.find_element_by_xpath(nextbutton_xpath)
        print(nextbutton.get_attribute('innerHTML'))
        nextbutton.click()
    except:
        break
    # input()

driver.close()
# sys.exit()

with open(os.path.join(output_folder, 'ACER_NEWS.json'), 'wt') as f:
    json.dump(fulljson, f)

# write to csv
with open(os.path.join(output_folder,'ACER_NEWS.csv'), 'wt') as csvfile:
    fieldnames = ['id', 'date','title', 'text', 'url']
    writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

    writer.writeheader()
    for item in fulljson:
        writer.writerow(item)