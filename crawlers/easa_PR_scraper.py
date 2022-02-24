from bs4 import BeautifulSoup
from csv import DictWriter
from datetime import datetime
import json
import os, sys
import re
import requests
import time
from urllib import parse
full_json = []
COUNTER = 0


output_folder = '/home/hugo/MEGA/work/Agency_Classification/crawling_files/EASA_DOCUMENTS'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

monthdict = {
    "JAN" : 1,
    "FEB" : 2,
    "MAR" : 3,
    "APR" : 4,
    "MAY" : 5,
    "JUN" : 6,
    "JUL" : 7,
    "AUG" : 8,
    "SEP" : 9,
    "OCT" : 10,
    "NOV" : 11,
    "DEC" : 12
}

article_selectors = []
for i in range(1,11):
    article_selectors.append(f'div.views-row:nth-child({i}) > div:nth-child(2) > span:nth-child(1) > span:nth-child(1) > a:nth-child(1)')

print(article_selectors)



def make_url(i):
    return f'https://www.easa.europa.eu/newsroom-and-events/press-releases?page={i}'

def process_easa_date(soup_element):
    datelist = soup_element[0].text.splitlines()
    
    day = int(datelist[2])
    month = monthdict[datelist[5].strip()]
    year = int(datelist[8])
    
    date = datetime(day=day, month=month, year = year)
    date = date.strftime('%Y-%m-%d')

    return date
    

def process_easa_text(text):
    text = re.sub(r'\n', ' ', text)

    text = re.sub(r' +', ' ', text)
    return text

def make_newspage_url(ending):
    return parse.urljoin('https://www.easa.europa.eu/newsroom-and-events/press-releases/', ending)

def process_newspage(html, all_news_json, url):
    print("\t\t+ Process newspage")
    global COUNTER
    COUNTER += 1
    soup = BeautifulSoup(html, features='lxml')

    try:
        title = soup.select('#page-title')[0].text
    except:
        title = 'NULL'
    print(f"\t\t\t- {title}")

    try:
        subject = soup.select('div.field-items:nth-child(2) > div:nth-child(1) > a:nth-child(1)')[0].text
    except:
        subject = "NULL"
    print(f"\t\t\t- {subject}")

    date = soup.select('.easa-displayed-date')
    date = process_easa_date(date)
    print(f'\t\t\t- {date}')

    try:
        text = soup.select('.body > div:nth-child(1)')[0].text
        text = process_easa_text(text)
    except:
        text = "NULL"
    
    item = {
        'id':f"EASA_PR_{COUNTER:04}",
        'date':date,
        'title':title,
        'text':text,
        'easa_category': subject,
        'url':url
    }
    all_news_json.append(item)

def process_article_page(html, all_news_json):
    print("\t+ Process article page")

    soup = BeautifulSoup(html, features='lxml')
    for selector in article_selectors:
        try:
            page = soup.select(selector)[0]
        except IndexError:
            return False

        href = page['href']
        url = make_newspage_url(href)
        print(f"\tDownload: {url}")
        newspage_response = requests.get(url)
        if response.status_code == 200:
            print(f"\t\t- Page Downloaded")
            newspage_html = newspage_response.text
            process_newspage(newspage_html, all_news_json, url)

    return True



i = 0
go_on = True
while go_on:
    url = make_url(i)
    print(f'Download: {url}')
    response = requests.get(url)
    if response.status_code == 200:
        print('\t- Page downloaded')
        html = response.text
        go_on = process_article_page(html, full_json)

    else:
        print(response.status_code)
        break

    i += 1


with open(os.path.join(output_folder, 'EASA_PR.json'), 'wt') as f:
    json.dump(full_json, f)

# write to csv
with open(os.path.join(output_folder,'EASA_PR.csv'), 'wt') as csvfile:
    fieldnames = ['id', 'date', 'easa_category', 'title', 'text', 'url']
    writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

    writer.writeheader()
    for item in full_json:
        writer.writerow(item)

print(f"Done. Downloaded {len(full_json)} articles and stored them in {output_folder}")