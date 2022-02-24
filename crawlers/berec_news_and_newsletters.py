from bs4 import BeautifulSoup
from csv import DictWriter
from datetime import datetime
import json
import os
import re
import requests
from urllib.parse import urljoin

baseurl = "https://berec.europa.eu/eng/news_and_publications/whats_new/"

COUNTER = 0

full_json = []

output_folder = '/home/hugo/MEGA/work/Agency_Classification/crawling_files/BEREC_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)


def make_results_page_url(i:int) -> str:
    return f"https://berec.europa.eu/eng/news_and_publications/whats_new/?qPage={i}"

def make_result_selector(i:int) -> str:
    return f"li.item:nth-child({i}) > h2:nth-child(1) > a:nth-child(1)"

def get_url(results_elem_list:list) -> tuple:
    href = result_elem_lst[0]['href']
    internal_id = int(href.split('-')[0].strip().split('/')[-1].strip())
    url = urljoin(baseurl, href)
    return url, internal_id

def process_text(text:str) -> str:
    text = ' '.join(text.splitlines())
    text = re.sub(r"{[^}]+}", " ", text)
    re.sub(r"<[^>]+>", ' ', text)
    text = text.replace("}(document, 'script', 'twitter-wjs');", " ")
    text = text.replace("lang: en_US Tweet !function(d,s,id)", ' ')
    
    text = re.sub(r' +', ' ', text)
    return text

def make_ID(text_type) -> str:
    """Generate a unique ID  for the press release or news item. NOTE: this id is unique during one run, but might not be persisten when the script is rerun as the script works back in time.
    
    :param text_type: the text type (NEWS, PR, NP)
    :type text_type: [type]
    :return: identifier for the text.
    :rtype: str
    """
    global COUNTER
    COUNTER += 1
    return f"BEREC_{text_type}_{COUNTER:04}"

def process_page(url, internal_id, full_json):
    response = requests.get(url, verify=False)

    if not response.status_code == 200:
        raise RuntimeError(f"Error while retrieving page {url}.\nError code: {response.status_code}")

    soup = BeautifulSoup(response.text, features='lxml')

    title = soup.select(".article > h1:nth-child(1)")[0].text
    print(f'\t{title}')

    date = soup.select('.date-style')[0].text.strip()
    date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')
    print(f'\t{date}')

    text = soup.select('.article')[0].text.strip()
    text = process_text(text)

    text_type = "UNK"

    text_id = make_ID(text_type)

    pagedata = {
            'id':text_id,
            'internal_berec_id': internal_id,
            'date':date,
            'text_type':text_type,
            'title':title,
            'text':text,
            'url':url,
        }

    full_json.append(pagedata)


i = 1
go_on = True
# while go_on:
while go_on:
    print(f"Process page {i}")
    results_page_url = make_results_page_url(i)
    # input(results_page_url)
    i+=1

    response = requests.get(results_page_url, verify=False)
    if not response.status_code == 200:
        raise RuntimeError(f"Error while retrieving results page {results_page_url}.\nError code: {response.status_code}")

    results_page_soup = BeautifulSoup(response.text, features='lxml')

    for result_nr in range(1,11):
        selector = make_result_selector(result_nr)
        result_elem_lst = results_page_soup.select(selector)
        if not result_elem_lst:
            go_on = False
            break
        
        url, internal_id = get_url(result_elem_lst)
 
        process_page(url, internal_id, full_json)


with open(os.path.join(output_folder, 'BEREC.json'), 'wt') as f:
    json.dump(full_json, f)

# write to csv
with open(os.path.join(output_folder,'BEREC.csv'), 'wt') as csvfile:
    fieldnames = ['id',"internal_berec_id" , 'date', 'text_type', 'title', 'text', 'url']
    writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

    writer.writeheader()
    for item in full_json:
        writer.writerow(item)
