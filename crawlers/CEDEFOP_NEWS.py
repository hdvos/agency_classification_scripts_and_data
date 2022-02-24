from bs4 import BeautifulSoup
from csv import DictWriter
from datetime import datetime
import json
import os
import re
import requests
from urllib.parse import urljoin
from multiprocessing.pool import ThreadPool

base_url = "https://www.cedefop.europa.eu/en/news-and-press/news/"

COUNTER = 0



def make_results_page_url(i:int) -> str:
    return f"https://www.cedefop.europa.eu/en/news-and-press/news?page={i}"

# def make_result_selector(i:int) -> str:
#     return f"div.views-row:nth-child({i}) > div:nth-child(3) > span:nth-child(1) > a:nth-child(1)"

def make_result_url(url_ending:list) -> str:
    url = urljoin(base_url, url_ending)

    return url

def check_status_code(response, url):
    if not response.status_code == 200:
        raise RuntimeError(f"Unable to retrieve page {result_page_url}.\nStatus Code: {rp_response.status_code}")

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
    return f"CEDEFOP_{text_type}_{COUNTER:04}"

def process_page(url:str, full_json:list):
    response = requests.get(url)
    check_status_code(response, url)

    soup = BeautifulSoup(response.text, features='lxml')

    title = soup.select("#page-title")[0].text
    print(f"\t- {title}")

    date = soup.select('.date-display-single')[0].text
    date = datetime.strptime(date, "%d/%m/%Y").strftime('%Y-%m-%d')
    print(f"\t\t- {date}")

    text = soup.select('.group-ced-main')[0].text
    text = process_text(text)
    # print(f"\t- {text}")

    text_type = "NEWS"

    text_id = make_ID(text_type)

    pagedata = {
            'id':text_id,
            'date':date,
            'text_type':text_type,
            'title':title,
            'text':text,
            'url':url,
        }

    return pagedata
    # full_json.append(pagedata)

def process_result(result_soup):
    result_href = result_soup.select_one('.views-field-title > span > a')['href']
    result_url = make_result_url(result_href)
    # input(result_url)

    
        
    page_data = process_page(result_url, full_json)
    return page_data

full_json = []

go_on = True
i = 0
while go_on:
    print(f"Processing result page {i + 1}")
    i += 1
    result_page_url = make_results_page_url(i)
    rp_response = requests.get(result_page_url)

    check_status_code(rp_response, result_page_url)
    
    rp_soup = BeautifulSoup(rp_response.text, features='lxml')
    results_soups = rp_soup.select(".views-row")


    # for result_soup in results_soups:
    #     result_data = process_result(result_soup)
    #     full_json.append(result_data)
    #     print(len(full_json))

    with ThreadPool(10) as mypool:
        results_data = mypool.map(process_result, results_soups)
        full_json += results_data
    print("len json = ", len(full_json))
    print("+"*30, '\n')
    # input()
    if len(results_soups) == 0:
        go_on = False
        break
    # for result_nr in range(1,21):
    #     result_selector = make_result_selector(result_nr)
    #     result_elem_lst = rp_soup.select(result_selector)
    #     if not result_elem_lst:
    #         go_on = False
    #         break
        


        # input()


output_folder = '/home/hugo/MEGA/work/Agency_Classification/crawling_files/CEDEFOP_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

with open(os.path.join(output_folder, 'CEDEFOP_NEWS.json'), 'wt') as f:
    json.dump(full_json, f)

# write to csv
with open(os.path.join(output_folder,'CEDEFOP_NEWS.csv'), 'wt') as csvfile:
    fieldnames = ['id' , 'date', 'text_type', 'title', 'text', 'url']
    writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

    writer.writeheader()
    for item in full_json:
        writer.writerow(item)

