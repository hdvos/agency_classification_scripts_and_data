from bs4 import BeautifulSoup
from csv import DictWriter
from datetime import datetime
import json
import os
import re
import requests
from urllib.parse import urljoin
from multiprocessing.pool import ThreadPool

COUNTER = 0

output_folder = '/home/hugo/MEGA/work/Agency_Classification/crawling_files/CVPO_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)


# full_json = []

baseurl = "https://cpvo.europa.eu/en/news-and-events/news/"

def make_results_page_url(i:int) -> str:
    return f"https://cpvo.europa.eu/en/news-and-events/news?page={i}"

def make_result_selector(i:int) -> str:
    return f"li.views-row:nth-child({i}) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > a:nth-child(1)"

def check_status_code(response, url):
    if not response.status_code == 200:
        raise RuntimeError(f"Unable to retrieve page {url}.\nStatus Code: {response.status_code}")

def get_url(href:str) -> tuple:
    # href = results_elem_list[0]['href']
    url = urljoin(baseurl, href)
    return url

def make_ID(text_type) -> str:
    """Generate a unique ID  for the press release or news item. NOTE: this id is unique during one run, but might not be persisten when the script is rerun as the script works back in time.
    
    :param text_type: the text type (NEWS, PR, NP)
    :type text_type: [type]
    :return: identifier for the text.
    :rtype: str
    """
    global COUNTER
    COUNTER += 1
    return f"CVPO_{text_type}_{COUNTER:04}"

def process_text(text:str) -> str:
    text = ' '.join(text.splitlines())
    text = re.sub(r"{[^}]+}", " ", text)
    re.sub(r"<[^>]+>", ' ', text)
    text = text.replace("}(document, 'script', 'twitter-wjs');", " ")
    text = text.replace("lang: en_US Tweet !function(d,s,id)", ' ')
    
    text = re.sub(r' +', ' ', text)
    return text

def process_page(url):
    response = requests.get(url)

    check_status_code(response, url)

    soup = BeautifulSoup(response.text, features='lxml')

    title = soup.select("#page-title")[0].text
    print(f'\t- {title}')

    date = soup.select('.date-display-single')[0].text.strip()
    date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')
    print(f'\t\t{date}')
    try:
        text = soup.select('.body')[0].text.strip()
        text = process_text(text)
    except:
        text = "NULL"
    # print(f'\t\t{text}')

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

    # full_json.append(pagedata)
    return pagedata

def process_result_soup(result_soup):
    href = result_soup.select_one('a.group-link-wrapper')['href']
    # input(href)
    url = get_url(href)
    print(f"\t\t- {url}")

    page_data = process_page(url)
    return page_data

full_json = []

go_on = True
i = 0
while go_on:
    print(f"Processing page {i + 1}")
    r_p_url = make_results_page_url(i)
    i += 1

    r_p_response = requests.get(r_p_url)
    check_status_code(r_p_response, r_p_url)
    
    r_p_soup = BeautifulSoup(r_p_response.text, features='lxml')

    results_soups = r_p_soup.select(".views-row")
    if len(results_soups) == 0:
        go_on = False
        break
    # input(len(results_soups))

    # for result_soup in results_soups:
    #     page_data = process_result_soup(result_soup)

    #     full_json.append(page_data)

    with ThreadPool(12) as mypool:
        pagedata_lst = mypool.map(process_result_soup, results_soups)
        full_json += pagedata_lst

    print('nr results: ', len(full_json))

    
    # input()

    # for r_p_nr in range(1,13):
    #     selector = make_result_selector(r_p_nr)
    #     result_elem_list = r_p_soup.select(selector)
    #     if not result_elem_list:
    #         go_on = False
    #         break

    #     url = get_url(result_elem_list)
    #     # print(f"\t\t- {url}")

    #     process_page(url, full_json)

    #     # input()

with open(os.path.join(output_folder, 'CVPO.json'), 'wt') as f:
    json.dump(full_json, f)

# write to csv
with open(os.path.join(output_folder,'CVPO.csv'), 'wt') as csvfile:
    fieldnames = ['id', 'date', 'text_type', 'title', 'text', 'url']
    writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

    writer.writeheader()
    for item in full_json:
        writer.writerow(item)