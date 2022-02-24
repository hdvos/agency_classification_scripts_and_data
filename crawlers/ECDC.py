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
AGENCY = "ECDC"

baseurl = "https://www.ecdc.europa.eu/en/news-events/"

full_json = []


output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def make_results_page_url(i:int) -> str:
    return f"https://www.ecdc.europa.eu/en/news-events?s=&sort_by=field_ct_publication_date&sort_order=DESC&f[0]=output_types%3A1307&page={i}"

def check_status_code(response, url):
    pass
    # if not response.status_code == 200:
    #     raise RuntimeError(f"Unable to retrieve page {url}.\nStatus Code: {response.status_code}")

def make_result_selector(i:int) -> str:
    return f"div.views-row:nth-child({i}) > article:nth-child(1) > a:nth-child(1)"


def get_url(href:str) -> str:
    url = urljoin(baseurl, href)
    return url

def make_ID(text_type, agency_abbr) -> str:
    """Generate a unique ID  for the press release or news item. NOTE: this id is unique during one run, but might not be persisten when the script is rerun as the script works back in time.
    
    :param text_type: the text type (NEWS, PR, NP)
    :type text_type: [type]
    :return: identifier for the text.
    :rtype: str
    """
    global COUNTER
    COUNTER += 1
    return f"{agency_abbr}_{text_type}_{COUNTER:04}"

def process_text(text:str) -> str:
    text = ' '.join(text.splitlines())
    text = re.sub(r"{[^}]+}", " ", text)
    re.sub(r"<[^>]+>", ' ', text)
    text = text.replace("}(document, 'script', 'twitter-wjs');", " ")
    text = text.replace("lang: en_US Tweet !function(d,s,id)", ' ')
    
    text = re.sub(r' +', ' ', text)
    return text

def process_page(url):
    try:
        response = requests.get(url)
    except RuntimeError:
        return     {
            'id':0,
            'date':'NULL',
            'text_type':'NULL',
            'title':'NULL',
            'text':'NULL',
            'url':url,
        }


    check_status_code(response, url)

    soup = BeautifulSoup(response.text, features='lxml')

    try:
        title = soup.select(".h1")[0].text.strip()
    except:
        title = "NULL"

    print(f'\t- {title}')
    try:
        date = soup.select('.ct__meta__value > time:nth-child(1)')[0].text.strip()
        date = datetime.strptime(date, '%d %b %Y').strftime('%Y-%m-%d')
    except:
        date = "NULL"
    print(f'\t\t{date}')

    try:
        text = soup.select('.col-md-9')[0].text.strip()
        text = process_text(text)
    except:
        text = "NULL"

    # print(f'\t\t{text}')
    try:
        text_type_block = soup.select(".ct__meta__block")[0].text
        if "press release" in text_type_block.lower():
            text_type = "PR"
        elif "news" in text_type_block.lower():
            text_type = "NEWS"
        else:
            text_type = "OTH"
    except:
        text_type = "NULL"

    text_id = make_ID(text_type, AGENCY)

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

def process_result(result_soup):
    href = result_soup.select_one('a.ct__link')['href']
    url = get_url(href)
    pagedata = process_page(url)

    return pagedata

try:
    i = 0
    go_on = True
    while go_on:
        print(f"Processing page {i+1}")
        r_p_url = make_results_page_url(i)
        print(r_p_url)
        i += 1

        r_p_response = requests.get(r_p_url)

        # if i == 10:
        #     break

        r_p_soup = BeautifulSoup(r_p_response.text, features='lxml')

        results_soups = r_p_soup.select('.views-row')
        print(len(results_soups))
        # input()

        if len(results_soups) == 0:
            go_on = False
            break

        # for result_soup in results_soups:
        #     pagedata = process_result(result_soup)
        #     full_json.append(pagedata)
        with ThreadPool(len(results_soups)) as mypool:
            full_json += mypool.map(process_result, results_soups)

        print(len(full_json))
        # for r_p_nr in range(1,11):
        #     selector = make_result_selector(r_p_nr)
        #     result_elem_list = r_p_soup.select(selector)
        #     if not result_elem_list:
        #         go_on = False
        #         break

        #     url = get_url(result_elem_list)
        #     # print(f"\t\t- {url}")

        #     process_page(url, full_json)
finally:
    with open(os.path.join(output_folder, 'ECDC.json'), 'wt') as f:
        json.dump(full_json, f)

    # # write to csv
    with open(os.path.join(output_folder,'ECDC.csv'), 'wt') as csvfile:
        fieldnames = ['id', 'date', 'text_type', 'title', 'text', 'url']
        writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

        writer.writeheader()
        for item in full_json:
            writer.writerow(item)