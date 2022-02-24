#NOTE: this script is a quick hack. It only works if the total number of articles is not a multiple of 200. To solve this, you need to change the critirion on exiting the while go_on loop.
from bs4 import BeautifulSoup
from csv import DictWriter
from datetime import datetime
import json
import os
import re
import requests
from urllib.parse import urljoin

COUNTER = 0
AGENCY = "ECHA"

baseurl = "https://echa.europa.eu/"

full_json = []
# monthyears = set()

csv_file = f"{AGENCY}.csv"
json_file = f"{AGENCY}.json"

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def make_results_page_url(i:int) -> str:
    return f"https://echa.europa.eu/news-and-events/news-alerts/all-news?p_p_id=101_INSTANCE_yhAseXkvBI2u&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&p_p_col_id=column-2&p_p_col_pos=1&p_p_col_count=2&_101_INSTANCE_yhAseXkvBI2u_delta=200&_101_INSTANCE_yhAseXkvBI2u_keywords=&_101_INSTANCE_yhAseXkvBI2u_advancedSearch=false&_101_INSTANCE_yhAseXkvBI2u_andOperator=true&p_r_p_564233524_resetCur=false&_101_INSTANCE_yhAseXkvBI2u_cur={i}"

def check_status_code(response, url):
    if not response.status_code == 200:
        raise RuntimeError(f"Unable to retrieve page {url}.\nStatus Code: {response.status_code}")

def make_result_selector(i:int) -> str:
    return f"div.views-row:nth-child({i}) > article:nth-child(1) > a:nth-child(1)"


def make_url(results_elem) -> str:
    href = results_elem['href']
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

def process_page(url, date_elem, text_type, full_json):
    response = requests.get(url)

    check_status_code(response, url)

    soup = BeautifulSoup(response.text, features='lxml')

    try:
        title = soup.select(".article-title")[0].text.strip()
    except:
        title = "NULL"

    print(f'\t- {title}')
    try:
        date = date_elem.text.replace('-', '').strip()
        date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')
    except:
        date = "NULL"
    print(f'\t\t{date}')

    try:
        text = soup.select('.single-news-article')[0].text.strip()
        text = process_text(text)
    except:
        text = "NULL"

    print(f"\t\t{text_type}")
    # print(text)
    # # print(f'\t\t{text}')
    # try:
    #     text_type_block = soup.select(".ct__meta__block")[0].text
    #     if "press release" in text_type_block.lower():
    #         text_type = "PR"
    #     elif "news" in text_type_block.lower():
    #         text_type = "NEWS"
    #     else:
    #         text_type = "OTH"
    # except:
    #     text_type = "NULL"

    text_id = make_ID(text_type, AGENCY)
    print(f"\t\t{text_id}")

    pagedata = {
            'id':text_id,
            'date':date,
            'text_type':text_type,
            'title':title,
            'text':text,
            'url':url,
        }

    full_json.append(pagedata)


i = 1
go_on = True
while go_on:
    print(f"Processing page {i+1}")
    r_p_url = make_results_page_url(i)
    # print(r_p_url)
    i += 1
    # print(r_p_url)
    r_p_response = requests.get(r_p_url)
    check_status_code(r_p_response, r_p_url)
    if i == 10:
        break

    r_p_soup = BeautifulSoup(r_p_response.text, features='lxml')

    selector = ".box-link > div > div:nth-child(1) > a:nth-child(2)"
    selections = r_p_soup.select(selector)
    print(len(selections))
    
    date_elements = r_p_soup.select(".box-link > div > div:nth-child(1) > span:nth-child(1)")

    text_type_elements = r_p_soup.select(".box-link > div > div:nth-child(1) > span:nth-child(3)")

    assert len(selections) == len(date_elements) == len(text_type_elements)
    
    for elem, date_elem, tt_elem in zip(selections, date_elements, text_type_elements):
        url = make_url(elem)

        text_type = tt_elem.text
        if "press" in text_type.lower():
            text_type = "PR"
        elif "news" in text_type.lower():
            text_type = "NEWS"
        else:
            text_type = "UNK"

        process_page(url, date_elem, text_type, full_json)
        # print(url)
        # input()

    if len(selections) < 200:
        break
    # for r_p_nr in range(1,11):
        # selector = make_result_selector(r_p_nr)

        # for elem in selections:
        #     print(elem.text)
        #     input()

        # result_elem_list = r_p_soup.select(selector)
        # if not result_elem_list:
        #     go_on = False
        #     break

#         url = get_url(result_elem_list)
#         # print(f"\t\t- {url}")

#         process_page(url, full_json)

# write json
with open(os.path.join(output_folder, json_file), 'wt') as f:
    json.dump(full_json, f)

# write to csv
with open(os.path.join(output_folder,csv_file), 'wt') as csvfile:
    fieldnames = ['id', 'date', 'text_type', 'title', 'text', 'url']
    writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

    writer.writeheader()
    for item in full_json:
        writer.writerow(item)


#https://echa.europa.eu/news-and-events/news-alerts/all-news?p_p_id=101_INSTANCE_yhAseXkvBI2u&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&p_p_col_id=column-2&p_p_col_pos=1&p_p_col_count=2&_101_INSTANCE_yhAseXkvBI2u_delta=200&_101_INSTANCE_yhAseXkvBI2u_keywords=&_101_INSTANCE_yhAseXkvBI2u_advancedSearch=false&_101_INSTANCE_yhAseXkvBI2u_andOperator=true&p_r_p_564233524_resetCur=false&_101_INSTANCE_yhAseXkvBI2u_cur=2