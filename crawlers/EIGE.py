from bs4 import BeautifulSoup
from csv import DictWriter
from datetime import datetime
import json
import os
import re
import requests
from urllib.parse import urljoin

DEBUG = False

COUNTER = 0
AGENCY = "EIGE"

baseurl = "https://eige.europa.eu/"

full_json = []
monthyears = set()

csv_file = f"{AGENCY}.csv"
json_file = f"{AGENCY}.json"

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def make_results_page_url(i:int) -> str:
    return f"https://eige.europa.eu/news?page={i}"
    # return f"https://www.eea.europa.eu/highlights#c7=en&c6=&b_start={number}"

def check_status_code(response, url):
    if not response.status_code == 200:
        raise RuntimeError(f"Unable to retrieve page {url}.\nStatus Code: {response.status_code}")

def make_result_selector(i:int) -> str:
    return f"div.views-row:nth-child({i}) > article:nth-child(1) > a:nth-child(1)"


def make_url(results_elem_list:list) -> str:
    href = results_elem_list[0]['href']
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

def process_page(url, full_json):
    response = requests.get(url)

    check_status_code(response, url)

    soup = BeautifulSoup(response.text, features='lxml')

    try:
        title = soup.select(".title")[0].text.strip('\n').strip()
    except:
        title = "NULL"

    print(f'\t- {title}')
    try:
        date = soup.select(".date-display-single")[0].text.strip()
        date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')
    except:
        date = "NULL"
    print(f'\t\t{date}')

    try:
        text = soup.select('.content')[0].text.strip()
        text = process_text(text)
    except:
        text = "NULL"

    # print(f'\t\t{text}')

    text_type = "NEWS"
    text_id = make_ID(text_type, AGENCY)

    pagedata = {
            'id':text_id,
            'date':date,
            'text_type':text_type,
            'title':title,
            'text':text,
            'url':url,
        }

    full_json.append(pagedata)


i = 0
go_on = True
while go_on:
    print(f"Processing page {i+1}")
    r_p_url = make_results_page_url(i)
    print(r_p_url)
    i += 1

    r_p_response = requests.get(r_p_url)
    check_status_code(r_p_response, r_p_url)

    if i == 10 and DEBUG:
        break

    r_p_soup = BeautifulSoup(r_p_response.text, features='lxml')

    # # if "There are currently no items in this folder".lower() in r_p_soup.text.lower():
    # #     go_on = False
    # #     print("break trough match")
    # #     break

    results = r_p_soup.select(".node")

    if not results:
        go_on = False
        print("break trough empty results")
        break

    for result_elem in results:
        # print(result_elem.select('a'))
        
        href_elem_list = result_elem.select('a')

        
    #     date_elem_list = result_elem.select('.date-display-single')
        url = make_url(href_elem_list)

        process_page(url, full_json)
        # input()
        
print()

with open(os.path.join(output_folder, json_file), 'wt') as f:
    json.dump(full_json, f)

print(f"Json saved to: {os.path.join(output_folder, json_file)}")


# # write to csv
with open(os.path.join(output_folder, csv_file), 'wt') as csvfile:
    fieldnames = ['id', 'date', 'text_type', 'title', 'text', 'url']
    writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

    writer.writeheader()
    for item in full_json:
        writer.writerow(item)

print(f"CSV saved to: {os.path.join(output_folder, csv_file)}")