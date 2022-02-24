from bs4 import BeautifulSoup
from csv import DictWriter
from datetime import datetime
import json
from multiprocessing.pool import ThreadPool
import os
import re
import requests
from urllib.parse import urljoin
import time

DEBUG = False

PROCESSES = 5
MAX_RETRIES = 10

COUNTER = 0
AGENCY = "ESMA"

# baseurl = "https://www.eiopa.europa.eu/"
baseurl = f"https://www.{AGENCY.lower()}.europa.eu"

full_json = []

# csv_file = f"{AGENCY}.csv"
# json_file = f"{AGENCY}.json"

csv_file = f"{AGENCY}_NEWS.csv"
json_file = f"{AGENCY}_NEWS.json"

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def make_results_page_url(i:int) -> str:
    return f"https://www.esma.europa.eu/press-news/esma-news?page={i}"
    # return f"https://www.eea.europa.eu/highlights#c7=en&c6=&b_start={number}"

def check_status_code(response, url):
    if not response.status_code == 200:
        raise RuntimeError(f"Unable to retrieve page {url}.\nStatus Code: {response.status_code}")

# def make_result_selector(i:int) -> str:
#     return f"div.views-row:nth-child({i}) > article:nth-child(1) > a:nth-child(1)"


def make_url(href:list) -> str:
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

def process_page(url, full_json, title):
    response = requests.get(url)
    try:
        check_status_code(response, url)
    except RuntimeError:
        retry_succeeded = False
        for i in range(MAX_RETRIES):
            print(f"\t xxx Failed to download page. Retry in {i} seconds.")
            time.sleep(i)
            response = requests.get(url)
            if response.status_code == 200:
                print("\t +++ Retry succeeded")
                retry_succeeded = True
                break
        
        if not retry_succeeded:
            print(f"\t +++ Retry failed after {MAX_RETRIES} attempts.")
            

    soup = BeautifulSoup(response.text, features='lxml')

    # try:
    #     title = soup.select(".title-main")[0].text.strip('\n').strip()
    # except:
    #     title = "NULL"

    print(f'\t- {title}')
    
    try:
        date = soup.select(".field-type-ds")[0].text.strip()
        date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')
    except:
        date = "NULL"
    # print(f'\t\t{date}')
    # input()
    
    try:
        text = soup.select_one('.field-type-text-long').text
        text = process_text(text)
    except AttributeError:
        text = "NULL"

    # print(f'\t\t{text}')
    # input()

    tags = soup.select(".section_link")
    text_type = ''
    for tag in tags:
        if "Press Releases".lower() in tag.text.lower():
            text_type = "PR"
            break
    if not text_type == "PR":
        text_type = "NEWS"
    
    # text_type = text_type
    # input(text_type)

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

def parse_date(datestr):
    input(datestr)
    # input(date)
    try:
        date = datetime.strptime(datestr, '%B %d, %Y').strftime('%Y-%m-%d')
    except:
        date = "NULL"

    return date

def process_result_elem(result_elem):
        # print(result_elem.text)
        # print(result_elem.select('a'))
        href = result_elem.select_one('h2 > a')["href"]

        url = make_url(href)

        # input(url)


        # date = result_elem.select_one('td > div:nth-child(1) > div')
        # date = parse_date(date)
        # input(date)

        title = result_elem.select_one('h2 > a').text
        # input(title)

        # tags = result_elem.select_one()
    #     text_type = "PR"
    # #     if "News Item" in text_type:
    # #         text_type = "NEWS"
    # #     elif "Press Release" in text_type:
    # #         text_type = "PR"
    # #     else:
    # #         text_type = "UNK"
    # #     # input(text_type)

        process_page(url, full_json, title)
    # #     # input()

i = 0
go_on = True
while go_on:
    print(f"Processing page {i+1}")
    r_p_url = make_results_page_url(i)

    if i == 10 and DEBUG:
        break

    print(r_p_url)
    i += 1

    # input()

    r_p_response = requests.get(r_p_url)
    check_status_code(r_p_response, r_p_url)

    r_p_soup = BeautifulSoup(r_p_response.text, features='lxml')


    results = r_p_soup.select("td > div")
    # print(len(results))
    # input()

    if not results:
        go_on = False
        print("break trough empty results")
        break

    if DEBUG:
        print("\tLOOP")
        for result_elem in results:
            process_result_elem(result_elem)
    else:
        print("\tMULTITHREDING")
        with ThreadPool(processes=PROCESSES) as p:
            p.map(process_result_elem, results)

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

print()
print(f"A total of {COUNTER} articles were downloaded.")