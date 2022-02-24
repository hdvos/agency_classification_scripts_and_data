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
AGENCY = "EMCDDA"

# baseurl = "https://www.eiopa.europa.eu/"
baseurl = f"https://www.{AGENCY.lower()}.europa.nu"

full_json = []


csv_file = f"{AGENCY}.csv"
json_file = f"{AGENCY}.json"

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def make_results_page_url(i:int) -> str:
    return f"http://www.emcdda.europa.eu/news/home_en?page={i}"
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

def process_page(url, full_json, title, date):
    response = requests.get(url)

    check_status_code(response, url)

    soup = BeautifulSoup(response.text, features='lxml')

    # try:
    #     title = soup.select(".title-main")[0].text.strip('\n').strip()
    # except:
    #     title = "NULL"

    print(f'\t- {title}')
    # try:
    #     date = soup.select("div.metadata-item:nth-child(2)")[0].text.strip()
    #     date = datetime.strptime(date, '%d %b %Y').strftime('%Y-%m-%d')
    # except:
    #     date = "NULL"
    print(f'\t\t{date}')

    try:
        text_candidates = soup.select('.field')
        longest_text = ''
        for item in text_candidates:
            if len(item.text) > len(longest_text):
                longest_text = item.text
        text = longest_text
        text = process_text(text).replace("Contact Us", ' ')
    except:
        text = "NULL"

    # print(f'\t\t{text}')
    
    text_type = "UNK"
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
    try:
        date = datetime.strptime(datestr, '%d.%m.%Y').strftime('%Y-%m-%d')
    except:
        date = "NULL"

    return date

i = 0
go_on = True
while go_on:
    print(f"Processing page {i+1}")
    r_p_url = make_results_page_url(i)

    if i == 10 and DEBUG:
        break

    print(r_p_url)
    i += 1

    r_p_response = requests.get(r_p_url)
    check_status_code(r_p_response, r_p_url)


    r_p_soup = BeautifulSoup(r_p_response.text, features='lxml')


    results = r_p_soup.select(".views-row")
    print(len(results))

    # input()
    if not results:
        go_on = False
        print("break trough empty results")
        break

    for result_elem in results:
        # print(result_elem.text)
        # print(result_elem.select('a'))
        href = result_elem.select('a')[0]["href"]
        
        url = make_url(href)

        date = result_elem.select_one('.date').text.strip()
        date = parse_date(date)

        title = result_elem.select_one('.title').text
        # input(title)
        process_page(url, full_json, title, date)
        # # input()
        
# print()

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