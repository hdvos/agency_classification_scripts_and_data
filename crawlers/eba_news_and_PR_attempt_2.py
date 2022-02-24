from bs4 import BeautifulSoup
from csv import DictWriter
from datetime import datetime
import json
import os, sys
import re
import requests
import time
from tqdm import tqdm
from urllib.parse import urljoin
COUNTER = 0

full_json = []      # JSON where all info will be stored

base_url = 'https://eba.europa.eu/'     # Base url to be combined with href extensions

output_folder = '/home/hugo/MEGA/work/Agency_Classification/crawling_files/EBA_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def make_results_page_url(i:int) -> str:
    """Make url that leads to a result page/
    
    :param i: Iterator indicating what result page url to generate.
    :type i: int
    :return: Url to the result page
    :rtype: str
    """
    return f"https://eba.europa.eu/all-news-and-press-releases?page={i}"

def make_result_selector(result_nr:int) -> str:
    """Generate a selector to select a single result for a results page.
    
    :param result_nr: A number (n) range(1,11) indicating the nth result of the page to be selected
    :type result_nr: int
    :return: A css selector for the requested element.
    :rtype: str
    """
    return f"div.views-row:nth-child({result_nr}) > div:nth-child(1) > span:nth-child(1) > a:nth-child(1)"


def get_href(elem_lst:list) -> str:
    """Extract the url to a page from a selected element.
    
    :param elem_lst: A list containing a soup element.
    :type elem_lst: list
    :return: The url to a single page containing news or a press release.
    :rtype: str
    """
    href_ending = elem_lst[0]['href']
    href = urljoin(base_url, href_ending)
    return href

def get_text_type(elem_lst:list) -> str:
    """Get the text type of a certain page.
    
    :param elem_lst: A list containing a soup element.
    :type elem_lst: list
    :return: An abbreviation indicating the text type. NEWS, PR (Press Release), NP (News & Press)
    :rtype: str
    """ 
    elem_text = elem_lst[0].text
    if "News" in elem_text[-6:]:
        text_type = "NEWS"
    elif "Press Releases" in elem_text[-16:]:
        text_type = "PR"
    elif "News & press" in elem_text[-14:]:
        text_type = "NP"
    else:
        text_type = "NULL"

    return text_type

def process_date(datestr:str) -> str:
    """Processes the date of publication in the desired date format.
    
    :param datestr: Date string extracted from page.
    :type datestr: str
    :return: Date string in the desired format.
    :rtype: str
    """
    date = datetime.strptime(datestr, "%d %B %Y")
    date = date.strftime('%Y-%m-%d')
    return date

def make_ID(text_type) -> str:
    """Generate a unique ID  for the press release or news item. NOTE: this id is unique during one run, but might not be persisten when the script is rerun as the script works back in time.
    
    :param text_type: the text type (NEWS, PR, NP)
    :type text_type: [type]
    :return: identifier for the text.
    :rtype: str
    """
    global COUNTER
    COUNTER += 1
    return f"EBA_{text_type}_{COUNTER:04}"

def process_page(url:str, full_json:list, text_type:str):
    """Processes a single news item or press release.
    
    :param url: Url of the news item or press release.
    :type url: str
    :param full_json: Json list object where the results will be appended to.
    :type full_json: list
    :param text_type: text type (NEWS, PR, NP)
    :type text_type: str
    :raises RuntimeError: If the response code from the request is not 200
    """
    response = requests.get(url)
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, features='lxml')

        try:
            title = soup.select('.pane-title')[0].text.strip()
        except:
            title = "NULL"
        print(f"\t- {title}")
        
        try:
            date = soup.select('.SecondaryInfo')[0].text
            date = process_date(date)
        except:
            date = "NULL"
        print(f"\t\t- {date}")

        try:
            text = soup.select('.FreetextArea')[0].text
            text = text.replace('\n', ' ')
            text = re.sub(' +', ' ', text)
        except:
            text = "NULL"

        text_id = make_ID(text_type)

        pagedata = {
                'id':text_id,
                'date':date,
                'text_type':text_type,
                'title':title,
                'text':text,
                'url':url,
            }

        full_json.append(pagedata)

        print(f"\t\t- {text_type}")
    else:
        raise RuntimeError("Bad request")


go_on = True
i = 1
while go_on:
    response = requests.get(make_results_page_url(i))
    print(f"Processing page {i}")
    i += 1
    if not response.status_code == 200:
        raise RuntimeError(f"Unable to retrieve webpage. Status code {response.status_code}")
    results_page_soup = BeautifulSoup(response.text, features='lxml')
    
    for result_nr in range(1,11):
        selector = make_result_selector(result_nr)
        elem_lst = results_page_soup.select(selector)
        if not elem_lst:
            go_on = False
            break

        url = get_href(elem_lst)

        text_type = get_text_type(elem_lst)
        
        process_page(url, full_json, text_type)
    

with open(os.path.join(output_folder, 'EBA_NEWS_and_PR.json'), 'wt') as f:
    json.dump(full_json, f)

# write to csv
with open(os.path.join(output_folder,'EBA_NEWS_and_PR.csv'), 'wt') as csvfile:
    fieldnames = ['id', 'date', 'text_type', 'title', 'text', 'url']
    writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

    writer.writeheader()
    for item in full_json:
        writer.writerow(item)
