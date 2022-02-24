from bs4 import BeautifulSoup
from csv import DictWriter
from datetime import datetime, timedelta
from dateutil.rrule import rrule, WEEKLY
import json
import os
import re
import requests
from urllib.parse import urljoin

#NOTE: this script assumes that there in a week only 25 news items or press releases are published. If more are published, only the 25 newest items of that week are downloaded.

base_url = 'https://www.ema.europa.eu/'

COUNTER = 0
AGENCY = "EMA"

full_json = []

end_date = datetime.today()
# start_date = end_date - timedelta(days=100)
start_date = datetime(1994, 4, 12)  # The first EMA news item was placed on this date.


csv_file = f"{AGENCY}.csv"
json_file = f"{AGENCY}.json"

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)


def make_results_page_url(date):
    start = date
    end = date + timedelta(days = 7)

    return f"https://www.ema.europa.eu/en/search/search/ema_editorial_content/ema_news/field_ema_computed_date_field/%5B{start.year}-{start.month}-{start.day}T22%3A00%3A00Z%20TO%20{end.year}-{end.month}-{end.day}T21%3A59%3A59Z%5D?sort=field_ema_computed_date_field&order=desc"


def check_status_code(response, url):
    if not response.status_code == 200:
        raise RuntimeError(f"Unable to retrieve page {url}.\nStatus Code: {response.status_code}")


def parse_text_type(text_type):
    # input(text_type)
    if "news" in text_type.lower():
        return "NEWS"
    elif "press release" in text_type.lower():
        return "PR"
    else:
        return "OTH"

def process_title(title_str:str):
    title_str = title_str.strip()
    title_list = title_str.split(':')
    text_type = title_list[0]
    title = ' : '.join(title_list[1:])
    title = re.sub(' +', ' ', title)
    title = title.strip()

    text_type = parse_text_type(text_type)
    return title, text_type


def make_result_url(href):
    return urljoin(base_url, href)

def process_text(text:str) -> str:
    text = ' '.join(text.splitlines())
    text = re.sub(r"{[^}]+}", " ", text)
    re.sub(r"<[^>]+>", ' ', text)
    text = text.replace("}(document, 'script', 'twitter-wjs');", " ")
    text = text.replace("lang: en_US Tweet !function(d,s,id)", ' ')
    
    text = re.sub(r' +', ' ', text)
    return text

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

def process_page(url, full_json, title, text_type):
    response = requests.get(url)

    check_status_code(response, url)

    soup = BeautifulSoup(response.text, features='lxml')

    # try:
    #     title = soup.select(".title-main")[0].text.strip('\n').strip()
    # except:
    #     title = "NULL"

    print(f'\t- {title}')
    try:
        date = soup.select(".date-display-single")[0].text.strip()
        date = datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        date = "NULL"
    print(f'\t\t{date}')

    try:
        text = soup.select('.paragraphs-items')[0].text.strip()
        text = process_text(text).replace("Contact Us", ' ')
    except:
        text = "NULL"

    # print(f'\t\t{text}')

    # text_type_str = soup.select('.content-type')[0].text
    # if "news" in text_type_str.lower():
    #     text_type = "NEWS"
    # elif "press" in text_type_str.lower():
    #     text_type = "PR"
    # else:
    #     text_type = "UNK"
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

for i, date in enumerate(rrule(freq =WEEKLY, dtstart=start_date, until=end_date)):
    print('--------------------------')
    print(i, date)
    

    results_url = make_results_page_url(date)
    print(results_url)
    results_response = requests.get(results_url)
    check_status_code(results_response, results_url)

    results_soup = BeautifulSoup(results_response.text, features= 'lxml')
    results_list = results_soup.select(".ecl-list-item")
    
    for result_soup in results_list:
        result_href = result_soup.select('a')[0]['href']
        title = result_soup.select('h3')[0].text

        title, text_type = process_title(title)

        result_url = make_result_url(result_href)

        process_page(result_url, full_json, title, text_type)
        print(result_url)


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