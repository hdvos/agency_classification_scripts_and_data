from requests import api
from selenium import webdriver
import time
from bs4 import BeautifulSoup
import os
from multiprocessing.pool import ThreadPool
from datetime import datetime
import locale
import requests
import re
import json
import selenium

from csv import DictWriter


# https://euipo.europa.eu/ohimportal/en/news 
DEBUG = False

PROCESSES = 3
MAX_RETRIES = 10

COUNTER = 0
AGENCY = "EUIPO"

full_json = []

# csv_file = f"{AGENCY}.csv"
# json_file = f"{AGENCY}.json"

# csv_file = f"{AGENCY}_NEWS.csv"
# json_file = f"{AGENCY}_NEWS.json"

csv_file = f"{AGENCY}_NEWS.csv"
json_file = f"{AGENCY}_NEWS.json"

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

output_folder = f'/home/hugo/MEGA/work/Agency_Classification/crawling_files/{AGENCY}_DOCUMENTS/'     # The place where the output files will go
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def modify_url(url:str):
    basesplit = url.split('?')
    apistring = basesplit[1]
    api_arguments = apistring.split('&')
    desired_args = []

    for argument in api_arguments:
        if argument.startswith("p_p_id"):
            desired_args.append(argument)
        if argument.startswith("journalId"):
            desired_args.append(argument)

    new_argstring = '&'.join(desired_args)
    new_url = '?'.join([basesplit[0],new_argstring])
    return new_url


def check_status_code(response, url):
    if not response.status_code == 200:
        raise RuntimeError(f"Unable to retrieve page {url}.\nStatus Code: {response.status_code}")

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

def process_page(url, full_json, title, date):
    # input(url)
    print(url)
    url = modify_url(url)
    # response = requests.get(url)
    # input("Im here")

    with webdriver.Firefox() as driver2:
        driver2.get(url)
        try:
            # time.sleep(3)
            cookiebtn = driver2.find_element_by_css_selector(".cookieBannerButton")
            # driver.switchTo().frame(cookiebtn)
            cookiebtn.click()
# driver.findElement(By.name("iframeWithElement")));

        except Exception as e:
            print(e)
            pass
        page_html = driver.page_source
        # input('test')
    # try:
    #     check_status_code(response, url)
    # except RuntimeError:
    #     retry_succeeded =     cookiebtn = driver.find_element_by_css_selector(".cookieBannerButton")
    
    #     for i in range(MAX_RETRIES):
    #         print(f"\t xxx Failed to download page. Retry in {i} seconds.")
    #         time.sleep(i)
    #         response = requests.get(url)
    #         if response.status_code == 200:
    #             print("\t +++ Retry succeeded")
    #             retry_succeeded = True
    #             break
        
    #     if not retry_succeeded:
    #         print(f"\t +++ Retry failed after {MAX_RETRIES} attempts.")
            
    
    soup = BeautifulSoup(page_html, features='lxml')
    # print(soup)
    # input("I made soup")
    # # try:
    # #     title = soup.select(".title-main")[0].text.strip('\n').strip()
    # # except:
    # #     title = "NULL"

    # print(f'\t- {title}')
    
    # # try:
    # #     date = soup.select(".field-type-ds")[0].text.strip()
    # #     date = datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')
    # # except:
    # #     date = "NULL"
    # # print(f'\t\t{date}')
    # # input()
    
    try:
        text = soup.select_one('#layout-column_column-1 > div:nth-child(1) > div:nth-child(2) > div:nth-child(1)').text
        text = process_text(text)
    except AttributeError:
        text = "NULL"

    # print('+'*50)
    # print(f'\t\t{text}')
    # print('+'*50)
    # input()

    text_type = "NEWS"
    # # tags = soup.select(".section_link")
    # # text_type = ''
    # # for tag in tags:
    # #     if "Press Releases".lower() in tag.text.lower():
    # #         text_type = "PR"
    # #         break
    # # if not text_type == "PR":
    # #     text_type = "NEWS"
    
    # # text_type = text_type
    # # input(text_type)

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
    # input(datestr)
    try:
        # date = datetime.strptime(datestr, '%B %m, %Y').strftime('%Y-%m-%d')
        date = datetime.strptime(datestr, '%B %d, %Y')
        # print("INTERMEDIATE:", date)
        date = date.strftime('%Y-%m-%B')


    except Exception as e:
        print(e)
        date = "NULL"

    return date

def process_result_elem(result_elem):
        # print(result_elem.text)
        # print(result_elem.select('a'))
        href = result_elem.select_one('h4 > a')["href"]
        # print(href)
        url = href
    #     # input(url)

        date = result_elem.select_one('.meta-info').text.split('-')[0].strip()
        # print('IN:', date)
        date = parse_date(date)
        print('OUT:', date)
    #     # input(date)

        title = result_elem.select_one('h4').text.strip()
        print(title)

        process_page(url, full_json, title, date)
    # # # #     # input()



i = 0
go_on = True

print("LOCALE:", locale.getlocale())

try:
    with webdriver.Firefox() as driver:
        driver.get("https://euipo.europa.eu/ohimportal/en/news")
        time.sleep(5)

        cookiebtn = driver.find_element_by_css_selector(".cookieBannerButton")
        cookiebtn.click()
        # input("check")
        while go_on:
            # print(driver)
            r_p_html = driver.page_source
            # print(x)
            r_p_soup = BeautifulSoup(r_p_html, features='lxml')

            results = r_p_soup.select("article")

            if len(results) <10 or i >= 170:
                print("No nxtbtn")
                go_on = False

            if DEBUG:
                print("\tLOOP")
                for result_elem in results:
                    process_result_elem(result_elem)
            else:
                print("\tMULTITHREDING")
                with ThreadPool(processes=PROCESSES) as p:
                    p.map(process_result_elem, results)

            print(len(results))


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




            # print(r_p_soup)
            nxt = driver.find_element_by_css_selector(".next-page ")
            nxt.click()
            i+= 1
            # input("check2")
finally:
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
