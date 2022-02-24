from csv import DictWriter

from datetime import datetime

import json

import os, sys

import re 

import requests

import tika
tika.initVM()
from tika import parser as pdfparser

import time

from urllib import parse as urlparse

ACER_counter = 0

start_year = 2011
end_year = 2020

output_folder = '../crawling_files/ACER_DOCUMENTS/'
full_json = []

class Meta(object):

    base_url = 'https://www.acer.europa.eu/Media/'
               

    pdf_name_base = '../crawling_files/ACER_DOCUMENTS/ACER_PR_PDFs/'

    def __init__(self, year, edition):
        self.year = year
        self.edition = edition

    def make_url(self, divider = '-', divider2 = '%20', divider3 = '-'):
        url_ending = f"Press%20releases/ACER{divider2}PR{divider}{meta.edition:02}{divider3}{str(meta.year)[-2:]}.pdf"
        self.url = urlparse.urljoin(self.base_url, url_ending)
        
        return self.url

    def make_pdf_filename(self):
        self.pdf_name = f"{self.pdf_name_base}_{self.year}_{self.edition:02}.pdf"
        return self.pdf_name

# def make_url(meta):
#     return meta.make_url()
    
# def make_pdf_filename(meta):
#     return meta.make_pdf_filename()

def save_pdf(response, meta):
    filename = meta.make_pdf_filename()
    with open(filename, 'wb') as f:
        f.write(response.content)
    return filename

def download_pdf(meta):
    response = requests.get(meta.url)
    
    if response.status_code == 200:
        pdf_location = save_pdf(response, meta)
        return pdf_location
    else:
        return False

def parse_date(text):
    m = re.search(r"(Ljubljana|Brussels|Vienna), ([0-9]+) ([A-Za-z]+),? ([0-9]{4})", text)
    try:

        day = m.group(2)
        month = m.group(3)
        year = m.group(4)

        date = datetime.strptime(f'{day} {month} {year}', '%d %B %Y')

        return date.strftime('%Y-%m-%d')
    except:
        print("NO DATE")
        return "NULL"

def process_pdf(filename, meta):
    global ACER_counter
    parsed = pdfparser.from_file(filename)
    text = parsed['content']
    text = re.sub('\n', ' ', text)
    text = re.sub(' +', ' ', text)
    date = parse_date(text)
    ACER_counter += 1
    document_json = {
        'id':f"ACER_PR_{ACER_counter:03}",
        'date':date,
        'year':meta.year,
        'edition':meta.edition,
        'url':meta.url,
        'pdf_doc':meta.pdf_name,
        'text':text,
    }

    return document_json

if __name__ == "__main__":
    for year in range(start_year, end_year+1):
        edition = 1
        
        while True:
            # time.sleep(1)
            print(f"Year: {year} - Edition: {edition}")
            meta = Meta(year, edition)
            url = meta.make_url()

            download_result = download_pdf(meta)

            if not download_result:
                
                url = meta.make_url(divider=' ')
                print(f'\tDownloading failed, trying again with: {url}')
                download_result = download_pdf(meta)
                if not download_result:
                    url = meta.make_url(divider='-', divider2= '-')
                    print(f'\tDownloading failed, trying again with: {url}')
                    download_result = download_pdf(meta)
                    if not download_result:
                        url = meta.make_url(divider2= ' ', divider3='- ')
                        print(f'\tDownloading failed, trying again with: {url}')
                        download_result = download_pdf(meta)
                        if not download_result:
                            url = meta.make_url(divider2= '- ')
                            print(f'\tDownloading failed, trying again with: {url}')
                            download_result = download_pdf(meta)
                            if not download_result:
                                url = meta.make_url(divider = ' ', divider2= ' ', divider3= ' ')
                                print(f'\tDownloading failed, trying again with: {url}')
                                download_result = download_pdf(meta)
                                if not download_result:
                                    print(f'\t\tDownloading failed again. Going on with {year + 1}')
                                    break
                        
            doc_json = process_pdf(download_result, meta)
            full_json.append(doc_json)

            
            edition += 1
            
    # Write to json
    with open(os.path.join(output_folder, 'ACER_press_releases.json'), 'wt') as f:
        json.dump(full_json, f)

    # write to csv
    with open(os.path.join(output_folder,'ACER_press_releases.csv'), 'wt') as csvfile:
        fieldnames = ['id', 'date', 'year', 'edition', 'url', 'pdf_doc', 'text',]
        writer = DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')

        writer.writeheader()
        for item in full_json:
            writer.writerow(item)
    
    print(f"Done. Downloaded {len(full_json)} documents and stored them in {output_folder}")
