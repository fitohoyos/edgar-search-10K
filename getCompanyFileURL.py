import requests
import re
import os
import csv
import pandas as pd
from urllib.parse import urlparse
from bs4 import BeautifulSoup
# https://www.sec.gov/cgi-bin/browse-edgar?CIK=1084765&owner=exclude&action=getcompany
from myConfigs import get_company_url_prefix, sec, data_folder, signatures_folder

def get_company_source_page_URLs(company_id) :
    company_url = get_company_url_prefix + str(company_id)

    print("Company URL = " + company_url)
    company_file_url_list = []
    html_doc = requests.get(company_url, timeout = 5).content
    [initial_company_urls, raw_results] = get_single_file_path(html_doc, is_exclusive_search=True)

    for file_url in initial_company_urls :
        company_file_url_list.append(file_url)

    start = 100
    while len(raw_results.find_all('input', attrs = { 'value' : "Next 100" })) > 0:
        html_doc = requests.get(company_url + "&start=" + str(start), timeout = 5).content
        [initial_company_urls, raw_results] = get_single_file_path(html_doc, is_exclusive_search=True)
        for file_url in initial_company_urls :
            company_file_url_list.append(file_url)
        start += 100



    return company_file_url_list

def get_single_file_path(html_doc, is_exclusive_search):
    files_page_content  = BeautifulSoup(html_doc, "html.parser")
    if is_exclusive_search:
        all_tds = files_page_content.find_all('td', text="10-K")
    else:
        all_tds = files_page_content.find_all('td', text=re.compile('10-K', re.DOTALL))

    url_list = []
    for one_td in all_tds:
        the_sibling = one_td.find_next_sibling('td')
        if the_sibling:
            the_a = the_sibling.find('a')
            if the_a:
                url = the_a['href']
                if url:
                    url_list.append(url)
    if len(url_list) == 0:
        if is_exclusive_search:
            all_tds = files_page_content.find_all('td', text="10-K")
        else:
            all_tds = files_page_content.find_all('td', text=re.compile('10-K', re.DOTALL))
        for one_td in all_tds:
            url = one_td.find_previous_sibling('td').find('a')['href']
            url_list.append(url)
    return [ url_list, files_page_content ]

def get_10K_dates(html_doc):
    file_page_content  = BeautifulSoup(html_doc, "html.parser")

    dates_to_search = ["Filing Date", "Period of Report"]

    dates = {}
    for current_search in dates_to_search:
        current_date = file_page_content.find('div', text=current_search).find_next_sibling('div').text
        dates[current_search] = [current_date]
    return dates

# 1084765 - only one 10K
# 1041803 - multiple 10K

# https://www.sec.gov/cgi-bin/browse-edgar?owner=exclude&action=getcompany&CIK=1041803

# u'/Archives/edgar/data/1041803/000104180319000042/0001041803-19-000042-index.htm


def get_10K_metadata(html_doc):
    metadata = get_10K_dates(html_doc)
    single_file_path = get_single_file_path(html_doc, is_exclusive_search=False)
    if len(single_file_path[0]) > 0:
        metadata['File URL'] = [sec + single_file_path[0][0]]
    else:
        metadata['File URL'] = [""]
    return pd.DataFrame(metadata)

def scrape_company_files(company_id):
    company_file_path_list = get_company_source_page_URLs(company_id)

    i = 0
    for path in company_file_path_list:
        print("Getting info from " + path)
        files_page_url = sec + path

        html_doc = requests.get(files_page_url, timeout = 5).content
        
        current_metadata = get_10K_metadata(html_doc)
        current_metadata['company_id'] = [company_id] * len(current_metadata.index)

        current_metadata['source_page'] = [sec+path] * len(current_metadata.index)

        table_name = 'files_metadata'

        if not os.path.isdir(data_folder):
            os.mkdir(data_folder)


        table_file_path = data_folder + table_name + '.csv'
        if os.path.isfile(table_file_path):
            d = pd.read_csv(table_file_path)

            for index, row in current_metadata.iterrows():
                if not row['File URL'] in d['File URL'].values:
                    print('New file added to database')
                    d.loc[len(d.index)] = row
                else:
                    print('File metadata was previously in database')
        else:
            d = current_metadata
        d.to_csv(table_file_path, index = None, header=True, encoding='utf-8')


def get_last_id_in_company_db():
    d = pd.read_csv(data_folder + "files_metadata" + ".csv")
    return d['company_id'].values[len(d.index)-1]
'''
url = u'https://www.sec.gov/Archives/edgar/data/1041803/000104180308000008/0001041803-08-000008-index.htm'
html_doc = requests.get(url, timeout = 5).content
open('rancia.html', 'wb').write(html_doc)  
get_single_file_path(html_doc, True)
'''

# company_id = 1041803
# 

def get_all_company_data(company_data_file = "misc/company_data.csv"):
    d = pd.read_csv(company_data_file)
    company_id_list = list(set(d['f_cik'].values))

    i = 1
    found_last = False
    last_id_in_db = get_last_id_in_company_db()
    print("Last ID is: " + str(last_id_in_db))
    for company_id in company_id_list:
        if found_last:
            print(" ("+ str(i) + "/"+ str(len(company_id_list))  + ")--------------------- Searching id=" + str(company_id))
            scrape_company_files(company_id)
            i += 1 
        else:
            found_last = str(company_id) == str(last_id_in_db)

def get_signatures(url, company_id, file_name):
    # url_parsed = urlparse(url)
    # file_name_with_ext = os.path. basename(url_parsed.path)
    # file_name = os.path.splitext(file_name_with_ext)[0]

    html_doc = requests.get(url).content
    open("k_10.html", 'wb').write(html_doc)
    k_10_content = BeautifulSoup(html_doc, "html.parser")    
    all_tables = k_10_content.find_all('table')
    i = 0
    for table in all_tables:
        if table is not None:
            if "signature" in table.get_text().lower() and "title" in table.get_text().lower():
                #if "title" in table.get_text().lower():
                #    print(table)
                #    break
                if not os.path.isdir(signatures_folder):
                    os.mkdir(signatures_folder)
                i += 1
                company_folder = signatures_folder + str(company_id) + "/"
                if not os.path.isdir(company_folder):
                    os.mkdir(company_folder)
                save_file_path = company_folder + file_name + "_" +str(i) + ".csv"
                with open(save_file_path, "w") as f:
                    wr = csv.writer(f)
                    wr.writerows([[td.p.get_text() for td in row.find_all("td") if td.p is not None] for row in table.select("tr + tr")])
    print("Found " + str(i) + " signature tables for company " + str(company_id))
d = pd.read_csv("data/files_metadata.csv")
# 2133
# 11238 --> id: 1589150

for index, row in d.iterrows():
    k_10_url = d["File URL"][index]
    print(k_10_url)
    company_id = d["company_id"][index]
    file_name = str(d["Period of Report"][index])

    get_signatures(k_10_url, company_id, file_name)

        
        






