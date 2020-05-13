import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
# https://www.sec.gov/cgi-bin/browse-edgar?CIK=1084765&owner=exclude&action=getcompany
from myConfigs import get_company_url_prefix, sec

def getCompanyFileURL(company_id) :
    company_url = get_company_url_prefix + str(company_id)

    print "Company URL = " + company_url
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
    # print files_page_content
    if is_exclusive_search:
        all_tds = files_page_content.find_all('td', text="10-K")
    else:
        all_tds = files_page_content.find_all('td', text=re.compile('10-K', re.DOTALL))

    url_list = []
    for one_td in all_tds:
        try:
            url_list.append(one_td.find_next_sibling('td').find('a')['href'])
        except:
            continue
    return [ url_list, files_page_content ]

def get_10K_dates(html_doc):
    file_page_content  = BeautifulSoup(html_doc, "html.parser")

    dates_to_search = ["Filing Date", "Period of Report"]

    dates = {}
    for current_search in dates_to_search:
        current_date = file_page_content.find('div', text=current_search).find_next_sibling('div').text
        dates[current_search] = current_date
    return dates

# 1084765 - only one 10K
# 1041803 - multiple 10K

# https://www.sec.gov/cgi-bin/browse-edgar?owner=exclude&action=getcompany&CIK=1041803

# u'/Archives/edgar/data/1041803/000104180319000042/0001041803-19-000042-index.htm


def get_10K_metadata(html_doc):
    metadata = get_10K_dates(html_doc)
    metadata['File URL'] = sec + get_single_file_path(html_doc, is_exclusive_search=False)[0][0]
    return metadata

company_file_path_list = getCompanyFileURL(1041803)

d = 0
i = 0
for path in company_file_path_list:
    files_page_url = sec + path

    html_doc = requests.get(files_page_url, timeout = 5).content
    current_metadata = get_10K_metadata(html_doc)
    
    if d == 0:
        d = current_metadata
    else:
        d.append(current_metadata)

    print d
    if i == 2:
        break
    i+=1

#############





