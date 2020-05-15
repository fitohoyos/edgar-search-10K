import requests
import re
import os
import pandas as pd
from bs4 import BeautifulSoup
# https://www.sec.gov/cgi-bin/browse-edgar?CIK=1084765&owner=exclude&action=getcompany
from myConfigs import get_company_url_prefix, sec, data_folder

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
        # print one_td
        print one_td
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
            print one_td

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
    # print single_file_path
    if len(single_file_path[0]) > 0:
        metadata['File URL'] = [sec + single_file_path[0][0]]
    else:
        metadata['File URL'] = [""]
    # print metadata
    return pd.DataFrame(metadata)


'''
company_id = 1041803


company_file_path_list = getCompanyFileURL(company_id)

i = 0
for path in company_file_path_list:
    print
    print "Getting info from " + path
    print
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
                print 'New file added to database'
                d.loc[len(d.index)] = row
            else:
                print 'File metadata was previously in database'
    else:
        d = current_metadata
    d.to_csv(table_file_path, index = None, header=True, encoding='utf-8')
   
'''

url = u'https://www.sec.gov/Archives/edgar/data/1041803/000104180308000008/0001041803-08-000008-index.htm'
html_doc = requests.get(url, timeout = 5).content
get_single_file_path(html_doc, True)




