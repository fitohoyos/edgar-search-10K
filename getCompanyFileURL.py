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
        metadata['FileURL'] = [sec + single_file_path[0][0]]
    else:
        metadata['FileURL'] = [""]
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
                if not row['FileURL'] in d['FileURL'].values:
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

def remove_empty_rows_from_list_of_lists(data):
    return [row for row in data if (not all((any(cell == check_for for check_for in [0, '0', ''])) for cell in row))]

def traspose_list_of_lists(l):
    return list(map(list, zip(*l)))

def table_tag_to_list_of_lists(table):
    #for row in table.select('tr + tr'):
    #    for td in row.find_all('td'):
    #        print(td.get_text().strip().encode('ascii', 'ignore').decode("utf-8"))
    #exit()
    #print('.'*45)
    #print(table)
    parsed_rows = [[td.get_text().strip().encode('ascii', 'ignore').decode("utf-8") for td in row.find_all("td") if td is not None] for row in table.select("tr + tr")]
    parsed_rows = remove_empty_rows_from_list_of_lists(parsed_rows)
    print(parsed_rows)
    print('3'*15)
    _max = max([ len(row) for row in parsed_rows])
    parsed_rows = [[ row.append(['']) for row in parsed_rows  ] for i in range(0, _max - len(row))]

    #print(parsed_rows)
    #print('.'*15)
    parsed_rows = traspose_list_of_lists(parsed_rows)
    #print('.'*15)
    #print(parsed_rows)
    parsed_rows = remove_empty_rows_from_list_of_lists(parsed_rows)
    return traspose_list_of_lists(parsed_rows)

def update_scrape(row, success, doable=True, scraped=True):
    file_full_path = data_folder + 'files_metadata.csv'
    row['success'] = str(success) 
    row['scraped'] = str(scraped)
    row['doable'] = str(doable)
    
    if not os.path.isdir(data_folder):
        os.mkdir(data_folder)
    if not os.path.isfile(file_full_path):
        d = pd.DataFrame(row).T
    else:        
        d = pd.read_csv(file_full_path)
        # print(len(d.index))
        d = d[d['FileURL'] != row['FileURL']]
        # print(len(d.index))
        d.loc[len(d.index)] = row
        d = d.sort_values('FileURL', ascending=False).drop_duplicates('FileURL', keep='last', inplace=False)
        # d.drop_duplicates(subset='FileURL')
    d.to_csv(file_full_path, index = None, header=True, encoding='utf-8')
    print("saved in " + file_full_path)


def get_signatures(url, company_id, file_name):
    html_doc = requests.get(url).content
    open("k_10.html", 'wb').write(html_doc)
    k_10_content = BeautifulSoup(html_doc, "html.parser")   

    signature_tables = []
    signature_table_count = 0
    intent = 0
    while signature_table_count == 0:
        all_tables = []
        print('intent #' + str(intent))
        if intent == 0: 
            tables = k_10_content.find_all('table')
            a=0
            for index, table in enumerate(tables, start=0):
                if table is not None:
                    if "signature" in table.get_text().lower() and "title" in table.get_text().lower():
                        all_tables.append(table)                      
        if intent == 1:
            all_tables = []
            search_terms = ["Pursuant to the requirements of Section 13 ", 'SIGNATURES']
            tag_types = ['div', 'p', 'a', 'P', 'A']
            for tag_type in tag_types:
                # print(tag_type)
                for text_to_search in search_terms: 
                    # print(text_to_search)
                    tag = k_10_content.find(lambda elm: elm.name == tag_type and elm.text and text_to_search in elm.text.replace(u'\xa0', ' '))
                    
                    if tag is not None:    
                        table = tag.findNext('table')                         
                        if table is not None:
                            if ('director' in table.text.lower() or 'executive' in table.text.lower()) and 'financial statements' not in table.text.lower():
                                # print('Found one')
                                all_tables.append(table)
                                table = table.findNext('table') 
                                if table not in all_tables:
                                    all_tables.append(table)                    
                    
            # print(tables)
            # exit()
        if intent == 2:
            tables = k_10_content.find_all('table', text=re.compile('Executive Officer', re.DOTALL))
            text_to_search = "Pursuant to the requirements of Section 13"
            div = k_10_content.find(lambda elm: elm.name == "div" and elm.text and text_to_search in elm.text.replace(u'\xa0', ' '))
            if div is not None:
                table = div.findNext('table') 
            #print(tables)
            # exit()
        if intent == 3:
            signature_tags = k_10_content.find_all('a', {'name':'_SIGNATURES'})
            
            if signature_tags is None or len(signature_tags) == 0:                
                signature_tags = k_10_content.find_all('div', text=re.compile('SIGNATURE', re.DOTALL))
            if signature_tags is not None:
                iii = 0
                # print(signature_tags)
                for signature_tag in signature_tags:
                    # print(str(iii) + '-' * 10)
                    while iii<5:
                        print(str(iii))
                        if 'pursuant' in signature_tag.get_text().lower():
                            if 'contents' in signature_tag.get_text().lower():
                                iii += 1
                                continue
                            else:
                                table_tags = signature_tag.find_all('table')
                                if table_tags is not None:
                                    all_tables = table_tags
                                    print(all_tables)
                                    if len(all_tables) > 0:
                                        break
                        signature_tag = signature_tag.parent 
                        iii += 1   
                    print('The end ' + '-' * 10)
        if intent == 4:
            for table in k_10_content.find_all('table'):
                if table is not None:
                    if 'Executive Officer' in table.get_text():
                        print('Found CEO')
                        #print(table)
                        #exit()
                        all_tables.append(table)
                
        if intent > 10:
            break
        # with all_tables[1] as table:
        for table in all_tables:
            if table is not None and len(table)>0:      
                #print(table)       
                #print(type(table))
                #exit()
                print('0'*20)
                if not '.txt' in url:
                    signature_table = table_tag_to_list_of_lists(table)
                else:
                    signature_table = table.get_text()
                    #print(signature_table)
                    #exit()
                if len(signature_table) > 0:
                    signature_tables.append(signature_table)
                    signature_table_count += 1

                    if not os.path.isdir(signatures_folder):
                        os.mkdir(signatures_folder)
                    
                    company_folder = signatures_folder + str(company_id) + "/"
                    if not os.path.isdir(company_folder):
                        os.mkdir(company_folder)
                    
                    save_file_path = company_folder + file_name + "_" +str(signature_table_count)

                    save_file_path = save_file_path + ('.txt' if '.txt' in url else '.csv')

                    with open(save_file_path, "w") as f:
                        if not '.txt' in url:
                            wr = csv.writer(f)
                            wr.writerows(signature_table)
                        else:
                            f.write(signature_table)

                    print("Saved in", save_file_path)
        intent += 1
    print("Found " + str(signature_table_count) + " signature tables for company " + str(company_id))
    return signature_tables



html = requests.get('https://www.sec.gov/Archives/edgar/data/1693696/000139390520000124/uncc_10k.htm').content

open('k_10.html', 'wb').write(html)


d = pd.read_csv("data/files_metadata.csv")

# 2133
# 11238 --> id: 1589150

for index, row in d.iterrows():
    k_10_url = d["FileURL"][index]
    print(k_10_url)
    company_id = d["company_id"][index]
    file_name = str(d["Period of Report"][index])
    if row['success'] != 'True' and row['scraped'] != 'True' and row['doable'] != False:
        if '/ix?doc=' in row['FileURL']:
            a = 9
            # update_scrape(row, success = False, doable=False, scraped=False)
        else:
            signature_tables = get_signatures(k_10_url, company_id, file_name)
            print('# of tables is ' + str(len(signature_tables)))
            if len(signature_tables) != 0:
                update_scrape(row, success = True)
                exit()
            else:
                update_scrape(row, success = False)
    else:
        print('File was already previously scraped')

        
        






