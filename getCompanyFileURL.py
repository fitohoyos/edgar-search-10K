import requests
from bs4 import BeautifulSoup
# https://www.sec.gov/cgi-bin/browse-edgar?CIK=1084765&owner=exclude&action=getcompany
from myConfigs import get_company_url_prefix

def getCompanyFileURL(company_id) :
    company_url = get_company_url_prefix + str(company_id)

    print "Company URL = " + company_url
    company_file_url_list = []
    [initial_company_urls, raw_results] = getSinglePageCompanyURL(company_url)

    for file_url in initial_company_urls :
        company_file_url_list.append(file_url)

    return company_file_url_list


def getSinglePageCompanyURL(page_url) :
    company_search_response = requests.get(page_url, timeout = 5)
    html_doc = company_search_response.content

    open('test.html', 'wb').write(html_doc)
    company_results_content  = BeautifulSoup(html_doc, "html.parser")

    all_tds = company_results_content.find_all('td', text="10-K")

    url_list = []
    for one_td in all_tds:
        url_list.append(one_td.find_next_sibling('td').find('a')['href'])
    
    return [ url_list, company_results_content ]

# 1084765 - only one 10K
# 1041803 - multiple 10K

# https://www.sec.gov/cgi-bin/browse-edgar?owner=exclude&action=getcompany&CIK=1041803

html_doc = ""

print getCompanyFileURL(1041803)