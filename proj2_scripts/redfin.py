"""
Various functions and dicts used to scrape Redfin and analyze scraped data.

sold_by_area_links_Seattle can be replaced with your own list of local search pages;
calibrate depth of search by altering how zoomed-in/granular you go with these links.
Note that any granularity beyond a city will result in duplicate listings that must be
de-duplicated.

type_dict is a very clunky work-around for the myriad of house types Redfin makes
available. Do NOT depend on it working for your dataset.
"""

import random
import re
import time
import os
import sys
import pickle
import requests

import pandas as pd

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium import webdriver
chromedriver = '/Applications/chromedriver'
os.environ["webdriver.chrome.driver"] = chromedriver

sold_by_area_links_Seattle = [
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.74409:47.69068:-122.30638:-122.40011',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.73611:47.68268:-122.2574:-122.35113',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.70017:47.67345:-122.31544:-122.3623',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.7098:47.65635:-122.34018:-122.43391',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.6748:47.62132:-122.30136:-122.39509',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.69562:47.64215:-122.24063:-122.33435',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.63781:47.61105:-122.31995:-122.36682',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.67059:47.56356:-122.21313:-122.40058',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.59167:47.56489:-122.37725:-122.42411',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.5792:47.52562:-122.32803:-122.42176',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.54585:47.49223:-122.31099:-122.40472',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.59386:47.5403:-122.26363:-122.35736',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.5791:47.52552:-122.22798:-122.32171',
    'https://www.redfin.com/city/16163/WA/Seattle/filter/include=sold-5yr,viewport=47.53334:47.47971:-122.20693:-122.30066']

def scrape_sales_pages(salespages_list):
    """
    Takes in a list of complete urls for sold pages within a given area; fetches relative urls for listings.
    At the moment, Redfin will only go up to page 17 within a given geographical boundary.

    salespages_list: list of complete urls (redfin.com/...) as strings
    """
    sales_links = []
    salespages_missed = []
    salespages_completed = []
    error_list = []
    user_agent = UserAgent()
    for pagelink in salespages_list:
        for i in range(17):
            if i > 1:
                target_url = (pagelink + '/page-' + str(i))
            else:
                target_url = pagelink
            user_agent = {'User-agent': user_agent.random}
            try:
                response  = requests.get(target_url, headers = user_agent)
                time.sleep(2)
                soup = BeautifulSoup(response.content, 'html.parser')
                for pagelink in soup.find_all('a', class_ = 'slider-item hidden'):
                    sales_links.append(pagelink.get('href'))
            except:
                error_list.append(sys.exc_info()[0])
                error_list.append(response.status_code)
                salespages_missed.append(target_url)
            time.sleep(random.random()*11)
        salespages_completed.append(pagelink)
    # Geo areas overlap; dropping duplicates of urls
    return {'listing_links': list(set(sales_links)), 'pages_scraped': salespages_completed, 'pages_missed': salespages_missed}

def scrape_listing_pages(sales_links, pickle_it = True):
    """
    Given a list of relative Redfin links (/WA/Seattle...), uses selenium and chromedriver to
    load listing pages, scroll down, and scrape page source. SLOW because of that but no catchpa
    issues. Returns a list of scraped soup objects recast as strings at even indices and their
    corresponding links at the subsequent odd indices.

    sales_links: input list of relative links to Redfin listings as strings
    pickle_it: should function regularly dump scrapes (True) or deliver all at end (False)
    listingpage_scrapes: output list of alternating scrapes as strings and urls as strings
    """
    base_url = 'https://www.redfin.com'
    listingpage_scrapes = []
    driver = webdriver.Chrome(chromedriver)
    counter = 1
    # Iterating through list of links
    for link in sales_links:
        listing_url = base_url + link
        driver.get(listing_url)
        for i in range(14):
            #Scroll
            driver.execute_script("window.scrollBy({top: 700,left: 0,behavior: 'smooth'});")
            time.sleep(0.5 + random.random()*1.5)
        # Getting the page source and soupifying it
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        listingpage_scrapes.append(soup)
        listingpage_scrapes.append(link)
        counter += 1
        # Intermittent pickling of raw html scrapes
        if pickle_it & counter%100 == 0:
            dump_list = [str(x) for x in listingpage_scrapes]
            with open(('listingpage_scrapes_dump' + str(counter) + '.txt'), 'wb') as file_pointer:
                pickle.dump(dump_list, file_pointer)
    if pickle_it:
        dump_list = [str(x) for x in listingpage_scrapes]
        with open(('listingpage_scrapes_dump' + str(counter) + '.txt'), 'wb') as file_pointer:
            pickle.dump(dump_list, file_pointer)
        return str(counter) + ' done and pickled.'
    if not pickle_it:
        return listingpage_scrapes

def parse_sold_page(soup, url):
    """
    Grabs desired information from Redfin page for a single sold property using BeatifulSoup.
    Returns a dictionary of the desired information.
    """
    pull_digits = re.compile('[0-9.]+')
    property_dict = {}
    try:
        property_dict['address'] = soup.find('span', class_ = 'street-address').text
    except:
        property_dict['address'] = ''
    try:
        property_dict['ZIP'] = soup.find('span', class_ = 'postal-code').text
    except:
        property_dict['ZIP'] = ''
    try:
        property_dict['comm'] = soup.find(string = 'Community').find_next('span', 
        class_ = 'content text-right').text
    except:
        property_dict['comm'] = ''
    try:
        property_dict['price'] = pull_digits.search(soup.find('div', 
        class_ = 'info-block price').text.replace(',', '')).group()
    except:
        property_dict['price'] = ''
    try:
        property_dict['beds'] = pull_digits.search(soup.find(attrs = 
        {'data-rf-test-id': "abp-beds"}).find('div', class_ = 'statsValue').text).group()
    except:
        property_dict['beds'] = ''
    try:
        property_dict['baths'] = pull_digits.search(soup.find(attrs = 
        {'data-rf-test-id': "abp-baths"}).find('div', class_ = 'statsValue').text).group()
    except:
        property_dict['baths'] = ''
    try:
        property_dict['size'] = pull_digits.search(soup.find('div', class_ = 
        'info-block sqft').find('span', class_ = 'statsValue').text.replace(',', '')).group()
    except:
        property_dict['size'] = ''
    try:
        property_dict['style'] = soup.find(string = 'Style').find_next().text
    except:
        property_dict['style'] = ''
    try:
        property_dict['lot'] = pull_digits.search(soup.find(string = 
        'Lot Size').find_next().text.replace(',', '')).group()
    except:
        property_dict['lot'] = ''
    try:
        property_dict['age'] = soup.find(string = 'Year Built').find_next().text
    except:
        property_dict['age'] = ''
    try:
        property_dict['status'] = soup.find(attrs = 
        {'data-rf-test-id': 'abp-status'}).find('span', class_ = 'value').text
    except:
        property_dict['status'] = ''
    try:
        property_dict['sold'] = soup.find('div', class_ = 
        "Pill Pill--red padding-vert-smallest padding-horiz-smaller font-size-smaller font-weight-bold font-color-white HomeSash margin-top-smallest margin-right-smaller").text.replace('SOLD BY REDFIN ', '')
    except:
        property_dict['sold'] = ''
    try:
        property_dict['park'] = soup.find(string = 'Parking Information').find_next().text
    except:
        property_dict['park'] = ''
    try:
        property_dict['brok'] = pull_digits.search(soup.find(string = 
        "Buyer's Brokerage Compensation").find_next('span', class_ = 'content text-right').text).group()
    except:
        property_dict['brok'] = ''
    property_dict['url'] = url
    return property_dict

# Not considering the houseboats, plexes, co-ops, and other oddballs (different enough to probably skew model)
type_dict = {'': '',
 '1 1/2 Story': 'house',
 '1 1/2 Story with Basement': 'house',
 '1 1/2 Story with Basement, Cape Cod': 'house',
 '1 1/2 Story with Basement, Colonial': 'house',
 '1 1/2 Story with Basement, Contemporary': 'house',
 '1 1/2 Story with Basement, Craftsman': 'house',
 '1 1/2 Story with Basement, Modern': 'house',
 '1 1/2 Story with Basement, Northwestern Contemporary': 'house',
 '1 1/2 Story with Basement, Traditional': 'house',
 '1 1/2 Story with Basement, Tudor': 'house',
 '1 1/2 Story, Cape Cod': 'house',
 '1 1/2 Story, Contemporary': 'house',
 '1 1/2 Story, Craftsman': 'house',
 '1 1/2 Story, Northwestern Contemporary': 'house',
 '1 1/2 Story, Other (See Remarks)': 'house',
 '1 1/2 Story, Traditional': 'house',
 '1 1/2 Story, Tudor': 'house',
 '1 Story': 'house',
 '1 Story with Basement': 'house',
 '1 Story with Basement, Cape Cod': 'house',
 '1 Story with Basement, Contemporary': 'house',
 '1 Story with Basement, Craftsman': 'house',
 '1 Story with Basement, Modern': 'house',
 '1 Story with Basement, Northwestern Contemporary': 'house',
 '1 Story with Basement, Other (See Remarks)': 'house',
 '1 Story with Basement, Spanish/Southwestern': 'house',
 '1 Story with Basement, Traditional': 'house',
 '1 Story with Basement, Tudor': 'house',
 '1 Story, Cabin': 'house',
 '1 Story, Cape Cod': 'house',
 '1 Story, Contemporary': 'house',
 '1 Story, Craftsman': 'house',
 '1 Story, Modern': 'house',
 '1 Story, Northwestern Contemporary': 'house',
 '1 Story, Other (See Remarks)': 'house',
 '1 Story, Traditional': 'house',
 '2 Stories with Basement': 'house',
 '2 Stories with Basement, Cape Cod': 'house',
 '2 Stories with Basement, Colonial': 'house',
 '2 Stories with Basement, Contemporary': 'house',
 '2 Stories with Basement, Craftsman': 'house',
 '2 Stories with Basement, Modern': 'house',
 '2 Stories with Basement, Northwestern Contemporary': 'house',
 '2 Stories with Basement, Other (See Remarks)': 'house',
 '2 Stories with Basement, Traditional': 'house',
 '2 Stories with Basement, Tudor': 'house',
 '2 Stories with Basement, Victorian': 'house',
 '2 Story': 'house',
 '2 Story, Cape Cod': 'house',
 '2 Story, Contemporary': 'house',
 '2 Story, Craftsman': 'house',
 '2 Story, Modern': 'house',
 '2 Story, Northwestern Contemporary': 'house',
 '2 Story, Other (See Remarks)': 'house',
 '2 Story, Spanish/Southwestern': 'house',
 '2 Story, Traditional': 'house',
 '4-Plex': '',
 '5-9 Units': '',
 'Co-op': '',
 'Condominium (2 Levels)': 'condo',
 'Condominium (2 Levels), Contemporary': 'condo',
 'Condominium (2 Levels), Loft': 'condo',
 'Condominium (2 Levels), Modern': 'condo',
 'Condominium (2 Levels), Townhouse': 'condo',
 'Condominium (2 Levels), Traditional': 'condo',
 'Condominium (3+ Levels)': 'condo',
 'Condominium (3+ Levels), Contemporary': 'condo',
 'Condominium (3+ Levels), Modern': 'condo',
 'Condominium (3+ Levels), Townhouse': 'condo',
 'Condominium (Single Level)': 'condo',
 'Condominium (Single Level), Contemporary': 'condo',
 'Condominium (Single Level), Craftsman': 'condo',
 'Condominium (Single Level), Loft': 'condo',
 'Condominium (Single Level), Modern': 'condo',
 'Condominium (Single Level), Other (See Remarks)': 'condo',
 'Condominium (Single Level), Spanish/Southwestern': 'condo',
 'Condominium (Single Level), Studio': 'condo',
 'Condominium (Single Level), Traditional': 'condo',
 'Condominium (Single Level), Tudor': 'condo',
 'Duplex': '',
 'Houseboat, Cabin': '',
 'Houseboat, Contemporary': '',
 'Manufactured Double-Wide': '',
 'Multi-Family': '',
 'Multi-Level': 'house',
 'Multi-Level, Contemporary': 'house',
 'Multi-Level, Craftsman': 'house',
 'Multi-Level, Modern': 'house',
 'Multi-Level, Northwestern Contemporary': 'house',
 'Multi-Level, Other (See Remarks)': 'house',
 'Multi-Level, Traditional': 'house',
 'Multi-Level, Tudor': 'house',
 'Multi-Level, Victorian': 'house',
 'Residential (1+ Acre)': 'house',
 'Residential (<1 Acre)': 'house',
 'Single Family Residential': 'house',
 'Split-Entry': 'house',
 'Split-Entry, Contemporary': 'house',
 'Split-Entry, Craftsman': 'house',
 'Split-Entry, Modern': 'house',
 'Split-Entry, Northwestern Contemporary': 'house',
 'Split-Entry, Other (See Remarks)': 'house',
 'Split-Entry, Traditional': 'house',
 'Townhouse': 'townhouse',
 'Townhouse, Contemporary': 'townhouse',
 'Townhouse, Craftsman': 'townhouse',
 'Townhouse, Modern': 'townhouse',
 'Townhouse, Northwestern Contemporary': 'townhouse',
 'Townhouse, Townhouse': 'townhouse',
 'Townhouse, Traditional': 'townhouse',
 'Tri-Level': 'house',
 'Tri-Level, Cape Cod': 'house',
 'Tri-Level, Contemporary': 'house',
 'Tri-Level, Craftsman': 'house',
 'Tri-Level, Modern': 'house',
 'Tri-Level, Northwestern Contemporary': 'house',
 'Tri-Level, Other (See Remarks)': 'house',
 'Tri-Level, Traditional': 'house',
 'Triplex': ''}
basement_dict = {'': 0,
 '1 1/2 Story': 0,
 '1 1/2 Story with Basement': 1,
 '1 1/2 Story with Basement, Cape Cod': 1,
 '1 1/2 Story with Basement, Colonial': 1,
 '1 1/2 Story with Basement, Contemporary': 1,
 '1 1/2 Story with Basement, Craftsman': 1,
 '1 1/2 Story with Basement, Modern': 1,
 '1 1/2 Story with Basement, Northwestern Contemporary': 1,
 '1 1/2 Story with Basement, Traditional': 1,
 '1 1/2 Story with Basement, Tudor': 1,
 '1 1/2 Story, Cape Cod': 0,
 '1 1/2 Story, Contemporary': 0,
 '1 1/2 Story, Craftsman': 0,
 '1 1/2 Story, Northwestern Contemporary': 0,
 '1 1/2 Story, Other (See Remarks)': 0,
 '1 1/2 Story, Traditional': 0,
 '1 1/2 Story, Tudor': 0,
 '1 Story': 0,
 '1 Story with Basement': 1,
 '1 Story with Basement, Cape Cod': 1,
 '1 Story with Basement, Contemporary': 1,
 '1 Story with Basement, Craftsman': 1,
 '1 Story with Basement, Modern': 1,
 '1 Story with Basement, Northwestern Contemporary': 1,
 '1 Story with Basement, Other (See Remarks)': 1,
 '1 Story with Basement, Spanish/Southwestern': 1,
 '1 Story with Basement, Traditional': 1,
 '1 Story with Basement, Tudor': 1,
 '1 Story, Cabin': 0,
 '1 Story, Cape Cod': 0,
 '1 Story, Contemporary': 0,
 '1 Story, Craftsman': 0,
 '1 Story, Modern': 0,
 '1 Story, Northwestern Contemporary': 0,
 '1 Story, Other (See Remarks)': 0,
 '1 Story, Traditional': 0,
 '2 Stories with Basement': 1,
 '2 Stories with Basement, Cape Cod': 1,
 '2 Stories with Basement, Colonial': 1,
 '2 Stories with Basement, Contemporary': 1,
 '2 Stories with Basement, Craftsman': 1,
 '2 Stories with Basement, Modern': 1,
 '2 Stories with Basement, Northwestern Contemporary': 1,
 '2 Stories with Basement, Other (See Remarks)': 1,
 '2 Stories with Basement, Traditional': 1,
 '2 Stories with Basement, Tudor': 1,
 '2 Stories with Basement, Victorian': 1,
 '2 Story': 0,
 '2 Story, Cape Cod': 0,
 '2 Story, Contemporary': 0,
 '2 Story, Craftsman': 0,
 '2 Story, Modern': 0,
 '2 Story, Northwestern Contemporary': 0,
 '2 Story, Other (See Remarks)': 0,
 '2 Story, Spanish/Southwestern': 0,
 '2 Story, Traditional': 0,
 '4-Plex': 0,
 '5-9 Units': 0,
 'Co-op': 0,
 'Condominium (2 Levels)': 0,
 'Condominium (2 Levels), Contemporary': 0,
 'Condominium (2 Levels), Loft': 0,
 'Condominium (2 Levels), Modern': 0,
 'Condominium (2 Levels), Townhouse': 0,
 'Condominium (2 Levels), Traditional': 0,
 'Condominium (3+ Levels)': 0,
 'Condominium (3+ Levels), Contemporary': 0,
 'Condominium (3+ Levels), Modern': 0,
 'Condominium (3+ Levels), Townhouse': 0,
 'Condominium (Single Level)': 0,
 'Condominium (Single Level), Contemporary': 0,
 'Condominium (Single Level), Craftsman': 0,
 'Condominium (Single Level), Loft': 0,
 'Condominium (Single Level), Modern': 0,
 'Condominium (Single Level), Other (See Remarks)': 0,
 'Condominium (Single Level), Spanish/Southwestern': 0,
 'Condominium (Single Level), Studio': 0,
 'Condominium (Single Level), Traditional': 0,
 'Condominium (Single Level), Tudor': 0,
 'Duplex': 0,
 'Houseboat, Cabin': 0,
 'Houseboat, Contemporary': 0,
 'Manufactured Double-Wide': 0,
 'Multi-Family': 0,
 'Multi-Level': 0,
 'Multi-Level, Contemporary': 0,
 'Multi-Level, Craftsman': 0,
 'Multi-Level, Modern': 0,
 'Multi-Level, Northwestern Contemporary': 0,
 'Multi-Level, Other (See Remarks)': 0,
 'Multi-Level, Traditional': 0,
 'Multi-Level, Tudor': 0,
 'Multi-Level, Victorian': 0,
 'Residential (1+ Acre)': 0,
 'Residential (<1 Acre)': 0,
 'Single Family Residential': 0,
 'Split-Entry': 0,
 'Split-Entry, Contemporary': 0,
 'Split-Entry, Craftsman': 0,
 'Split-Entry, Modern': 0,
 'Split-Entry, Northwestern Contemporary': 0,
 'Split-Entry, Other (See Remarks)': 0,
 'Split-Entry, Traditional': 0,
 'Townhouse': 0,
 'Townhouse, Contemporary': 0,
 'Townhouse, Craftsman': 0,
 'Townhouse, Modern': 0,
 'Townhouse, Northwestern Contemporary': 0,
 'Townhouse, Townhouse': 0,
 'Townhouse, Traditional': 0,
 'Tri-Level': 0,
 'Tri-Level, Cape Cod': 0,
 'Tri-Level, Contemporary': 0,
 'Tri-Level, Craftsman': 0,
 'Tri-Level, Modern': 0,
 'Tri-Level, Northwestern Contemporary': 0,
 'Tri-Level, Other (See Remarks)': 0,
 'Tri-Level, Traditional': 0,
 'Triplex': 0}

def scrapes_to_df(listingpage_scrapes):
    data_dicts = []
    for i in range(len(listingpage_scrapes), 2):
        data_dicts.append(parse_sold_page(listingpage_scrapes[i], listingpage_scrapes[i+1]))
    listings_data = pd.DataFrame(data_dicts)
    # excess of caution over duplicates
    listings_data.drop_duplicates(keep = 'first', inplace = True, ignore_index = True)
    # converting data to consistent groups/types
    listings_data['type'] = listings_data['style'].map(type_dict)
    listings_data['price'] = pd.to_numeric(listings_data['price'], 'coerce')
    listings_data['beds'] = pd.to_numeric(listings_data['beds'], 'coerce')
    listings_data['baths'] = pd.to_numeric(listings_data['baths'], 'coerce')
    listings_data['lot'] = pd.to_numeric(listings_data['lot'], 'coerce')
    listings_data['brok'] = pd.to_numeric(listings_data['brok'], 'coerce')
    listings_data['age'] = pd.to_datetime(listings_data['age'], 'coerce')
    listings_data['sold'] = pd.to_datetime(listings_data['sold'], 'coerce')
    listings_data['size'] = pd.to_numeric(listings_data['size'], 'coerce')
    # dropping unnecessary columns
    listings_data.drop(columns = ['address', 'comm', 'style', 'status', 'url', 'park'], inplace = True)
    # converting lot size mistakes where realtors entered acres instead of sqft
    listings_data.loc[(listings_data.type == 'house') & (listings_data.lot < 1), 'lot'] = listings_data['lot']*43560
    # dummies for 3 types: condo, townhouse, house
    listings_data = pd.get_dummies(listings_data, columns = ['type'], drop_first = True)
    return listings_data