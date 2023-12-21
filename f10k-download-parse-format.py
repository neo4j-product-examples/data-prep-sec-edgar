import argparse
import http.client
import io
import json
from typing import Dict, List
import datetime
import os
import re
import pandas as pd
from bs4 import BeautifulSoup
from pandas import DataFrame
from secedgar import filings, FilingType


def main() -> int:
    args = parse_args()
    temp_dir = args.temp_directory
    output_dir = args.output_directory
    user_email = args.user_email
    user_name = args.user_name

    print('Pulling urls...')
    url_df = get_cik_url_df(args.input_file)

    print(f'Found {url_df.shape[0]:,} companies to pull filings for')
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    count = 0
    total = url_df.shape[0]
    print(f'=== Downloading {total:,} 10K filings ===')
    for ind, row in url_df.iterrows():
        count += 1
        print(f'--- Downloading {count:,} of {total:,} 10K filings for {row.name}')
        raw_file_path, file_name = download_filing(row.form10KUrls, f'{user_name} {user_email}', temp_dir)
        if len(raw_file_path) > 0:
            output_file_path = os.path.join(output_dir, file_name)
            try:
                load_parse_save(raw_file_path, output_file_path, row.cik)
                os.remove(raw_file_path)
            except Exception as e:
                print(e)
    return 0


def get_cik_url_df(formatted_data_path: str) -> DataFrame:
    res = pd.read_csv(formatted_data_path)
    res.cik = res.cik.astype(str)
    return res


def parse_args():
    parser = argparse.ArgumentParser(
        description='download 10k filings and pull text from sections 1,1A, 7, and 7A from 10-ks and save as json',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-t', '--temp-directory', required=False, default='data/temp-10k',
                        help='Directory to temporarily store raw SEC 10K files')
    parser.add_argument('-o', '--output-directory', required=False, default='data/form10k-clean',
                        help='Local path to write formatted text to')
    parser.add_argument('-un', '--user-name', default='Neo4j',
                        help='Name to use for user agent in SEC EDGAR calls')
    parser.add_argument('-ue', '--user-email', default='sales@neo4j.com',
                        help='Email address to use for user agent in SEC EDGAR calls')
    parser.add_argument('-i', '--input-file', required=False, default='data/cik-10k-urls.csv',
                        help='Formatted File with 10K Urls and ciks')
    args = parser.parse_args()
    return args


def download_filing(url: str, user_agent: str, temp_dir: str) -> tuple:
    conn = http.client.HTTPSConnection('www.sec.gov')
    conn.request('GET', url, headers={'User-Agent': user_agent})
    response = conn.getresponse()
    data = response.read()
    conn.close()

    if response.status == 200 and response.reason == 'OK':
        text = data.decode('utf-8')
        file = io.StringIO(text)
        contents = file.read()
        file.close()
        file_id = url[url.rindex('/') + 1:]
        file_path = os.path.join(temp_dir, 'raw_' + file_id)
        with open(file_path, 'w') as file:
            file.write(contents)
        return file_path, file_id
    else:
        print('Download failed for form13 file.')
        print(response.status, response.reason)
        return '', ''


def extract_10_k(txt: str) -> str:
    # Regex to find <DOCUMENT> tags
    doc_start_pattern = re.compile(r'<DOCUMENT>')
    doc_end_pattern = re.compile(r'</DOCUMENT>')
    # Regex to find <TYPE> tag proceeding any characters, terminating at new line
    type_pattern = re.compile(r'<TYPE>[^\n]+')
    # Create 3 lists with the span idices for each regex

    # There are many <Document> Tags in this text file, each as specific exhibit like 10-K, EX-10.17 etc
    # First filter will give us document tag start <end> and document tag end's <start>
    # We will use this to later grab content in between these tags
    doc_start_is = [x.end() for x in doc_start_pattern.finditer(txt)]
    doc_end_is = [x.start() for x in doc_end_pattern.finditer(txt)]

    # Type filter is interesting, it looks for <TYPE> with Not flag as new line, ie terminare there, with + sign
    # to look for any char afterwards until new line \n. This will give us <TYPE> followed Section Name like '10-K'
    # Once we have have this, it returns String Array, below line will with find content after <TYPE> ie, '10-K'
    # as section names
    doc_types = [x[len('<TYPE>'):] for x in type_pattern.findall(txt)]
    # Create a loop to go through each section type and save only the 10-K section in the dictionary
    # there is just one 10-K section
    for doc_type, doc_start, doc_end in zip(doc_types, doc_start_is, doc_end_is):
        if doc_type == '10-K':
            return txt[doc_start:doc_end]


# Extract text using position dataframe and beautiful soup
def beautify_text(txt: str) -> str:
    stg_txt = BeautifulSoup(txt, 'lxml')
    return stg_txt.get_text('\n')


def extract_text(row: pd.Series, txt: str):
    section_txt = txt[row.start:row.sectionEnd].replace('Error! Bookmark not defined.', '')
    return beautify_text(section_txt)


def extract_section_text(doc: str) -> Dict[str, str]:
    # Write the regex
    regex = re.compile(r'(>(Item|ITEM)(\s|&#160;|&nbsp;)(1A|1B|1\.|7A|7|8)\.{0,1})|(ITEM\s(1A|1B|1\.|7A|7|8))')
    # Use finditer to math the regex
    matches = regex.finditer(doc)
    # Write a for loop to print the matches
    # Create the dataframe
    item_df = pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])
    item_df.columns = ['item', 'start', 'end']
    item_df['item'] = item_df.item.str.lower()

    item_df.replace('&#160;', ' ', regex=True, inplace=True)
    item_df.replace('&nbsp;', ' ', regex=True, inplace=True)
    item_df.replace(' ', '', regex=True, inplace=True)
    item_df.replace('\.', '', regex=True, inplace=True)
    item_df.replace('>', '', regex=True, inplace=True)

    all_pos_df = item_df.sort_values('start', ascending=True).drop_duplicates(subset=['item'], keep='last').set_index(
        'item')
    # Add section end using start of next section
    all_pos_df['sectionEnd'] = all_pos_df.start.iloc[1:].tolist() + [len(doc)]
    # filter to just the sections we care about
    pos_df = all_pos_df.loc[['item1', 'item1a', 'item7', 'item7a'], :]
    res = dict()
    for i, row in pos_df.iterrows():
        res[i] = extract_text(row, doc)
    return res


def load_parse_save(input_file_path: str, output_file_path: str, cik: str):
    with open(input_file_path, 'r') as file:
        raw_txt = file.read()
    print('Extracting 10-K')
    doc = extract_10_k(raw_txt)
    print('Parsing relevant sections')
    cleaned_json_txt = extract_section_text(doc)
    cleaned_json_txt['cik'] = cik
    print('Writing clean text to json')
    with open(output_file_path, 'w') as json_file:
        json.dump(cleaned_json_txt, json_file, indent=4)


if __name__ == "__main__":
    raise SystemExit(main())
