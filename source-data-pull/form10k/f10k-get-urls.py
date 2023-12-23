import argparse
import http.client
import json
from typing import Dict, List
import datetime
import pandas as pd
from pandas import DataFrame

BASE_URL = 'https://www.sec.gov'


def main() -> int:
    args = parse_args()
    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d').date()
    user_email = args.user_email
    user_name = args.user_name

    print('Pulling company list...')
    cik_df = get_cik_df(args.input_file)

    print(f'Found {cik_df.shape[0]:,} companies to pull filings for')

    urls_list = []

    counter = 0
    for ind, row in cik_df.iterrows():
        counter += 1
        print(f'pulling 10k urls for cik: {row.cik}, {counter} of {cik_df.shape[0]} ciks')
        urls = get_urls(row.cik, start_date, end_date, f'{user_name} {user_email}')
        print(f'{row.cik}: {urls}')
        urls_list.append(urls)

    cik_df['form10KUrls'] = urls_list
    urls_df = cik_df[cik_df.form10KUrls.map(len) > 0].explode(column='form10KUrls')
    urls_df.to_csv(args.output_file, index=False)
    return 0


def parse_args():
    parser = argparse.ArgumentParser(
        description='download 10k filings and pull text from sections 1,1A, 7, and 7A from 10-ks and save as json',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-s', '--start-date', default='2022-01-01', help='Start date in the format yyyy-mm-dd')
    parser.add_argument('-e', '--end-date', default='2023-01-01', help='End date in the format yyyy-mm-dd')
    parser.add_argument('-i', '--input-file', required=False, default='./cik-sample-mapping.csv',
                        help='Formatted cik dataframe to pull cik ids from')
    parser.add_argument('-o', '--output-file', required=False, default='data/cik-10k-urls.csv',
                        help='Local path to write cik dataframe with urls too.')
    parser.add_argument('-un', '--user-name', default='Neo4j',
                        help='Name to use for user agent in SEC EDGAR calls')
    parser.add_argument('-ue', '--user-email', default='sales@neo4j.com',
                        help='Email address to use for user agent in SEC EDGAR calls')
    args = parser.parse_args()
    return args


def get_urls(cik: str, start_date: datetime.date, end_date: datetime.date, user_agent: str):
    filing_accessors = get_filing_accessors(cik, start_date, end_date, user_agent)
    return [format_url(cik, f) for f in filing_accessors]


def get_filing_accessors(cik: str, start_date: datetime.date, end_date: datetime.date, user_agent: str) -> List[str]:
    history = get_filing_history(cik, user_agent)
    history_df = pd.DataFrame.from_dict(history['filings']['recent'])
    history_df.filingDate = pd.to_datetime(history_df.filingDate).dt.date
    filtered_df = history_df[(history_df.filingDate <= end_date) &
                             (history_df.filingDate >= start_date) &
                             (history_df.form == '10-K')]
    return filtered_df.accessionNumber.tolist()


def get_filing_history(cik: str, user_agent: str) -> Dict:
    url = f'https://data.sec.gov//submissions/CIK{int(cik):010d}.json'
    print(f'Downloading filing history for cik: {cik}')
    conn = http.client.HTTPSConnection('www.sec.gov')
    conn.request('GET', url, headers={'User-Agent': user_agent})
    response = conn.getresponse()
    print(response.status, response.reason)
    data = response.read()
    conn.close()

    if response.status == 200 and response.reason == 'OK':
        res = data.decode('utf-8')
        return json.loads(res)
    else:
        print(f'Download failed for cik: {cik} filings.')
    return dict()


def format_url(cik: str, filing_accessor: str):
    return BASE_URL + f'/Archives/edgar/data/{int(cik)}/{filing_accessor.replace("-", "")}/{filing_accessor}.txt'


def get_cik_df(formatted_data_path: str) -> DataFrame:
    res = pd.read_csv(formatted_data_path)
    res.cik = res.cik.astype(str)
    return res


if __name__ == "__main__":
    raise SystemExit(main())
