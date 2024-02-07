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


def main() -> int:
    args = parse_args()

    url_df = get_cik_url_df(args.input_file)

    print(f'Found {url_df.shape[0]:,} companies to pull filings for, as listed in {args.input_file}')

    count = 0
    total = url_df.shape[0]
    print(f'=== Showing {total:,} 10K filings ===')
    for ind, row in url_df.iterrows():
        count += 1
        print(f'--- Showing {count:,} of {total:,} 10K filings for {toList(row.names)}')
        print(f"""
              cik: {row.cik} 
              cusip6: {row.cusip6}
              form10KUrls: {row.form10KUrls}
              names: {toList(row.names)}
              cusip: {toList(row.cusip)}
              """
        )
    return 0

def stripSingleQuotesAndSpaces(s: str) -> str:
    return s.strip("' ")

def toList(asStr: str) -> List[str]:
    return list(map(stripSingleQuotesAndSpaces, asStr.strip("{}").split(",")))

def get_cik_url_df(formatted_data_path: str) -> DataFrame:
    res = pd.read_csv(formatted_data_path)
    res.cik = res.cik.astype(str)
    return res


def parse_args():
    parser = argparse.ArgumentParser(
        description='download 10k filings and pull text from sections 1,1A, 7, and 7A from 10-ks and save as json',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-i', '--input-file', required=False, default='data/cik-10k-urls.csv',
                        help='Formatted File with 10K Urls and ciks')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    raise SystemExit(main())
