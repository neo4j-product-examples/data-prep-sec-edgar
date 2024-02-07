import argparse
from typing import Dict, List
import datetime
import os
import re
import pandas as pd
from bs4 import BeautifulSoup
from pandas import DataFrame


def main() -> int:
    args = parse_args()

    form10_df = get_form10_df(args.left_10k)
    form13_df = get_form13_df(args.right_13)

    print(f'Found {form10_df.shape[0]:,} form 10k listings in {args.left_10k}')
    print(f'Found {form13_df.shape[0]:,} form 13 listings in {args.right_13}')

    # Left join
    leftjoin = pd.merge(form10_df, form13_df,  
                   on='cusip6',  
                   how='left') 

    print(f'Joined {leftjoin.shape[0]:,} rows in left join of form 10k and form 13 data')
    print(f'\tColumn names: {leftjoin.columns}')

    filtered_leftjoin = leftjoin[leftjoin['managerCik'].notna()]
    print(f'Filtered {filtered_leftjoin.shape[0]:,} by empty "managerCik" rows in left join of form 10k and form 13 data')

    deduped_leftjoin = filtered_leftjoin.drop_duplicates(subset=['cusip6'])
    print(f'Deduped {deduped_leftjoin.shape[0]:,} rows in filtered left join of form 10k and form 13 data')
    print(f'\t{filtered_leftjoin.shape[0] - deduped_leftjoin.shape[0]:,} duplicates removed')

    reduced_form10 = deduped_leftjoin[["cusip6","cik","names","cusip_x","form10KUrls"]]
    reduced_form10.rename(columns={"cusip_x": "cusip"}, inplace=True)
    print(reduced_form10.head(5)[["cusip6","cik","names","cusip","form10KUrls"]])

    reduced_form10.to_csv(args.left_10k + '.joined.csv', index=False)

    # Right join

    rightjoin = pd.merge(form10_df, form13_df,  
                   on='cusip6',  
                   how='right') 

    print(f'Joined {rightjoin.shape[0]:,} rows in right join of form 10k and form 13 data')
    print(f'\tColumn names: {rightjoin.columns}')

    filtered_rightjoin = rightjoin[rightjoin['cik'].notna()]
    print(f'Filtered {filtered_rightjoin.shape[0]:,} by empty "cik" rows in right join of form 10k and form 13 data')

    deduped_rightjoin = filtered_rightjoin.drop_duplicates(subset=['managerCik', 'cik'])
    print(f'Deduped {deduped_rightjoin.shape[0]:,} rows in filtered right join of form 10k and form 13 data')
    print(f'\t{deduped_rightjoin.shape[0] - deduped_rightjoin.shape[0]:,} duplicates removed')

    reduced_form13 = deduped_rightjoin[["source","managerCik","managerAddress","managerName","reportCalendarOrQuarter","cusip6","cusip_y","companyName","value","shares"]]
    reduced_form13.rename(columns={"cusip_y": "cusip"}, inplace=True)
    print(reduced_form13.head(5)[["source","managerCik","managerAddress","managerName","reportCalendarOrQuarter","cusip6","cusip","companyName","value","shares"]])

    reduced_form13.to_csv(args.right_13 + '.joined.csv', index=False)

    return 0

def get_form10_df(formatted_data_path: str) -> DataFrame:
    res = pd.read_csv(formatted_data_path)
    # res.cik = res.cik.astype(str)
    return res

def get_form13_df(formatted_data_path: str) -> DataFrame:
    res = pd.read_csv(formatted_data_path)
    # res.cik = res.cik.astype(str)
    return res

def parse_args():
    parser = argparse.ArgumentParser(
        description='find matching form 10ks and 13s for a subset of companies in the 10k data set and save reduced csvs',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-l', '--left-10k', required=False, default='form10k/data/cik-10k-urls.csv',
                        help='Formatted file with 10K Urls and ciks')
    
    parser.add_argument('-r', '--right-13', required=False, default='form13/data/form13.csv',
                        help='Formatted file with form 13 data')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    raise SystemExit(main())
