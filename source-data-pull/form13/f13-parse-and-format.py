import argparse
from datetime import datetime
from typing import List, Dict

import pandas as pd
import os
import re
import xmltodict

FILING_MANAGER_NAME_COL = 'managerName'
FILING_MANAGER_CIK_COL = 'managerCik'
REPORT_PERIOD_COL = 'reportCalendarOrQuarter'
COMPANY_CUSIP_COL = 'cusip'
COMPANY_NAME_COL = 'companyName'
SOURCE_ID_COL = 'sourceFilingId'
VALUE_COL = 'value'
SHARES_COL = 'shares'


def main() -> int:
    args = parse_args()
    filings_df, failures = parse_from_dir(args.input_directory)
    stg_df = aggregate_data(filings_df)
    if args.top_periods is not None:
        stg_df = filter_data(stg_df, args.top_periods)
    stg_df.to_csv(args.output_file, index=False)
    print(f'===== Had {len(failures)} failed file parsings ====')
    for failure in failures:
        print(failure)
    return 0


def parse_args():
    parser = argparse.ArgumentParser(description='format raw form13s from EDGAR SEC into a csv for graph loading',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-i', '--input-directory', required=False, default='data/form13-raw/',
                        help='Directory containing raw EDGAR files')
    parser.add_argument('-o', '--output-file', required=False, default='data/form13.csv',
                        help='Local path + file name to write formatted csv too')
    parser.add_argument('-p', '--top-periods', required=False, type=int,
                        help='Only include data from `n` most recent report quarters')
    args = parser.parse_args()
    return args


# function to strip namespaces post xmltodict transformation
def strip_ns(x):
    if isinstance(x, dict):
        x_striped = dict()
        for k, v in x.items():
            x_striped[k.split(':')[-1]] = strip_ns(v)
    elif isinstance(x, list):
        x_striped = [strip_ns(i) for i in x]
    else:
        x_striped = x
    return x_striped


def extract_submission_info(contents: str) -> str:
    xml = contents[1].split('</XML>')[0].strip()
    return strip_ns(xmltodict.parse(xml))['edgarSubmission']


def extract_investment_info(contents: str) -> str:
    xml = contents[2].split('</XML>')[0].strip()
    return strip_ns(xmltodict.parse(xml))['informationTable']['infoTable']


def filter_and_format(info_tables: str, manager_cik: str, manager_name: str,
                      report_period: datetime.date) -> List[Dict]:
    res = []
    if isinstance(info_tables, dict):
        info_tables = [info_tables]
    for info_table in info_tables:
        # Skip none to report incidences
        if info_table['cusip'] == '000000000':
            pass
        # Only want stock holdings, not options
        if info_table['shrsOrPrnAmt']['sshPrnamtType'] != 'SH':
            pass
        # Only want holdings over $10m
        elif (float(info_table['value']) * 1000) < 10000000:
            pass
        # Only want common stock
        elif info_table['titleOfClass'] != 'COM':
            pass
        else:
            res.append({FILING_MANAGER_CIK_COL: manager_cik,
                        FILING_MANAGER_NAME_COL: manager_name,
                        REPORT_PERIOD_COL: report_period,
                        COMPANY_CUSIP_COL: info_table['cusip'],
                        COMPANY_NAME_COL: info_table['nameOfIssuer'],
                        VALUE_COL: info_table['value'].replace(' ', '') + '000',
                        SHARES_COL: info_table['shrsOrPrnAmt']['sshPrnamt']})
    return res


def extract_dicts(txt: str) -> List[Dict]:
    contents = txt.split('<XML>')
    submt_dict = extract_submission_info(contents)
    mng_cik = submt_dict['headerData']['filerInfo']['filer']['credentials']['cik']
    mng_name = submt_dict['formData']['coverPage']['filingManager']['name']
    report_period = submt_dict['formData']['coverPage']['reportCalendarOrQuarter']
    info_dict = extract_investment_info(contents)
    return filter_and_format(info_dict, mng_cik, mng_name, report_period)


def parse_from_dir(directory_path: str):
    # Go through all files and concatenate to dataframe
    print(f'=== Begin Parsing from {directory_path} ===')
    filing_dfs = []
    failures = []
    for file_name in os.listdir(directory_path):
        if file_name.endswith('.txt'):
            print(f'parsing {file_name}')
            file_path = os.path.join(directory_path, file_name)
            try:
                with open(file_path, 'r') as file:
                    filing = extract_dicts(file.read())
                    tmp_filing_df = pd.DataFrame(filing)
                    tmp_filing_df[SOURCE_ID_COL] = file_name
                    filing_dfs.append(tmp_filing_df)
            except Exception as e:
                print(e)
                failures.append(file_name) 
    filing_df = pd.concat(filing_dfs, ignore_index=True)
    filing_df[REPORT_PERIOD_COL] = pd.to_datetime(filing_df[REPORT_PERIOD_COL]).dt.date
    filing_df[VALUE_COL] = filing_df[VALUE_COL].astype(float)
    filing_df[SHARES_COL] = filing_df[SHARES_COL].astype(int)
    return filing_df, failures


# This data contains duplicates where an asset is reported more than once for the same filing manager within the same
# report calendar/quarter.
# See for example https://www.sec.gov/Archives/edgar/data/1962636/000139834423009400/0001398344-23-009400.txt
# for our intents and purposes we will sum over values and shares to aggregate the duplicates out
def aggregate_data(filings_df: pd.DataFrame) -> pd.DataFrame:
    print(f'=== Aggregating Parsed Data ===')
    return filings_df.groupby([FILING_MANAGER_NAME_COL, REPORT_PERIOD_COL, COMPANY_CUSIP_COL]) \
        .agg({VALUE_COL: sum, SHARES_COL: sum, SOURCE_ID_COL: max, COMPANY_NAME_COL: 'first'}) \
        .reset_index()


def filter_data(filings_df: pd.DataFrame, top_n_periods: int) -> pd.DataFrame:
    print(f'=== Filtering Data ===')
    periods_df = filings_df[[REPORT_PERIOD_COL, VALUE_COL]] \
        .groupby(REPORT_PERIOD_COL).count().reset_index().sort_values(REPORT_PERIOD_COL)
    num_periods = min(periods_df.shape[0], top_n_periods)
    top_periods = periods_df[REPORT_PERIOD_COL][-num_periods:].tolist()
    return filings_df[filings_df[REPORT_PERIOD_COL].isin(top_periods)]


if __name__ == "__main__":
    raise SystemExit(main())
