import datetime
import http.client
import io
import os
import math
import csv
import argparse


def main() -> int:
    args = parse_args()
    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d').date()
    date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d').date()
    output_dir = args.output_directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    while date >= start_date:
        print(date)
        download_date(date, output_dir)
        date = date - datetime.timedelta(days=1)
    return 0


def parse_args():
    parser = argparse.ArgumentParser(description='Download raw form13s from EDGAR SEC',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-s', '--start-date', default='2022-12-31', help='Start date in the format yyyy-mm-dd')
    parser.add_argument('-e', '--end-date', default='2023-12-22', help='End date in the format yyyy-mm-dd')
    parser.add_argument('-o', '--output-directory', default='data/form13-raw/',
                        help='Local directory to write forms to')
    args = parser.parse_args()
    return args


def download_date(date, output_dir):
    form13_paths = get_form13_urls(date)
    # Download each Form 13
    print('We have ' + str(len(form13_paths)) + ' Form 13 URLs for the date ' + str(date))
    for path in form13_paths:
        try:
            filings = download_form13(path)
            with open(os.path.join(output_dir, path.replace('/', '_')), 'w') as file:
                file.write(filings)
        except Exception as e:
            print(e)


def get_form13_urls(date):
    print('Composing the URL of the master file...')
    year = str(date.year)
    quarter = 'QTR' + str(math.ceil(date.month / 3))
    date = date.strftime('%Y%m%d')
    path = '/Archives/edgar/daily-index/' + year + '/' + quarter + '/master.' + date + '.idx'
    url = 'https://www.sec.gov' + path
    print('The URL of the master file is ' + url)

    print('Downloading the master file...')
    conn = http.client.HTTPSConnection('www.sec.gov')
    conn.request('GET', path, headers={'User-Agent': 'Neo4j Ben.Lackey@Neo4j.com'})
    response = conn.getresponse()
    print(response.status, response.reason)
    data = response.read()
    conn.close()

    if response.status == 200 and response.reason == 'OK':
        text = data.decode('windows-1252')
        form4_paths = parse_master_file(text)
        return form4_paths
    else:
        print('Download failed for master file.')
        return []


def parse_master_file(text):
    print('Parsing the master file...')
    form4_paths = []
    file = io.StringIO(text)
    reader = csv.reader(file, delimiter='|')
    for row in reader:
        if len(row) != 5:
            # This is a header
            pass
        elif row[2] == '13F-HR':
            # This is a Form 13
            form4_paths.append('/Archives/' + row[4])

    return form4_paths


def download_form13(path):
    conn = http.client.HTTPSConnection('www.sec.gov')
    conn.request('GET', path, headers={'User-Agent': 'Neo4j sales@neo4j.com'})
    response = conn.getresponse()
    data = response.read()
    conn.close()

    if response.status == 200 and response.reason == 'OK':
        print('http://sec.gov' + path)
        text = data.decode('utf-8')
        file = io.StringIO(text)
        contents = file.read()
        file.close()
        return contents
    else:
        print('Download failed for form13 file.')
        print(response.status, response.reason)
        return []


if __name__ == "__main__":
    raise SystemExit(main())
