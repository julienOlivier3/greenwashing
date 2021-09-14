# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.12.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Setup corporate URLs

import pandas as pd
import re
import requests

df_url = pd.read_csv(r"Q:\Meine Bibliotheken\Research\01_Promotion\05_Ideas\06_GreenFinance\05_Data\mup2afid_urls_sample.txt", sep = "\t")
df_mup = pd.read_csv(r"Q:\Meine Bibliotheken\Research\01_Promotion\05_Ideas\06_GreenFinance\05_Data\mup2afid.txt", sep="\t")

# Corrections:

df_url.loc[(df_url.crefo==3270030744) & (df_url.year.isin([2019, 2020])),'url'] = "www.thomas-gruppe.de"


# Make urls wildcarded.

def wildcarding(url):
    domain_end = r'(?:com|de|rwe|lu|SH|ru|pt|hu|ei|ne|hamburg|glass|cz|Beer|systems|international|mobi|me|bike|vom|solutions|restaurant|works|us|net|eu|fr|heise-service|biz|info|berlin|cc|tv|be|ch|gmbh|tech|site|es|aero|org|shop|online|world|ms|digital|pro|ag|sh|tc|ev|energy|gov|nl|nrw|om|ps|team|ac|io|d|uk|it|media|hk|industries|co|hr|is|group|ch|at|gs|COM|ws|st|ie|ocm|si|DE|ro|tb|canon|global|cn|DE|audio|som|pl|koeln|care|NRW|lighting|tube|to|li|bio|se|bz)'
    try:
        url_wildcarded = re.search(r'\..{1,}\.' + domain_end, url).group(0)[1:] + '/*'
    except AttributeError:
        try:
            url_wildcarded = re.search(r'.{1,}\.' + domain_end, url).group(0) + '/*'
        except:
            try:
                url_wildcarded = re.search(r'.{1,}-(?:com|de)', url).group(0)[1:] + '/*'
            except:
                url_wildcarded = url + '/*'
    return url_wildcarded


# Reset index for loop
df_url = df_url.reset_index(drop=True)

# Check if any url cannot be wildcarded
ind = []
for i in range(df_url.shape[0]):
    if pd.notna(df_url.url[i]):
        try:
            wildcarding(df_url.url[i])
        except:
            ind.append(i)
        

# All urls are "clean" if this cell equals 0
len(ind)

# Otherwise which cells could not be wildcarded
df_url.iloc[ind,:].drop_duplicates(subset=['url'])

# Conduct the wildcarding
df_url.loc[df_url.url.notnull(), 'url'] = df_url.loc[df_url.url.notnull(), 'url'].apply(lambda x: wildcarding(x))

df_url.sample(12)

# # Access web archives 

# We now define some parameters for accessing CC and IA and also for storing the retrieved data in WARC files both via `cdx_toolkit`.

import cdx_toolkit
from tqdm import tqdm

client = cdx_toolkit.CDXFetcher(source='ia')         # define client for fetching data from source (ia: Internet Archive, cc: Common Crawl)
limit = 1000                                         # define maximum number of captures that is suppossed to be retrieved for each year-url from the respective archive
crefos = list(df_url.crefo.drop_duplicates().values) # create list of unique company IDs (crefos) for which panel dataset of corporate website content is created
len(crefos)

# A 'warcinfo' record describes the records that follow it, up through end of file, end of input, or until next 'warcinfo' record.
# Typically, this appears once and at the beginning of a WARC file. 
# For a web archive, it often contains information about the web crawl which generated the following records.
warcinfo = {
    'software': 'pypi_cdx_toolkit iter-and-warc',
    'isPartOf': 'GREENWASHING-SAMPLE-IA',
    'description': 'warc extraction',
    'format': 'WARC file version 1.0',
}

# Define directory where WARC files shall be stored.

import os
os.getcwd()

os.chdir(r'J:\Daten\jax\01_Data\02_Webdata')
os.getcwd()

# + jupyter={"outputs_hidden": true} tags=[]
# %%time
for year in range(2011, 2021):
    writer = cdx_toolkit.warc.CDXToolkitWARCWriter(
        prefix='afid',           # first part of .warc file where warc records will be stored
        subprefix=str(year),     # second part of .warc file where warc records will be stored
        info=warcinfo,           
        size=1000000000,         # once the .warc file exceeds 1 GB of size a new .warc file will be created for succeeding records
        gzip=True)            
    
    for i, crefo in enumerate([2150024763, 2310264952]):
        row = df_url.loc[(df_url.crefo==crefo) & (df_url.year == year),:].squeeze(axis=0)
        
        if pd.isna(row.url):
            pass                 # pass if firm has not existed in the respective year (which refers to the url entry is NA in the respective year)
        else:
            print(str(i), '- Crefo: ', str(row.crefo))
            try:
                # Create iterator object including all captures in of the url in the given year
                capture = client.iter(row.url, from_ts=str(row.year), to=str(row.year), limit=limit, verbose='v', collapse='urlkey', filter=['status:200', 'mime:text/html'])
                
                # If corporate website has not been captured write crefo to "not_captured-YEAR.txt" file
                if len(capture.captures) == 0:
                    with open('not_captured-' + str(year) + '.txt', 'a') as crefo_out:
                            crefo_out.write("%s\n" % str(crefo))
                
                # Else iterate over all captures and save header and body information in "ID.warc.gz" file
                else:
                    with open('captured-' + str(year) + '.txt', 'a') as crefo_out:
                            crefo_out.write("%s\n" % str(crefo))
                    for obj in tqdm(capture):
                        url = obj['url']
                        status = obj['status']
                        timestamp = obj['timestamp']

                        try:
                            record = obj.fetch_warc_record()
                            # Save crefo into header information of the WARC record so it is not lost in the WARC file
                            record.rec_headers['crefo'] = str(row.crefo)
                            writer.write_record(record)
                        
                        # Single captures can run into errors:
                        # Except RuntimeError
                        except RuntimeError:
                            print('Skipping capture for RuntimeError 404: %s %s', url, timestamp)
                            continue                
                    
                        # Except encoding error that typically arises if no content found on webpage
                        except UnicodeEncodeError:
                            print('Skipping capture for UnicodeEncodeError: %s %s', url, timestamp)
                            continue
            
            # URLs can also run into errors
            # Except HTTPError if URL has been excluded from the Wayback Machine
            except requests.HTTPError:
                print('Skipping url for HTTPError 403: %s', crefo)
                with open('http_error-' + str(year) + '.txt', 'a') as crefo_out:
                            crefo_out.write("%s\n" % str(crefo))
                continue
# -

# Execute this cell, if you you want to close the CDXToolkitWARCWriter 
writer.fd.close()













client = cdx_toolkit.CDXFetcher(source='ia')

pattern = r'^.*(sustain|nachhaltig).*$'

df_url.shape

limit = 1000

# Number of firms
crefos = list(df_url.crefo.drop_duplicates().values)
len(df_url.crefo.drop_duplicates())

# + active=""
# # Write as .txt file (deprecated)
# for c in tqdm(crefos):
#     df = pd.DataFrame()
#     df_sub = df_url.loc[df_url.crefo==c,:]
#     for y in range(2010, 2021):
#         row = df_sub.loc[df_sub.year == y,:].squeeze(axis=0)
#         if pd.isna(row.url):
#             pass
#         else:
#             requests = list(client.iter(row.url, from_ts=str(row.year), to=str(row.year), limit=limit, verbose='v', collapse='urlkey', filter=['status:200', 'mime:text/html']))
#             df_meta = pd.DataFrame([r.data for r in requests])
#             df_text = pd.DataFrame([r.text for r in requests], columns=['html'])
#             df_temp = pd.concat([df_meta, df_text], axis=1)
#             if not df_temp.empty:
#                 df_temp['year'] = df_temp.timestamp.apply(lambda x: str(x)[0:4])
#                 df_temp['id'] = c
#                 df_temp = df_temp[['id', 'year', 'timestamp', 'urlkey', 'url', 'digest', 'html']]
#         
#                 df = df.append(df_temp, ignore_index=True)
#     df.to_csv('J:\\Daten\\jax\\01_Data\\02_Webdata\\webpanel_' + str(c) + '.txt', sep='\t')
#         
# -

# Write as .warc file

warcinfo = {
    'software': 'pypi_cdx_toolkit iter-and-warc',
    'isPartOf': 'GREENWASHING-SAMPLE-IA',
    'description': 'warc extraction',
    'format': 'WARC file version 1.0',
}

writer = get_writer(prefix='sample', subprefix='2010', info=warcinfo)

writer.segment

crefos.index(7010003235)

crefos[68]

# +
#with open(r'J:\Daten\jax\01_Data\02_Webdata\sample_2010.warc.gz', 'wb') as output:

for c in [crefos[72]]:
    df_sub = df_url.loc[df_url.crefo==c,:]
    for y in range(2010, 2011):
        row = df_sub.loc[df_sub.year == y,:].squeeze(axis=0)
        if pd.isna(row.url):
            pass
        else:
            print('Crefo: ', str(row.crefo))
            for obj in tqdm(client.iter(row.url, from_ts=str(row.year), to=str(row.year), limit=limit, verbose='v', collapse='urlkey', filter=['status:200', 'mime:text/html'])):
                url = obj['url']
                status = obj['status']
                timestamp = obj['timestamp']

                try:
                    record = obj.fetch_warc_record()
                    record.rec_headers['crefo'] = str(row.crefo)
                except RuntimeError:
                    print('Skipping capture for RuntimeError 404: %s %s', url, timestamp)
                    continue
                writer.write_record(record)
                
        



# +
from warcio import WARCWriter
import logging
LOGGER = logging.getLogger(__name__)

class CDXToolkitWARCWriter:
    def __init__(self, prefix, subprefix, info, size=1000000000, gzip=True, warc_version=None):
        self.prefix = prefix
        self.subprefix = subprefix
        self.info = info
        self.size = size
        self.gzip = gzip
        self.warc_version = warc_version
        self.segment = 0
        self.writer = None

    def write_record(self, *args, **kwargs):
        if self.writer is None:
            if self.warc_version is None:
                # opportunity to intuit warc version here
                self.warc_version = '1.0'
            if self.warc_version != '1.0':
                LOGGER.error('WARC versions other than 1.0 are not correctly supported yet')
                # ...because fake_wb_warc always generates 1.0
            # should we also check the warcinfo record to make sure it's got a matching warc_version inside?
            self._start_new_warc()

        self.writer.write_record(*args, **kwargs)

        fsize = os.fstat(self.fd.fileno()).st_size
        if fsize > self.size:
            self.fd.close()
            self.writer = None
            self.segment += 1

    def _unique_warc_filename(self):
        #while True:
        name = self.prefix + '-'
        if self.subprefix is not None:
            name += self.subprefix + '-'
        name += '{:06d}'.format(self.segment) + '.extracted.warc'
        if self.gzip:
            name += '.gz'
            #if os.path.exists(name):
            #    self.segment += 1
        #else:
        #    break
        return name

    def _start_new_warc(self):
        self.filename = self._unique_warc_filename()
        self.fd = open(self.filename, 'wb')
        LOGGER.info('opening new warc file %s', self.filename)
        self.writer = WARCWriter(self.fd, gzip=self.gzip, warc_version=self.warc_version)
        warcinfo = self.writer.create_warcinfo_record(self.filename, self.info)
        self.writer.write_record(warcinfo)


# -

def get_writer(prefix, subprefix, info, **kwargs):
    return CDXToolkitWARCWriter(prefix, subprefix, info, **kwargs)


with open(r"J:\Daten\jax\01_Data\02_Webdata\sample-2010-000000.extracted.warc.gz", 'wb') as output:
    print(os.fstat(output.fileno()).st_size)

# + [markdown] tags=[]
# # Work Bench 
# -

# ## Write .warc files 

# Just one URL ✅

# +
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders

import requests

with open(r'J:\Daten\jax\01_Data\02_Webdata\example.warc.gz', 'wb') as output:
    writer = WARCWriter(output, gzip=True)

    resp = requests.get('https://www.sandundkies.de/',
                        headers={'Accept-Encoding': 'identity'},
                        stream=True)

    # get raw headers from urllib3
    headers_list = resp.raw.headers.items()

    http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

    record = writer.create_warc_record('http://example.com/', 'response',
                                        payload=resp.raw,
                                        http_headers=http_headers)

    writer.write_record(record)
# -

# Several URLs ✅

urls = ['https://www.sandundkies.de/', 'https://www.kk-service24.de/', 'https://new.siemens.com/de/de/produkte/gebaeudetechnik/']

# +
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders

import requests

with open(r'J:\Daten\jax\01_Data\02_Webdata\example.warc.gz', 'a') as output:
    writer = WARCWriter(output, gzip=True)

    for u in urls:
        resp = requests.get(u,
                        headers={'Accept-Encoding': 'identity'},
                        stream=True)

        # get raw headers from urllib3
        headers_list = resp.raw.headers.items()

        http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

        record = writer.create_warc_record(u, 'response',
                                        payload=resp.raw,
                                        http_headers=http_headers)

        writer.write_record(record)
# -

# From Internet Archive and Common Crawl ✅

warcinfo = {
    'software': 'pypi_cdx_toolkit iter-and-warc sample',
    'isPartOf': 'GREENWASHING-SAMPLE-IA',
    'description': 'warc extraction',
    'format': 'WARC file version 1.0',
}

import os
os.getcwd()

os.chdir(r'J:\Daten\jax\01_Data\02_Webdata')
os.getcwd()

writer = cdx_toolkit.warc.get_writer(prefix='sample', subprefix='2010', info=warcinfo)

# +
#with open(r'J:\Daten\jax\01_Data\02_Webdata\sample_2010.warc.gz', 'wb') as output:

for c in [crefos[3]]:
    df_sub = df_url.loc[df_url.crefo==c,:]
    for y in range(2010, 2011):
        row = df_sub.loc[df_sub.year == y,:].squeeze(axis=0)
        if pd.isna(row.url):
            pass
        else:
            for obj in tqdm(client.iter(row.url, from_ts=str(row.year), to=str(row.year), limit=limit, verbose='v', collapse='urlkey', filter=['status:200', 'mime:text/html'])):

                try:
                    record = obj.fetch_warc_record()
                    record.rec_headers['crefo'] = row.crefo
                except RuntimeError:
                    print(' skipping capture for RuntimeError 404: %s %s', url, timestamp)
                    continue
                writer.write_record(record)
                
        print(row.crefo)
        


# -

row

# ## Read .warc files 

# From life websites .warc files ✅

from bs4 import BeautifulSoup

# +
from warcio.archiveiterator import ArchiveIterator

with open(r'J:\Daten\jax\01_Data\02_Webdata\example.warc.gz', 'rb') as stream:
    for record in ArchiveIterator(stream):
        print(dir(record))

# +
from warcio.archiveiterator import ArchiveIterator

with open(r'J:\Daten\jax\01_Data\02_Webdata\example.warc.gz', 'rb') as stream:
    for record in ArchiveIterator(stream):
        print(record.http_headers)

# +
from warcio.archiveiterator import ArchiveIterator

with open(r'J:\Daten\jax\01_Data\02_Webdata\example.warc.gz', 'rb') as stream:
    for record in ArchiveIterator(stream):
        print(record.rec_headers.get_header('WARC-Target-URI') + '\n' + BeautifulSoup(record.content_stream().read(), 'html.parser').get_text(strip=True))
        print('\n')
# -

# From IA/CC .warc files ✅

# +
from warcio.archiveiterator import ArchiveIterator

with open(r'J:\Daten\jax\01_Data\02_Webdata\sample-2010-000000.extracted.warc.gz', 'rb') as stream:
    for i, record in enumerate(ArchiveIterator(stream)):
        print(record.http_headers)
        if i > 2:
            break

# +
from warcio.archiveiterator import ArchiveIterator

with open(r'J:\Daten\jax\01_Data\02_Webdata\sample-2010-000000.extracted.warc.gz', 'rb') as stream:
    for i, record in enumerate(ArchiveIterator(stream)):
        if record.http_headers is None:
            pass
        else:
            print(record.http_headers['X-Archive-X-Cache-Key'] + '\n' + BeautifulSoup(record.content_stream().read(), 'html.parser').get_text(strip=True))
            print('\n')
            if i > 2:
                break

# +
from warcio.archiveiterator import ArchiveIterator

with open(r'J:\Daten\jax\01_Data\02_Webdata\sample-2010-000000.extracted.warc.gz', 'rb') as stream:
    for i, record in enumerate(ArchiveIterator(stream)):
        if record.http_headers is None:
            pass
        else:
            print(record.http_headers['X-Archive-X-Cache-Key'])
        
