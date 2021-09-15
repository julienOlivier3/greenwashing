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

import config                                      # configuration files includes API keys and paths
from warcio.archiveiterator import ArchiveIterator # iterate over .warc files
import pandas as pd 
from tqdm import tqdm                              # progress bar
from bs4 import BeautifulSoup                      # html parsing
import re                                          # regular expressions
from pyprojroot import here                        # relative paths

# # Sample pledges
#

df_pledges = pd.read_excel(here('./01_Data/02_Webdata/environmental_pledges.xlsx'), sheet_name='environmental_pledges')

df_pledges.pledge.sample(3).values

df_pledges.keywords.dropna().values

# + tags=[]
keywords = [
    'iso 140001', 
    'klimaneutral', 'climate neutral',
    'co2 neutral', 'co2 frei', 'carbon free', 'carbon neutral',
    'netto null', 'net zero',
    'klimapositiv', 'climate positive',
    'co2 bilanz', 'co2 fußabdruck', 'kohlenstoffbilanz', 'carbon footprint',
    'co2 äquivalent', 'kohlenstoffzertifikat', 'co2 equivalent', 'carbon certificate',
    'klimaschutz', 'climate protection',
    'klimamaßnahme', 'klimaschutz', 'climate action', 'climate protection'
]
# -

# # Extract matching paragraphs from archived corporate website data

# ## By streaming the .warc files 

import regex
pattern = ['(%s){e<=1}' % keyword for keyword in keywords] # allow one error in the match for each keyword
pattern = '|'.join(f"{k}" for k in pattern)                # join keywords into one string
pattern = regex.compile(pattern, regex.IGNORECASE, regex.V1)         # make string a regex
pattern

for i in regex.finditer(pattern, "Wir wollen klimaneutral werden. Ein net-zero Ziel."):
    print(i)

files = [
    '2010-000000', #'2010-000001', '2011-000000', '2012-000000', '2013-000000', '2014-000000', 
         #'2015-000000', '2016-000000', '2017-000000', '2018-000000', '2019-000000', '2020-000000'
        ]

# + tags=[]
for file in files:
    df_payload = pd.DataFrame()
    
    print('File:', str(file))
    with open(here(r'./01_Data/02_Webdata/02_Archive' + '/afid_sample-' + str(file) + '.extracted.warc.gz') , 'rb') as stream:
        for i, record in enumerate(tqdm(ArchiveIterator(stream))):
            #if i > 300:
            #    break
            if record.rec_headers['WARC-Type'] == 'warcinfo':
                pass
            else:
                crefo = record.rec_headers['crefo']
                date = record.rec_headers['WARC-Date']
                try:
                    html_code = BeautifulSoup(record.content_stream().read().decode('utf-8'), features="html.parser")
                    lst_temp = [i.replace('\n', " ").replace('\r', " ").replace('\t', " ").strip() for i in html_code.findAll(text=pattern)]
                    df_temp = pd.DataFrame({'crefo': crefo, 'date': date, 'node': lst_temp})
                    df_payload = df_payload.append(df_temp)
                except UnicodeDecodeError:
                    html_code = BeautifulSoup(record.content_stream().read(), features="html.parser")
                    lst_temp = [i.replace('\n', " ").replace('\r', " ").replace('\t', " ").strip() for i in html_code.findAll(text=pattern)]
                    df_temp = pd.DataFrame({'crefo': crefo, 'date': date, 'node': lst_temp})
                    df_payload = df_payload.append(df_temp)
            df_payload.drop_duplicates(subset=['crefo', 'node'], inplace=True)
            
    stream.close()
    df_payload.to_csv(here(r'\01_Data\02_Webdata\02_Archive\02_Second_Indicator\afid_sample_texts-' + str(file) + '.txt'), sep='\t', encoding='utf-8', index=False)
# -

df_payload.to_csv(here(r'.\01_Data\02_Webdata\02_Archive\02_Second_Indicator\afid_sample_texts-' + str(file) + '.txt'), sep='\t', encoding='utf-8', index=False)

here(r'\01_Data\02_Webdata\02_Archive\02_Second_Indicator\afid_sample_texts-' + str(file) + '.txt')

df_payload.shape

# + tags=[]
df_temp = df_payload.drop_duplicates(subset=['node']).reset_index(drop=True).sample(10)
df_temp.style.set_properties(subset=['node'], **{'width': '2000px', 'text-align': 'left'})
# -

# Very time intensive!
