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
    'iso 14001', 
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

# # Extract matching paragraphs from archived corporate websites

# ## By streaming the .warc files 

import regex
pattern = ['(%s){e<=1}' % keyword for keyword in keywords] # allow one error in the match for each keyword
pattern = '|'.join(f"{k}" for k in pattern)                # join keywords into one string
pattern = regex.compile(pattern, regex.IGNORECASE)         # make string a regex
pattern

for i in regex.finditer(pattern, "Wir wollen klimaneutral werden. Ein net-zero Ziel."):
    print(i)

files = [
    #'2010-000000', '2010-000001', 
    '2011-000000', '2012-000000', '2013-000000', '2014-000000', 
    '2015-000000', '2016-000000', '2017-000000', '2018-000000', '2019-000000', '2020-000000'
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
    df_payload.to_csv(here(r'.\01_Data\02_Webdata\02_Archive\02_Pledge
                           _Indicator\afid_sample_texts-' + str(file) + '.txt'), sep='\t', encoding='utf-8', index=False)
# -

# Very time intensive! A spark solution will be inevitable going forward.

# ## by connecting to a Spark cluster 

# ...

# # Create training data 

# From the extracted html nodes create a training dataset for labelling whether the html node comprises a environmental pledge or not. The training dataset can then be used to develop a text classification model (possibly to finetune a pretrained language model from the transformers family). 

files = [
    '2010-000000', '2010-000001', 
    '2011-000000', '2012-000000', '2013-000000', '2014-000000', 
    '2015-000000', '2016-000000', '2017-000000', '2018-000000', '2019-000000', '2020-000000'
        ]

df_nodes = pd.concat(
    [pd.read_csv(here(r'.\01_Data\02_Webdata\02_Archive\02_Pledge_Indicator\afid_sample_texts-' + str(file) + '.txt'), sep='\t', encoding='utf-8') for file in files],
    ignore_index=True)

df_nodes.shape

# Quickly check if all keywords did find a match on the archived websites.

pattern = ['(%s){e<=1}' % keyword for keyword in keywords]                                  # allow one error in the match for each keyword
pattern_dict = {k: regex.compile(p, regex.IGNORECASE) for (k, p) in zip(keywords, pattern)} # make string a regex
pattern_dict

# + tags=[]
from collections import Counter
result = {}
for k, p in tqdm(pattern_dict.items()):
    rows = df_nodes.node.apply(lambda row: regex.search(p, row))
    n_matches = rows.notnull().sum()
    result[k] = n_matches
# -

result

# Most keywords matched at least once. 

# Conduct some minor cleaning of the matched nodes
df_nodes = df_nodes.loc[(df_nodes.node.apply(len)>100) & (df_nodes.node.apply(len)<1000)]        # reduce to texts with character boundaries
df_nodes['node'] = df_nodes.node.apply(lambda x: " ".join(re.split("\s+", x, flags=re.UNICODE))) # remove trailing whitespaces (including unicode whitespaces!)
df_nodes = df_nodes.drop_duplicates(subset=['node'])                                             # drop duplicated nodes (these may occur if the same website content keeps online over years)
df_nodes = df_nodes.sample(frac=1)                                                               # shuffle nodes

df_nodes.shape

df_nodes.sample(20).style.set_properties(subset=['node'], **{'width': '2000px', 'text-align': 'left'})

# Looks quite promising! Some of these samples are clear environmental pledges others are not. So write the samples in excel format and start labelling!

df_nodes.to_excel(here(r'.\01_Data\02_Webdata\02_Archive\02_Pledge_Indicator\training_data.xlsx', warn=False), sheet_name='to_label', index=False, encoding = "utf-8")
