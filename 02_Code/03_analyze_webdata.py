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

# # Metadata

files = ['2010-000000', '2010-000001', '2011-000000', '2012-000000', '2013-000000', '2014-000000', 
         '2015-000000', '2016-000000', '2017-000000', '2018-000000', '2019-000000', '2020-000000'
        ]

# +
df_header = pd.DataFrame()

for file in files:
    
    print('File:', str(file))
    with open(here(r'./01_Data/02_Webdata/02_Archive' + '/afid_sample-' + str(file) + '.extracted.warc.gz') , 'rb') as stream:
        for record in tqdm(ArchiveIterator(stream)):
            if record.rec_headers['WARC-Type'] == 'warcinfo':
                pass
            else:
                temp = sorted([(i[0], [i[1]]) for i in record.rec_headers.headers if i[0] in ['crefo', 'WARC-Date', 'Content-Type', 'Content-Length', 'WARC-Source-URI']], key=lambda tup: tup[0], reverse=True) 
                df_temp = pd.DataFrame.from_dict(dict(temp))
                df_header = df_header.append(df_temp)
    stream.close()
df_header.reset_index(drop=True, inplace=True)
# -

df_header

df_header['Content-Type'].value_counts(dropna=False)

df_header.iloc[0:4]['WARC-Source-URI'].values

# # Payload 

keywords = [
    "klimaneutral", "net.{,1}zero"
]

pattern = '|'.join(f"{k}" for k in keywords)  # Whole words only    
pattern = re.compile(pattern, flags=re.IGNORECASE)
pattern

re.findall(pattern, "Wir wollen klimaneutral werden. Ein net zero Ziel.")

files = [#'2010-000000', '2010-000001', '2011-000000', '2012-000000', '2013-000000', '2014-000000', 
         #'2015-000000', '2016-000000', '2017-000000', '2018-000000', '2019-000000', 
    '2020-000000'
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
    df_payload.to_csv(config.PATH_TO_POJECT_FOLDER + r'\01_Data\02_Webdata\afid_sample_texts-' + str(file) + '.txt', sep='\t', encoding='utf-8', index=False)
# -

df_temp = df_payload.drop_duplicates(subset=['node']).reset_index(drop=True).head(10)
df_temp.style.set_properties(subset=['node'], **{'width': '2000px', 'text-align': 'left'})

# # Work Bench 
