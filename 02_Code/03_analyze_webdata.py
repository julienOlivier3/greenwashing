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

from warcio.archiveiterator import ArchiveIterator
import pandas as pd # used to store results in a data frame
from tqdm import tqdm
from bs4 import BeautifulSoup
import re

# # Metadata

files = ['2010-000000', '2010-000001', '2011-000000', '2012-000000', '2013-000000', '2014-000000', 
         '2015-000000', '2016-000000', '2017-000000', '2018-000000', '2019-000000', '2020-000000'
        ]

# +
df_header = pd.DataFrame()

for file in files:
    
    print('File:', str(file))
    with open(r'J:\Daten\jax\01_Data\02_Webdata\afid_sample-' + str(file) + '.extracted.warc.gz', 'rb') as stream:
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
    with open(r'J:\Daten\jax\01_Data\02_Webdata\afid_sample-' + str(file) + '.extracted.warc.gz', 'rb') as stream:
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
    df_payload.to_csv(r'J:\Daten\jax\01_Data\02_Webdata\afid_sample_texts-' + str(file) + '.txt', sep='\t', encoding='utf-8', index=False)
# -

df_temp = df_payload.drop_duplicates(subset=['node']).reset_index(drop=True).head(10)
df_temp.style.set_properties(subset=['node'], **{'width': '2000px', 'text-align': 'left'})

temp = pd.read_csv(r"J:\Daten\jax\01_Data\02_Webdata\afid_sample_texts-2019-000000.txt", sep='\t', encoding='utf-8')

temp.iloc[16].node

# # Work Bench 

print(dir(html_code.descendants))

for child in html_code.find('body').descendants:
    print(child.get_text())

html_doc = """
<html><head><title>The Dormouse's story</title></head>
<body>
<p class="title">  Climate neutrality             <b>The Dormouse's story and the role of carbon offset</b></p>

<p class="story">Once upon a time there were three little sisters; and their names were
<a href="http://example.com/elsie" class="sister" id="link1">Elsie</a>,
<a href="http://example.com/lacie" class="sister" id="link2">Lacie</a> and
<a href="http://example.com/tillie" class="sister" id="link3">Tillie</a>;
and they lived at the bottom of a well.</p>

<p class="story">Sustainability goals</p>
"""

soup = BeautifulSoup(html_doc, 'html.parser')
soup

print(soup.prettify())

[i.strip() for i in soup.findAll(text=pattern)]
