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
import re, regex                                   # regular expressions
from pyprojroot import here                        # relative paths

# # ISO 14001
#

# According to [Umwelt Bundesamt](https://www.umweltbundesamt.de/themen/wirtschaft-konsum/wirtschaft-umwelt/umwelt-energiemanagement/iso-14001-umweltmanagementsystemnorm#inhalte-der-iso-14001):
#
# Die internationale Norm legt Anforderungen an ein Umweltmanagementsystem fest, mit dem eine Organisation ihre Umweltleistung verbessern, rechtliche und sonstige Verpflichtungen erfüllen und Umweltziele erreichen kann. Die zentralen Elemente der ISO 14001 sind:
#
# - Planung: Festlegung von Umweltzielen und entsprechenden Maßnahmen, Zuständigkeiten und Verfahrensweisen;
# - Durchführung: Umsetzung der festgelegten Maßnahmen und Verfahrensweisen;
# - Kontrolle: Überprüfung der Zuständigkeiten und Verfahrensweisen sowie der Maßnahmen im Hinblick auf die Umweltziele und die Umweltleitlinien (sog. „Umweltpolitik“) der Organisation;
# - Verbesserung: Anpassung der Zuständigkeiten, Verfahren und Maßnahmen sowie ggf. auch der Umweltziele und Umweltleitlinien
#
# Die ISO 14001 ist auf Organisationen jeder Art und Größe sowie auf unterschiedliche geografische, kulturelle, soziale oder ökologische Bedingungen anwendbar. Sie legt allerdings keine absoluten Anforderungen für die Umweltleistung fest. So können zwei Organisationen, die ähnliche Tätigkeiten ausüben, aber unterschiedliche Umweltleistung zeigen, dennoch beide die Anforderungen der ISO 14001 erfüllen.

# + tags=[]
keywords = [
    'iso 14001', 'iso 14002', 'iso 14004', 'iso 14005', 'iso 14006', 'iso 14007', 'iso 14008', 'iso 14009', 'iso 14031', 'iso 14040', 
    'iso 14053'
]
# -

# # Extract matching paragraphs from archived corporate websites

# ## By streaming the .warc files 

pattern = ['(%s){e<=1}' % keyword for keyword in keywords] # allow one error in the match for each keyword
pattern = '|'.join(f"{k}" for k in pattern)                # join keywords into one string
pattern = regex.compile(pattern, regex.IGNORECASE)         # make string a regex
pattern

for i in regex.finditer(pattern, "Wir wollen klimaneutral werden. Ein net-zero Ziel. Dazu haben wir uns ISO-14001 zertifizieren lassen."):
    print(i)

files = [
    '2010-000000', '2010-000001', 
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

#pattern = ['(%s){e<=1}' % keyword for keyword in keywords]                                  # allow one error in the match for each keyword
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

df_nodes['date'] = df_nodes.date.apply(lambda x: pd.to_datetime(x[0:10]))
df_nodes['year'] = df_nodes.date.apply(lambda x: x.year)

pattern = [keyword for keyword in keywords]                # allow one error in the match for each keyword
pattern = '|'.join(f"{k}" for k in pattern)                # join keywords into one string
pattern = re.compile(pattern, flags=re.IGNORECASE)         # make string a regex
pattern


# Define function which only returns nodes that match ISO pattern
def re_filter(pattern, x):
    if re.search(pattern, x):
        return True
    else:
        return False


df_iso = df_nodes.loc[df_nodes.node.apply(lambda x: re_filter(pattern, x)), ['crefo', 'year']].reset_index(drop=True) # apply filter
df_iso = df_iso.drop_duplicates().reset_index(drop=True)                                                              # keep only non duplicated entries between firm id (crefo) and year

df_iso_year = df_iso.groupby('year').agg({'crefo': lambda x: list(x)}) # think of perpetuating the identification of a firm's adoption of ISO 14001to the subsequent years (thereby implicitly assuming that abandoing the standard does not take place)
df_iso_year['n_firms'] = df_iso_year.crefo.apply(len)
df_iso_year.plot(kind='line')

# Plot gives first hint of increasing adoption of ISO 14001 standard.
