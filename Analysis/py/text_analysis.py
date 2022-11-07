# %%
import configparser
import KeywordSearch
import json
import plotly.io as pio
import plotly_express as px
from Sqlite3Helpers import Sqlite3Functions as s3
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

config = configparser.ConfigParser()
config.read('../../Scraping/config.ini')

# Setting configuration values
sqlite3db_name = config['Database']['sqlite3db_name']
print(sqlite3db_name)
pio.templates.default = 'plotly_white'

# %%
conn = s3.connect_to_sqlite3(f'../../Scraping/db/{sqlite3db_name}')

# %%


def remove_all_spaces(txt):
    return ''.join(txt.split())


def search(msg, terms_dict):
    msg = remove_all_spaces(msg)
    return [
        (k, [t for t in v if t in msg])
        for k, v in terms_dict.items()
    ]


def search_simple(msg, terms):
    msg = remove_all_spaces(msg)
    return [
        t for t in terms
        if t in msg
    ]


def count_terms_found(terms_found):
    for t in terms_found:
        for i in t:
            yield i[0], len(i[1])

# %% [markdown]
# # Get People


# %%
people = pd.read_csv('../data/keywords/people.csv')
people_ids = [c for c in people.columns if c.startswith('A') and len(c) == 3]
print('IDs:', people_ids)

people_terms = {}
for pid in people_ids:
    p = people[[c for c in people.columns if c.startswith(pid)]]
    person_abbr = p.iloc[1, 1]
    people_terms[person_abbr] = [t.strip()
                                 for t in p[pid].dropna() if t != '-']

people_terms

# %% [markdown]
# # Get HS Terms

# %%
hsdf = pd.read_csv('../data/keywords/hs_terms.csv')
hsdf.dropna(subset=['BURMESE TERM'], inplace=True)
hs_terms = hsdf['BURMESE TERM'].loc[hsdf['Term Targets Woman']].tolist()
hs_terms

# %% [markdown]
# # Load and Process in Loop
#
# ## Algo
#
# - Get the total number of rows.
# - Define chunksize which will be the size of each batch.

# %%
NROWS = int(s3.fetch(conn, 'SELECT COUNT("index") FROM etl_clean').iloc[0, 0])
print(NROWS)

# %%
# Delete `hs`
s3.q(con=conn, query='''
DROP TABLE IF EXISTS hs;
''')

index_start = 0
chunksize = 100000

while index_start <= NROWS:
    index_end = index_start + chunksize - 1  # -1 because sql between is inclusive
    print(f'Processing index={index_start}:{index_end}')

    df = s3.fetch(conn,
                  f'SELECT post_url, datetime_posted, msg_clean msg, msg_seg FROM etl_clean WHERE "index" BETWEEN {index_start} AND {index_end};')
    df['msg_seg'] = df.msg_seg.apply(json.loads)
    df['datetime_posted'] = pd.to_datetime(df.datetime_posted)

    # Find HS
    df = KeywordSearch.find_hs(df, hs_terms)
    people_dict = KeywordSearch.search_persons(df, people_terms)
    df = KeywordSearch.format_df(df, people_dict)

    for c in people_dict.keys():
        df[c] = [json.dumps(l, ensure_ascii=False) for l in df[c]]
    df['hs_terms_found'] = [json.dumps(
        l, ensure_ascii=False) for l in df.hs_terms_found]

    s3.insert(table='hs', con=conn, df=df,
              if_exists='append', chunksize=chunksize)

    index_start = index_end + 1

# %%
df

# %%
a, b = 100, 199
print(f'Processing index={a}:{b}')
df = s3.fetch(conn,
              f'SELECT post_url, datetime_posted, msg_clean msg, msg_seg FROM etl_clean WHERE "index" BETWEEN {a} AND {b};')
df['msg_seg'] = df.msg_seg.apply(json.loads)
df['datetime_posted'] = pd.to_datetime(df.datetime_posted)

# Find HS
df = KeywordSearch.find_hs(df, hs_terms)
people_dict = KeywordSearch.search_persons(df, people_terms)
df = KeywordSearch.format_df(df, people_dict)

# %%
print(s3.insert(table='hs', con=conn, df=df.applymap(
    str), if_exists='append', chunksize=chunksize))

# %%
