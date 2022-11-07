
import json
import configparser
from tqdm import tqdm
from MyanmarNLPTools import MMSegmentor, MMCleaner, MMTools
from Sqlite3Helpers import Sqlite3Functions as s3
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

config = configparser.ConfigParser()
config.read('../config.ini')

# Setting configuration values
sqlite3db_name = config['Database']['sqlite3db_name']

seg = MMSegmentor.MMSegmentor()
cln = MMCleaner.MMCleaner()


# Connect to the Sqlite3 database file
conn = s3.connect_to_sqlite3(filepath=f'../../Scraping/db/{sqlite3db_name}')


with open('../sqlite/etl.sql') as f:
    conn.cursor().executescript(f.read())


df = s3.fetch(con=conn, query='''
SELECT * FROM etl1
''')


# Set datatypes
df['datetime'] = pd.to_datetime(df.datetime)
df['edit_datetime'] = pd.to_datetime(df.edit_datetime)
df['message'] = df.message.astype(str)
df['num_forwards'] = df.num_forwards.fillna(0).astype(int)
df['from_scheduled'] = df.from_scheduled.astype(bool)
df['fwd_from'] = df.fwd_from.astype(str)
df['mentioned'] = df.mentioned.astype(int)
df['pinned'] = df.pinned.astype(bool)
df['post'] = df.post.astype(bool)
df['replies_replies'] = df.replies_replies.astype(float)  # because of nan
df['replies_replies_pts'] = df.replies_replies_pts.astype(
    float)  # because of nan
# df['reply_to'] = df.reply_to.astype(?)
df['views'] = df.views.fillna(0).astype(int)
# df['via_bot_id'] = df.via_bot_id.astype(?)
df['date_posted'] = df.datetime.dt.date
df['time_posted'] = df.datetime.dt.time
df['date_edited'] = df.edit_datetime.dt.date
df['time_edited'] = df.edit_datetime.dt.time


data = df[['post_url', 'channel_url', 'date_posted', 'message',
           'time_posted', 'date_edited', 'time_edited', 'views']]
data.rename(mapper={
    'message': 'msg_og'
}, axis=1, inplace=True)


# # Extract Outlinks

data['outlinks'] = data.msg_og.apply(MMTools.extract_urls)


# # Clean Messages


data['msg_clean'] = data.msg_og.apply(MMTools.remove_urls).apply(
    cln.web_clean)  # `cln.web_clean` adds <newline> token for each \n


# # Normalize MM Text


zg_detected = 0
total_sentences = 0
msg_uni = []

for m in tqdm(data.msg_clean):
    row_lvl = []
    for l in m.split('<newline>'):
        total_sentences += 1
        if MMTools.is_zawgyi_probability(l) > 0.95:
            zg_detected += 1
            row_lvl.append(MMTools.zg2uni(l))
        else:
            row_lvl.append(l)
    msg_uni.append('<newline>'.join(row_lvl))

print('Written in ZG:', zg_detected, 'out of', total_sentences)
data['msg_clean'] = msg_uni


# # Clean Again after Unicode Conversion


data['msg_clean'] = data.msg_clean.apply(
    cln.web_clean).str.replace('_', '').str.lower()

# # Finding Dups


msg_dups = data.groupby('msg_clean').size(
).sort_values(ascending=False).reset_index()
msg_dups.columns = ['msg_clean', 'qty']
msg_dups = msg_dups.loc[msg_dups.qty > 1]
msg_dups = msg_dups.loc[msg_dups.msg_clean.str.len() > 0]
msg_dups = msg_dups.loc[[m != '-' for m in msg_dups.msg_clean]]
print('Number of messages with dups:', msg_dups.shape[0])
msg_dups


# # Drop Dups through Post URLs


data.drop_duplicates(subset=['post_url'], inplace=True)
tmp = data.groupby('post_url').size().sort_values(ascending=False)
assert (tmp != 1).sum() == 0, 'There are duplicates.'


# # Prepare Datatypes


data['outlinks'] = [json.dumps(l, ensure_ascii=False) for l in data.outlinks]


# # Syllable Segmentation Chunkwise
#
# __Issue__: The number of rows is too high that kernel crashes due to memory limit.
#
# - Before syllable segmentation, save the data to drive.
# - Delete the existing `etl_clean` table from Sqlite3 database since the chunk approach below will require to append instead replace.
# - Read the saved file from drive in chunks.
# - Each chunk get syllable-segmented and inserted into the Sqlite3 database.
# - Adjust the chunk size until it's optimal.


# Save data to tmp file
filename = '../data/tmp/tmp.csv'
data.to_csv(filename, index=False)
# # Delete `data`
# try: del data
# except: pass

# Delete `etl_clean`
s3.q(con=conn, query='''
DROP TABLE IF EXISTS etl_clean;
''')

# Read csv in chunks
chunksize = 10000
for chunk in tqdm(pd.read_csv(filename, chunksize=chunksize)):
    chunk['msg_seg'] = [
        json.dumps([a for a in l if a])
        for l in chunk.msg_clean.fillna('').apply(seg.syllable_segment)]
    s3.insert(table='etl_clean', con=conn, df=chunk,
              if_exists='append', chunksize=chunksize)
