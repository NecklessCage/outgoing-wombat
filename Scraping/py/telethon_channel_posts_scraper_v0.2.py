
'''
v0.2    20220512    ted     Script added to fetch the details of the current channel and the forwarded channel.
v0.2    20220803    ted     Saves to sqlite3 db. Minor adjustments to keep scraping thorough and documentable.
'''
'''
How to run this script
======================
- Correct the channel property `scraped_date_range` in `Scraping/data/manual/channels.csv`
    - For set the date range in this column for the channels to be scraped.
    - For "from 3 Aug 2022 to 29 Oct 2022", set the column as `20220803_20221029`

'''


# Reading Configs
import re
import configparser
import json
import asyncio
from telethon import TelegramClient
from telethon.tl.types import PeerChannel, PeerUser
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.errors.rpcerrorlist import ChannelPrivateError
import datetime
from tqdm import tqdm
import pandas as pd
import Sqlite3Functions as s3
config = configparser.ConfigParser()
config.read('../config.ini')

# Setting configuration values
api_id = config['Telegram']['api_id']
api_hash = str(config['Telegram']['api_hash'])
sqlite3db_name = config['Database']['sqlite3db_name']

# Extract URLs
r = re.compile(r'(?:(?:https?|ftp):\/\/)?[\w/\-?=%.]+\.[\w/\-&?=%.]+')
WRITE2DB = True


def str2date(yyyymmdd: str):
    assert len(yyyymmdd) == 8 and yyyymmdd.isnumeric(
    ), 'yyyymmdd is in a wrong format.'
    return datetime.datetime(int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:]))


async def main():
    channelsdf = pd.read_csv('../data/manual/channels.csv')
    channel_to_scrape = channelsdf.loc[
        ~channelsdf.scraped_date_range.str.startswith('DONE')]
    # # Change the data range filter parameter.
    # end_date_exclusive = datetime.datetime(2022, 8, 3)
    # channel_to_scrape = channelsdf.loc[
    #     channelsdf.scraped_date_range == f'None_{end_date_exclusive.strftime("%Y%m%d")}']
    print(f'Scraping {channel_to_scrape.shape[0]} channels.')

    post_limit_per_channel = None  # If None, scrapes all
    exceptions = {}

    sqlite3con = s3.connect_to_sqlite3(f'../db/{sqlite3db_name}')
    today = datetime.datetime.today().strftime('%Y%m%d')

    # To change this file name and append or not when actually scraping
    with open(f'../data/raw/raw_posts_{today}.txt', 'a', encoding='utf8') as f:
        for h, dr in tqdm(zip(channel_to_scrape.channel_handle, channel_to_scrape.scraped_date_range)):
            assert dr.count('_') == 1, 'scraped_date_range is in wrong format.'
            start_date_inclusive_str, end_date_exclusive_str = dr.split('_')
            start_date_inclusive = str2date(start_date_inclusive_str)
            end_date_exclusive = str2date(end_date_exclusive_str)
            # ex saves a list of exceptions for this channel # TODO: this isn't working
            ex = []
            try:  # This try encompasses all code, and only catches `ChannelPrivateError` from telethon
                h = h.lower()
                channel = f'https://t.me/{h}'
                print(f'Processing {channel}')

                json_list = []
                async with TelegramClient('anon', api_id, api_hash) as client:
                    async for message in client.iter_messages(channel, offset_date=end_date_exclusive, limit=post_limit_per_channel):
                        # Remove timezone from message.date just for comparison
                        if message.date.replace(tzinfo=None) >= start_date_inclusive:
                            data = message.to_dict()
                            data['channel_url'] = channel
                            # json.dump(data, f, sort_keys=True, default=str)
                            # Check forwarded channel validity
                            if ('fwd_from' in data) and data['fwd_from']:
                                try:
                                    if data['fwd_from']['from_id']:  # not null
                                        if data['fwd_from']['from_id']['_'] == 'PeerChannel':
                                            forward_entity_id = data['fwd_from']['from_id']['channel_id']
                                            en = await client.get_entity(PeerChannel(forward_entity_id))
                                            en_full = await client(GetFullChannelRequest(channel=en))
                                        elif data['fwd_from']['from_id']['_'] == 'PeerUser':
                                            forward_entity_id = data['fwd_from']['from_id']['user_id']
                                            en = await client.get_entity(PeerUser(forward_entity_id))
                                            en_full = await client(GetFullUserRequest(channel=en))
                                        else:
                                            assert False, data['fwd_from']['from_id']

                                        fwd_entity_info = en_full.to_dict()
                                        fwd_entity_usernames = [
                                            a['username'].lower(
                                            ) if a['username'] else '-'
                                            for a in fwd_entity_info['chats']]
                                        data['fwd_from'][
                                            'from_entity_url'] = f'https://t.me/{fwd_entity_usernames[0]}'
                                    else:  # from_id is null
                                        data['fwd_from']['from_name'] = data['fwd_from']['from_name']
                                except Exception as e:
                                    print('Exception:', e)
                                    pass
                            json.dump(data, f, sort_keys=True, default=str)
                            f.write('\n')

                            # Save to array
                            json_list.append(json.dumps(
                                data, sort_keys=True, default=str))
                        else:
                            print('Breaking ...', message.date)
                            break
                # Save this channel's data to the sqlite db
                L = len(json_list)
                print(L)
                df = pd.DataFrame({
                    'scraper_version': ['0.2']*L,
                    'channel_handle': [h]*L,
                    'window_start': None,
                    'windows_end_exclusive': end_date_exclusive.strftime('%Y%m%d'),
                    'raw_post': json_list
                })
                if WRITE2DB:
                    s3.insert(table='raw_posts', con=sqlite3con,
                              df=df, if_exists='append', chunksize=1000)
            except ChannelPrivateError:
                print(channel, 'ChannelPrivateError')
                ex.append('ChannelPrivateError')
                pass
            except Exception as e:
                print(e)
                ex.append(e)
                pass
            finally:
                exceptions[h] = ex
    with open(f'../log/exceptions_{today}.json', 'w') as g:
        json.dump(exceptions, g, sort_keys=True, default=str)

asyncio.run(main())
