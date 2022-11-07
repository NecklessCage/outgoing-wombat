
import Sqlite3Functions as s3
from datetime import datetime


if __name__ == '__main__':
    # Connect to db
    conn = s3.connect_to_sqlite3('../db/ghs.db')

    # Fetch data
    data = s3.fetch(conn, '''
        WITH a AS (
        SELECT
            channel_handle,
            json_extract(raw_post, '$.fwd_from') AS fwd_from
        FROM
            raw_posts rp
        WHERE
            fwd_from IS NOT NULL
        )
        SELECT
        channel_handle forwarder,
        json_extract(fwd_from, '$.from_entity_url') src
        FROM
        a
        ''')

    # Clean data
    data = data.loc[~data.src.isna()]
    data['src'] = [l.split('/')[-1] for l in data.src]
    data.columns = ['target', 'source']
    data = data[['source', 'target']]

    # Calculate forward pair counts
    forwards = data.groupby(['source', 'target']).size().reset_index()
    forwards.columns = ['source', 'target', 'weight']
    forwards.sort_values('weight', ascending=False, inplace=True)

    # Save
    now = datetime.now().strftime('%Y%m%d%H%M')
    forwards.to_csv(
        f'../data/output/{now}source_target_weight.csv', index=False)
