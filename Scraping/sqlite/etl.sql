DROP TABLE IF EXISTS etl1;

CREATE TABLE etl1 AS
SELECT
  json_extract(raw_post, '$.channel_url') || '/' || json_extract(raw_post, '$.id') post_url,
  json_extract(raw_post, '$.channel_url') channel_url,
  json_extract(raw_post, '$.date') datetime,
  json_extract(raw_post, '$.edit_date') edit_datetime,
  json_extract(raw_post, '$.message') message,
  json_extract(raw_post, '$.forwards') num_forwards,
  json_extract(raw_post, '$.from_scheduled') from_scheduled,
  json_extract(
    json_extract(raw_post, '$.fwd_from'),
    '$.from_entity_url'
  ) fwd_from,
  json_extract(raw_post, '$.mentioned') mentioned,
  json_extract(raw_post, '$.pinned') pinned,
  json_extract(raw_post, '$.post') post,
  json_extract(json_extract(raw_post, '$.replies'), '$.replies') replies_replies,
  json_extract(
    json_extract(raw_post, '$.replies'),
    '$.replies_pts'
  ) replies_replies_pts,
  json_extract(raw_post, '$.reply_to') reply_to,
  json_extract(raw_post, '$.views') views,
  json_extract(raw_post, '$.via_bot_id') via_bot_id
FROM
  raw_posts;
