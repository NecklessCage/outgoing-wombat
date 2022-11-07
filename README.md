# outgoing-wombat

## How to Setup

- Create your Python environment and activate it
- Run `cd Sqlite3Helpers` and `pip install -e .`
- Run `cd MyanmarNLPTools` and `pip install -e .`
- In the root folder, run `pip install -r requirements.txt`

## Start Scraping

- Edit `Scraping/data/manual/channels.csv` and add public channels to scrape
- Get your telegram API key and API hash, and add to the `Scraping/config.ini` (rename `config.blank.ini` to `config.ini` when ready)
  - Create a puppet TG account
  - Get your API details at [https://my.telegram.org/](https://my.telegram.org/)
      - TG will give error if you’re using a VPN. So disconnect VPN temporarily for this part. In my experience, API details can still give error. Firefox works for me every time.
      - Save the API details in a file.
- Activate Python environment set up above
- Go to `Scraping/py/` and run `python telethon_channel_posts_scraper_v0.2.py`. It'll take a few minutes or a few days depending on the number of channels and how active they are
- Once it's done, in the same directory, run `python etl_clean.py` which will extract and create useful data table in Sqlite3 database

## Exploring the Data

- Open the database file `Scraping/db/database.db` using your favorite DB explorer that an open Sqlite3 databases. Keep in mind that the file extension might be different from what you find on the internet but it'll work.
- I use [DBeaver](https://dbeaver.io/). It's free and lots of great functionalities.
- You may query the data as you wish in the DBeaver and export to Excel if needed.
