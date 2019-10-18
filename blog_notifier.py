# -*- coding: utf-8 -*-

############################################################
## Imports
############################################################

import aiohttp
import argparse
import asyncio
import async_timeout
import bs4
import http
import sqlite3
import smtplib
import yaml

from collections import namedtuple
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse


BLOGS_DB = 'blogs.sqlite3'
NEW_POST_TUPLE = namedtuple('new_post', 'site header url')
TIMEOUT = 30


def async_request(func):
    def wrapper(link):
        async def inner(*args, **kwargs):
            try:
                async with async_timeout.timeout(TIMEOUT):
                    async with aiohttp.ClientSession() as session:
                        async with session.get(link) as response:
                            kwargs.update({'link': link, 'response': response})
                            await func(*args, **kwargs)
            except Exception:
                print(f'Blame {link}')
        return inner
    return wrapper


@async_request
async def crawl(queue: asyncio.Queue, blogs_information: dict, last_post=None, **kwargs):
    response = kwargs.get('response')
    link = kwargs.get('link')

    if response.status == http.HTTPStatus.OK:
        content = await response.content.read()
        soup = bs4.BeautifulSoup(content, 'lxml')
        if blogs_information[link].get('article_container_class'):
            posts = soup.findAll(
                blogs_information[link].get('article_container'),
                {'class': blogs_information[link].get('article_container_class')}
            )
        else:
            posts = soup.findAll(blogs_information[link].get('article_container'))

        for post in posts:
            link_container = blogs_information[link].get('link_container')
            header = post.find(link_container).text
            url = post.find(link_container).find('a').attrs.get('href')

            if url.startswith('/'):
                parsed_uri = urlparse(link)
                url = f'{parsed_uri.scheme}://{parsed_uri.netloc}{url}'
            if url == last_post:
                break
            queue.put_nowait(NEW_POST_TUPLE(link, header, url))


def execute(query: str):
    connection = sqlite3.Connection(BLOGS_DB)
    connection.execute(query)
    connection.commit()
    connection.close()


def main():
    connection = sqlite3.Connection(BLOGS_DB)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    __blogs_information = cursor.execute('SELECT * from blogs')
    blogs_information = {}
    for info in __blogs_information:
        info = dict(info)
        blogs_information.update({info.pop('site'): info})

    blogs_last_urls = {
        url: values['last_link']
        for url, values in blogs_information.items()
    }

    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()
    tasks = [
        crawl(site)(queue, blogs_information, blogs_last_urls[site])
        for site in blogs_last_urls
    ]

    loop.run_until_complete(asyncio.gather(*tasks))
    loop.run_until_complete(update_blogs(queue, blogs_last_urls))

    for site, last_link in blogs_last_urls.items():
        execute(f'UPDATE blogs SET last_link = "{last_link}" WHERE site = "{site}"')


def migrate():
    blogs_db = Path(BLOGS_DB)
    if not blogs_db.exists():
        blogs_db.touch()
        execute(
            """
            CREATE TABLE IF NOT EXISTS blogs (
                site                    VARCHAR(256) PRIMARY KEY,
                last_link               VARCHAR(256),
                aricle_container        VARCHAR(256),
                article_container_class VARCHAR(256),
                link_container          VARCHAR(256)
            )
            """
        )
        execute(
            """
            CREATE TABLE IF NOT EXISTS mails (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                mail    TEXT,
                is_sent INTEGER DEFAULT 0
            )
            """
        )


def notify():
    with open('credentials.yml') as rfile:
        conf = yaml.load(rfile, Loader=yaml.FullLoader)
        smtp = smtplib.SMTP_SSL(conf['server']['host'], conf['server']['port'])
        smtp.login(conf['client']['email'], conf['client']['password'])

        connection = sqlite3.Connection(BLOGS_DB)

        for _id, mail in connection.execute('SELECT id, mail FROM mails WHERE is_sent = 0'):
            date = datetime.now().strftime("%d/%m/%Y %H:%M")
            msg = (
                f'From: blogs@notification.com\n'
                f'To: {conf["client"]["email"]}\n'
                f'Subject: Blog notifications\n'
                f'Date: {date}\n\n'
                f'{mail}'
            )
            smtp.sendmail('blogs@notification.com', conf['client']['email'], msg.encode())
            execute(f'UPDATE mails SET is_sent = 1 WHERE id = {_id}')

        smtp.quit()


async def update_blogs(queue: asyncio.Queue, blogs_information: dict):
    last_links = {}
    mail_text = ''

    while not queue.empty():
        parsed_tuple = await queue.get()
        site, header, url = parsed_tuple.site, parsed_tuple.header, parsed_tuple.url
        if site not in last_links:
            last_links.update({site: url})
        mail_text += f'{header}\n\t\t{url}\n\n'

    if mail_text:
        execute(f'INSERT INTO mails (mail) VALUES ("{mail_text}")')
    blogs_information.update(last_links)


############################################################
## Entrypoint
############################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Blogs crawler')

    parser.add_argument(
        '--migrate',
        action='store_true',
        default=False,
        help='Create sqlite3 database and prepare tables'
    )
    parser.add_argument(
        '--crawl',
        action='store_true',
        default=False,
        help='Crawl web links'
    )

    args = parser.parse_args()

    if args.migrate:
        migrate()

    if args.crawl:
        main()
        notify()
