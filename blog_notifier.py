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
import os
import re
import sqlite3
import smtplib
import yaml

from collections import namedtuple, Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse


############################################################
## Constants and variables
############################################################

BLOGS_DB = 'blogs.sqlite3'
NEW_POST_TUPLE = namedtuple('new_post', 'site header url')
TIMEOUT = 30

conf = {}


############################################################
## Functions
############################################################

def add_to_library(soup: bs4.BeautifulSoup, article: bs4.element.Tag, site: str):
    article_class = __find_class(soup, article)
    last_link = prepare_url(__find_link(article), site)
    try:
        if article_class:
            execute(
                'INSERT INTO blogs (site, last_link, article_container, article_container_class) '
                f'VALUES("{site}", "{last_link}", "{article.name}", "{article_class}")'
            )
        else:
            execute(
                'INSERT INTO blogs (site, last_link, article_container) '
                f'VALUES("{site}", "{last_link}", "{article.name}")'
            )
    except sqlite3.IntegrityError:
        print('\nSite is already present in database')


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
    assert isinstance(link, str), f'Expected {link} to be a string'

    if getattr(response, 'status', None) == http.HTTPStatus.OK:
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
            url = prepare_url(__find_link(post), link)
            if url == last_post:
                break
            queue.put_nowait(
                NEW_POST_TUPLE(link, post.text.replace('\n', ' ')[:400] + "...", url)
            )


async def explore(site: str):
    try:
        soup: Optional[bs4.BeautifulSoup] = None

        async def get_soup(*args, **kwargs):
            nonlocal soup
            content = await kwargs['response'].content.read()
            soup = bs4.BeautifulSoup(content, 'lxml')
        await async_request(get_soup)(site)()
        assert isinstance(soup, bs4.BeautifulSoup), f'Cannot get content of {site}'

        for selector in (
            'article[class*=post]:has(a)',
            'article:has(a)',
            'div[class*=post]:has(a)',
            'div[class*=article]:has(a)',
            'section:has(a)',
        ):
            articles = soup.select(selector)
            if len(articles) > 1 and len(articles) < 101:
                add_to_library(soup, articles[0], site)
                break
    except ConnectionError:
        print(f'Unable to fetch {site}')


def execute(query: str):
    connection = sqlite3.Connection(BLOGS_DB)
    connection.execute(query)
    connection.commit()
    connection.close()


def __find_class(soup: bs4.BeautifulSoup, article: bs4.element.Tag) -> str:
    article_class = ''
    classes = article.attrs.get('class') or []
    for _class in classes:
        if _class.startswith('post') or _class.startswith('article'):
            if len(soup.findAll(article.name, {'class': _class})) > 4:
                article_class = _class
                break
    return article_class


def __find_link(article: bs4.element.Tag) -> str:
    links: Counter = Counter()
    first_link = ""
    for a_element in article.select('a[href]'):
        if not first_link:
            first_link = a_element.attrs.get('href')
        links.update([a_element.attrs.get('href')])

    most_common = links.most_common()[0][0]
    return (
        links.most_common()[0][0]
        if (
            links.most_common()[0][1] > links.get(first_link, 0)
            and (most_common.startswith('/') or most_common.startswith('http'))
        )
        else first_link
    )


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
                article_container        VARCHAR(256),
                article_container_class VARCHAR(256)
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
    with smtplib.SMTP_SSL(conf['server']['host'], conf['server']['port']) as smtp:
        smtp.login(conf['client']['email'], conf['client']['password'])
        connection = sqlite3.Connection(BLOGS_DB)

        for _id, mail in connection.execute('SELECT id, mail FROM mails WHERE is_sent = 0'):
            date = datetime.now().strftime("%d/%m/%Y %H:%M")
            msg = (
                f'From: {conf["client"]["send_to"]}\n'
                f'To: {conf["client"]["send_to"]}\n'
                f'Subject: Blog notifications\n'
                f'Date: {date}\n\n'
                f'{mail}'
            )
            smtp.sendmail(
                conf['client']['email'],
                f'{conf["client"]["send_to"]}',
                msg.encode()
            )
            execute(f'UPDATE mails SET is_sent = 1 WHERE id = {_id}')


def parse_mail_configuration():
    with open('credentials.yml') as rfile:
        conf.update(yaml.load(rfile, Loader=yaml.FullLoader))
        conf['client']['email'] = (
            conf['client']['email'] or os.environ.get('NOTIFIER_CLIENT_EMAIL')
        )
        conf['client']['password'] = (
            conf['client']['password'] or os.environ.get('NOTIFIER_CLIENT_PASSWORD')
        )
        conf['client']['send_to'] = (
            conf['client']['send_to'] or os.environ.get('NOTIFIER_CLIENT_SEND_TO')
        )

    for (first_key, second_key) in (
        ('server', 'host'),
        ('server', 'port'),
        ('client', 'email'),
        ('client', 'password'),
        ('client', 'send_to'),
    ):
        if conf.get(first_key, {}).get(second_key) is None:
            print(f'Please provide conf for {first_key} {second_key}')
            exit(1)
        print(
            f'{first_key} {second_key}: '
            f'{"********" if second_key == "password" else conf[first_key][second_key]}'
        )

    try:
        with smtplib.SMTP_SSL(conf['server']['host'], conf['server']['port']) as smtp:
            smtp.login(conf['client']['email'], conf['client']['password'])
    except smtplib.SMTPException:
        print('Check configuration of the server and correctness of credentials')
        exit(1)


def prepare_url(url: str, site: str) -> str:
    if url.startswith('/'):
        parsed_uri = urlparse(site)
        return f'{parsed_uri.scheme}://{parsed_uri.netloc}{url}'
    return url


async def update_blogs(queue: asyncio.Queue, blogs_information: dict):
    last_links: Dict[str, str] = {}
    mail_text = ''

    while not queue.empty():
        parsed_tuple = await queue.get()
        site, header, url = parsed_tuple.site, parsed_tuple.header, parsed_tuple.url
        if site not in last_links:
            last_links.update({site: url})
        mail_text += f'{header}\n\t\t{url}\n\n'

    if mail_text:
        mail_text = mail_text.replace('"', "'")
        mail_text = re.sub('[ \t]+', ' ', mail_text)[:200]
        execute(f'INSERT INTO mails (mail) VALUES ("{mail_text}")')
    blogs_information.update(last_links)


############################################################
## Entrypoint
############################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Blogs crawler')

    parser.add_argument(
        '-migrate',
        action='store_true',
        default=False,
        help='Create sqlite3 database and prepare tables'
    )
    parser.add_argument(
        '-crawl',
        action='store_true',
        default=False,
        help='Crawl web links'
    )
    parser.add_argument(
        '-explore',
        default='',
        type=str,
        help='Add site to watchlist'
    )

    args = parser.parse_args()

    if args.migrate:
        migrate()

    if args.crawl:
        parse_mail_configuration()
        main()
        notify()

    if args.explore:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(explore(args.explore))
