#!/usr/bin/env python3
# -*- coding: utf-8 -*-

############################################################
# Imports
############################################################

import argparse
import asyncio
import contextlib
import http
import os
import re
import smtplib
import sqlite3
import sys

from collections import Counter, namedtuple
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, Optional
from urllib.parse import urlparse, quote

import aiohttp
import async_timeout
import bs4
import requests
import yaml


############################################################
# Constants and variables
############################################################

NewPostTuple = namedtuple('new_post', 'site header url')

CONFIG_FILE = os.environ.get('NOTIFIER_CONFIG', 'credentials.yml')
BLOGS_DB = os.environ.get('NOTIFIER_DB', 'blogs.sqlite3')

MAIL_MODE = 'mail'
TELEGRAM_MODE = 'telegram'

LIMIT_NEW_POSTS = int(os.environ.get('NOTIFIER_LIMIT_NEW_POSTS', 3))
TIMEOUT = 30
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) '
                  'Gecko/20100101 Firefox/109.0'
}

conf: Dict[str, Any] = {}


############################################################
# Functions
############################################################

def add_to_library(soup: bs4.BeautifulSoup, article: bs4.element.Tag, site: str):
    article_class = __find_class(soup, article)
    last_link = prepare_url(__find_link(article), site)
    try:
        if article_class:
            execute(
                'INSERT INTO blogs (site, last_link, article_container, article_container_class) '
                'VALUES(?, ?, ?, ?)',
                site, last_link, article.name, article_class,
            )
        else:
            execute(
                'INSERT INTO blogs (site, last_link, article_container) VALUES(?, ?, ?)',
                site, last_link, article.name,
            )
        execute('INSERT INTO posts (site, link) VALUES(?, ?)', site, last_link)
    except sqlite3.IntegrityError:
        print('\nSite is already present in database')


def async_request(func):
    def wrapper(link):
        async def inner(*args, **kwargs):
            try:
                async with async_timeout.timeout(TIMEOUT):
                    async with aiohttp.ClientSession(headers=HEADERS) as session:
                        async with session.get(link) as response:
                            kwargs.update({'link': link, 'response': response})
                            await func(*args, **kwargs)
            except Exception as e:
                print(f'Blame {link}: {e}')
        return inner
    return wrapper


@async_request
async def crawl(queue: asyncio.Queue, blogs_information: dict, **kwargs):
    response = kwargs.get('response')
    link = kwargs.get('link')
    assert isinstance(link, str), f'Expected {link} to be a string'

    if getattr(response, 'status', None) == http.HTTPStatus.OK:
        assert isinstance(response, aiohttp.ClientResponse), 'Expected aiohttp.ClientResponse'
        content = await response.content.read()
        soup = bs4.BeautifulSoup(
            content, 'lxml-xml' if response.real_url.human_repr().endswith('xml') else 'lxml',
        )
        if blogs_information[link].get('article_container_class') is not None:
            posts = soup.findAll(
                blogs_information[link].get('article_container'),
                {'class': blogs_information[link].get('article_container_class')}
            )
        else:
            posts = soup.findAll(blogs_information[link].get('article_container'))

        added_urls = set()
        for post in list(posts)[:LIMIT_NEW_POSTS]:
            url = prepare_url(__find_link(post), link)
            if url == prepare_url('', link) or url in added_urls:
                continue
            if execute('SELECT 1 FROM posts WHERE site = ? AND link = ?', link, url):
                continue

            added_urls.add(url)
            queue.put_nowait(
                NewPostTuple(link, post.text[:400] + '...', url)
            )


async def explore(site: str):
    try:
        soup: Optional[bs4.BeautifulSoup] = None

        async def get_soup(*_, **kwargs):
            nonlocal soup
            content = await kwargs['response'].content.read()
            soup = bs4.BeautifulSoup(content, 'lxml-xml' if site.endswith('xml') else 'lxml')
        await async_request(get_soup)(site)()
        assert isinstance(soup, bs4.BeautifulSoup), f'Cannot get content of {site}'

        for selector in (
            'article[class*=post]:has(a)',
            'article[class=issue]:has(a)',
            'article:has(a)',
            'div[class*=post]:has(a)',
            'div[class*=article]:has(a)',
            'div[class=issue]:has(a)',
            'div[class=summary]:has(a)',
            'section:has(a)',
            'h2:has(a)',
            'tr:has(a)',
            'li:has(a)',
            'item:has(link)',
        ):
            articles = soup.select(selector)
            if len(articles) > 1:
                add_to_library(soup, articles[0], site)
                break
    except ConnectionError:
        print(f'Unable to fetch {site}')


def execute(query: str, *args):
    connection = sqlite3.Connection(BLOGS_DB)
    cursor = connection.execute(query, args)
    result = cursor.fetchall()
    connection.commit()
    connection.close()
    return result


def __find_class(soup: bs4.BeautifulSoup, article: bs4.element.Tag) -> str:
    article_class = ''
    classes = article.attrs.get('class') or []
    for _class in classes:
        for prefix in ('post', 'article', 'issue', 'summary'):
            if _class.startswith(prefix) and len(soup.findAll(article.name, {'class': _class})) > 4:
                article_class = _class
                break
    return article_class


def __find_link(article: bs4.element.Tag) -> str:
    links: Counter = Counter()
    first_link = ''
    for header in ('h1', 'h2', 'h3'):
        header_link = article.select(f'{header} a[href]')
        if header_link:
            return header_link[0].attrs.get('href')
    for a_element in article.select('a[href]'):
        if not first_link:
            first_link = a_element.attrs.get('href')
        links.update([a_element.attrs.get('href')])
    for link_element in article.select('link'):
        href = link_element.attrs.get('href', link_element.text)
        if not first_link:
            first_link = href
        links.update([href])

    if not links:
        return ''

    most_common = links.most_common()[0][0]
    return (
        links.most_common()[0][0]
        if (
            links.most_common()[0][1] > links.get(first_link, 0)
            and (most_common.startswith('/') or most_common.startswith('http'))
        )
        else first_link
    )


@contextlib.contextmanager
def __get_cursor() -> Generator[sqlite3.Cursor, None, None]:
    connection = sqlite3.Connection(BLOGS_DB)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        connection.commit()


def list_links():
    with __get_cursor() as cursor:
        for info in cursor.execute('SELECT * from blogs'):
            print(f'\033[1m{info["site"]}\033[0m\n{info["last_link"]}\n')


async def gather_tasks(*tasks):
    await asyncio.gather(*tasks)


def run():
    blogs_information: Dict[str, dict] = {}

    with __get_cursor() as cursor:
        __blogs_information = cursor.execute('SELECT * from blogs')
        for info in __blogs_information:
            info = dict(info)
            blogs_information.update({info.pop('site'): info})

    blogs_last_urls = {
        url: [] for url, values in blogs_information.items()
    }

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    queue = asyncio.Queue()
    tasks = [
        crawl(site)(queue, blogs_information)
        for site in blogs_last_urls
    ]

    asyncio.run(gather_tasks(*tasks))
    asyncio.run(update_blogs(queue, blogs_last_urls))

    for site, links in blogs_last_urls.items():
        for i in range(len(links)):
            if i == 0:
                execute('UPDATE blogs SET last_link = ? WHERE site = ?', links[i], site)
            execute('INSERT INTO posts (link, site) VALUES (?, ?)', links[i], site)


def migrate():
    blogs_db = Path(BLOGS_DB)
    if not blogs_db.exists():
        blogs_db.touch()

    execute(
        """
        CREATE TABLE IF NOT EXISTS blogs (
            site                    VARCHAR(256) PRIMARY KEY,
            last_link               VARCHAR(256),
            article_container       VARCHAR(256),
            article_container_class VARCHAR(256)
        )
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS posts (
            site    VARCHAR(256),
            link    VARCHAR(256),
            FOREIGN KEY (site) REFERENCES blogs(site) ON DELETE CASCADE
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
    if conf['mode'] == TELEGRAM_MODE:  # noop
        return

    with smtplib.SMTP_SSL(conf['server']['host'], conf['server']['port']) as smtp:
        smtp.login(conf['client']['email'], conf['client']['password'])

        for _id, mail in execute('SELECT id, mail FROM mails WHERE is_sent = 0'):
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
            execute('UPDATE mails SET is_sent = 1 WHERE id = ?', _id)


def parse_configuration():
    os.environ.get('')
    with open(CONFIG_FILE) as rfile:
        conf.update(yaml.load(rfile, Loader=yaml.FullLoader))
        for section, key in (
            ('client', 'email'),
            ('client', 'password'),
            ('client', 'send_to'),
            ('telegram', 'bot_token'),
            ('telegram', 'channel'),
        ):
            conf[section][key] = (
                conf[section][key] or os.environ.get(f'NOTIFIER_{section.upper()}_{key.upper()}')
            )

    for (first_key, second_key, mode) in (
        ('server', 'host', MAIL_MODE),
        ('server', 'port', MAIL_MODE),
        ('client', 'email', MAIL_MODE),
        ('client', 'password', MAIL_MODE),
        ('client', 'send_to', MAIL_MODE),
        ('telegram', 'bot_token', TELEGRAM_MODE),
        ('telegram', 'channel', TELEGRAM_MODE),
    ):
        if conf.get(first_key, {}).get(second_key) is None and conf.get('mode') == mode:
            print(f'Please provide conf for {first_key} {second_key}')
            sys.exit(1)
        print(
            f'{first_key} {second_key}: '
            f'{"********" if second_key in ("password", "bot_token") else conf[first_key][second_key]}'
        )

    if conf['mode'] == TELEGRAM_MODE:  # noop
        return

    try:
        with smtplib.SMTP_SSL(conf['server']['host'], conf['server']['port']) as smtp:
            smtp.login(conf['client']['email'], conf['client']['password'])
    except smtplib.SMTPException:
        print('Check configuration of the server and correctness of credentials')
        sys.exit(1)


def prepare_url(url: str, site: str) -> str:
    if '://' not in url:
        parsed_uri = urlparse(site)
        url = url.lstrip('/')
        return f'{parsed_uri.scheme}://{parsed_uri.netloc}/{url}'
    return url


def remove(site: str) -> None:
    execute('DELETE from blogs WHERE site = ?', site)


async def update_blogs(queue: asyncio.Queue, blogs_information: dict):
    mail_text, tg_messages = '', []

    while not queue.empty():
        parsed_tuple = await queue.get()
        site, text, url = parsed_tuple.site, parsed_tuple.header, parsed_tuple.url
        blogs_information[site].append(url)
        mail_text += text.replace("\n", " ") + f'\n\t\t{url}\n\n'
        tg_messages.append(f'{text}\n\n{url}')

    if mail_text:
        mail_text = mail_text.replace('"', "'")
        mail_text = re.sub('[ ]+', ' ', mail_text)
        if conf['mode'] == MAIL_MODE:
            execute('INSERT INTO mails (mail) VALUES (?)', mail_text)
        elif conf['mode'] == TELEGRAM_MODE and conf['telegram']['bot_token']:
            for message in tg_messages:
                requests.get(
                    f'https://api.telegram.org/bot{conf["telegram"]["bot_token"]}/sendMessage'
                    f'?chat_id={conf["telegram"]["channel"]}&text={quote(message)}'
                )
        else:
            return


############################################################
# Entrypoint
############################################################

def init_parser() -> argparse.ArgumentParser:
    _parser = argparse.ArgumentParser(description='Blogs crawler')

    _parser.add_argument(
        '-migrate',
        action='store_true',
        default=False,
        help='Create sqlite3 database and prepare tables'
    )
    _parser.add_argument(
        '-crawl',
        action='store_true',
        default=False,
        help='Crawl web links'
    )
    _parser.add_argument(
        '-explore',
        default='',
        type=str,
        help='Add site to watchlist'
    )
    _parser.add_argument(
        '-list',
        action='store_true',
        default=False,
        help='List saved sites'
    )
    _parser.add_argument(
        '-remove',
        default='',
        type=str,
        help='Remove site from watchlist'
    )

    return _parser


def main() -> None:
    parser = init_parser()

    args = parser.parse_args()

    if args.migrate:
        migrate()

    if args.crawl:
        parse_configuration()
        run()
        notify()

    if args.explore:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        asyncio.run(explore(args.explore))

    if args.list:
        list_links()

    if args.remove:
        remove(args.remove)


if __name__ == '__main__':
    main()
