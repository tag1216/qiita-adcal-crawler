import csv
import datetime
import time
import traceback
import typing
from http import HTTPStatus
from pathlib import Path
from typing import Iterable, List, Tuple
from urllib.parse import urljoin

import click
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

WAIT_SEC = 1


class HttpException(Exception):

    def __init__(self, status_code):
        self.status_code = status_code


Calendar = typing.NamedTuple('Calendar', [('year', int),
                                          ('calendar_id', str),
                                          ('title', str),
                                          ('url', str),
                                          ('category', str),
                                          ('participants_count', int),
                                          ('likes_count', int),
                                          ('subscribers_count', int)])

Item = typing.NamedTuple('Item', [('year', int),
                                  ('calendar_id', str),
                                  ('date', int),
                                  ('user_name', str),
                                  ('user_url', str),
                                  ('title', str),
                                  ('url', str)])


Liker = typing.NamedTuple('Liker', [('year', int),
                                    ('calendar_id', str),
                                    ('date', int),
                                    ('user_name', str),
                                    ('user_url', str)])


class CalendarCrawler:

    site = 'https://qiita.com/'
    calendars_url = 'https://qiita.com/advent-calendar/{year}/calendars'
    categories_url = 'https://qiita.com/advent-calendar/{year}/categories/{category}'
    calendar_url = 'https://qiita.com/advent-calendar/{year}/{calendar_id}'

    def __init__(self):
        self.request_count = 0

    def crawl_calendars(self,
                        year: int,
                        category=None) -> Iterable[Tuple[str, str, str]]:

        if category:
            url = self.categories_url.format(year=year, category=category)
        else:
            url = self.calendars_url.format(year=year)

        for soup in self.iterate_pagination(url):

            for tr in soup.select('.adventCalendarList tbody tr'):
                title_link = tr.select_one('.adventCalendarList_calendarTitle > a')
                title = title_link.text
                url = urljoin(self.site, title_link['href'])
                calendar_id = Path(url).parts[-1]

                yield calendar_id, title, url,

    def crawl_calendar(self, year: int, calendar_id: str) -> Tuple[Calendar, List[Item]]:

        url = self.calendar_url.format(year=year,
                                       calendar_id=calendar_id)

        soup = self.get_page(url)

        title = soup.h1.text

        category = soup.select_one('.adventCalendarSection_info a').text

        participants_count = int(soup.select('.adventCalendarJumbotron_stats')[0].text)
        likes_count = int(soup.select('.adventCalendarJumbotron_stats')[1].text)
        subscribers_count = int(soup.select('.adventCalendarJumbotron_stats')[2].text)

        items = list(self.parse_calendar_items(year, calendar_id, soup))

        return (Calendar(year,
                         calendar_id,
                         title,
                         url,
                         category,
                         participants_count,
                         likes_count,
                         subscribers_count),
                items,)

    def parse_calendar_items(self,
                             year: int,
                             calendar_id: str,
                             soup: BeautifulSoup) -> Iterable[Item]:

        for td in soup.select('.adventCalendarCalendar_day'):

            date = int(td.select_one('.adventCalendarCalendar_date').text)

            user_name, user_url = None, None
            user_link = td.select_one('.adventCalendarCalendar_author a')
            if user_link:
                user_name = user_link.text.strip()
                user_url = urljoin(self.site, user_link['href'])

            item_title, item_url = None, None
            comment_div = td.select_one('.adventCalendarCalendar_comment')
            if comment_div:
                item_title = comment_div.text
                item_link = comment_div.select_one('a')
                item_url = item_link['href'] if item_link else None

            yield Item(year, calendar_id, date, user_name, user_url, item_title, item_url)

    def crawl_likers(self, year, calendar_id) -> Iterable[Liker]:

        calendar, items = self.crawl_calendar(year, calendar_id)

        for item in items:

            if not self.is_qiita_item(item.url):
                continue

            likers_url = item.url + '/likers'

            try:
                for soup in self.iterate_pagination(likers_url):

                    for user_el in soup.select('.GridList__user'):
                        user_name = user_el.select_one('.UserInfo__name').text
                        user_url = urljoin(self.site, user_el.a['href'])
                        yield Liker(year, calendar_id, item.date, user_name, user_url)
            except Exception:
                traceback.extract_stack()

    def get_page(self, url: str) -> BeautifulSoup:

        self.request_count += 1

        time.sleep(WAIT_SEC)

        response = requests.get(url)
        if response.status_code != HTTPStatus.OK:
            raise HttpException(response.status_code)

        soup = BeautifulSoup(response.content, 'html.parser')

        return soup

    def iterate_pagination(self, url: str) -> Iterable[BeautifulSoup]:

        next_url = url

        while next_url:

            soup = self.get_page(next_url)

            yield soup

            next_link = soup.select_one('a[rel=next]')
            next_url = urljoin(self.site, next_link['href']) if next_link else None

    def is_qiita_item(self, url: str) -> bool:
        return url and url.startswith(self.site) and '/private/' not in url


@click.command()
@click.argument('year', type=int)
@click.option('--output', '-o', type=Path, default=Path('output'))
@click.option('--category', '-c', type=click.STRING)
def crawl_calendars(output, year, category):

    click.echo(datetime.datetime.now())

    if category:
        calendars_path = output / str(year) / f'calendars_{category}.tsv'
        items_path = output / str(year) / f'items_{category}.tsv'
    else:
        calendars_path = output / str(year) / 'calendars.tsv'
        items_path = output / str(year) / f'items.tsv'

    calendars_path.parent.mkdir(parents=True, exist_ok=True)

    crawler = CalendarCrawler()

    calendar_ids = [
        calendar_id
        for calendar_id, *_ in crawler.crawl_calendars(year, category)
    ]

    calendars_file = calendars_path.open('w')
    items_file = items_path.open('w')
    with calendars_file, items_file:

        calendars_writer = csv.writer(calendars_file, delimiter='\t')
        calendars_writer.writerow(Calendar._fields)

        items_writer = csv.writer(items_file, delimiter='\t')
        items_writer.writerow(Item._fields)

        max_len = max(map(len, calendar_ids))
        pbar = tqdm(calendar_ids)
        for calendar_id in pbar:

            pbar.set_description_str(f'{calendar_id:{max_len}s}')

            calendar, items = crawler.crawl_calendar(year, calendar_id)

            calendars_writer.writerow(calendar)
            items_writer.writerows(items)

    click.echo(datetime.datetime.now())
    click.echo(f'request count: {crawler.request_count}')


@click.command()
@click.argument('year', type=int)
@click.option('--output', '-o', type=Path, default=Path('output'))
@click.option('--category', '-c', type=click.STRING)
def crawl_likers(output, year, category=None):

    click.echo(datetime.datetime.now())

    if category:
        likers_path = output / str(year) / f'likers_{category}.tsv'
    else:
        likers_path = output / str(year) / 'likers.tsv'

    likers_path.parent.mkdir(parents=True, exist_ok=True)

    crawler = CalendarCrawler()

    calendar_ids = [category_id
                    for category_id, *_
                    in crawler.crawl_calendars(year, category)]

    with likers_path.open('w') as f:

        writer = csv.writer(f, delimiter='\t')
        writer.writerow(Liker._fields)

        max_len = max(map(len, calendar_ids))
        pbar = tqdm(calendar_ids)
        for calendar_id in pbar:

            pbar.set_description_str(f'{calendar_id:{max_len}s}')

            likers = crawler.crawl_likers(year, calendar_id)

            writer.writerows(likers)

    click.echo(datetime.datetime.now())
    click.echo(f'request count: {crawler.request_count}')


@click.group()
def cli():
    pass


cli.add_command(crawl_calendars, name='calendars')
cli.add_command(crawl_likers, name='likers')


if __name__ == '__main__':
    cli()
