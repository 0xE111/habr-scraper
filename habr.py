from dataclasses import dataclass, field
from functools import partialmethod
from logging import getLogger
from typing import ClassVar, Iterator, List

import requests
from bs4 import BeautifulSoup
from tenacity import TryAgain, retry, stop_after_attempt, wait_incrementing

log = getLogger(__name__)


class TooManyIterations(Exception):
    pass


@dataclass
class Habr:
    session: requests.Session = field(default_factory=requests.Session)

    SITE_URL: ClassVar[str] = 'https://habr.com'
    API_URL: ClassVar[str] = 'https://habr.com/kek/v2'
    TIMEOUT: ClassVar[int] = 10

    @retry(
        reraise=True,
        wait=wait_incrementing(start=1, increment=2),
        stop=stop_after_attempt(20),
    )
    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        if path.startswith('/'):
            path = self.BASE_URL + path

        kwargs.setdefault('timeout', self.TIMEOUT)
        response = self.session.request(method, path, **kwargs)

        if response.status_code == requests.codes.too_many_requests:
            raise TryAgain()

        return response

    get = partialmethod(request, 'get')
    post = partialmethod(request, 'post')

    def iter_posts(self, flow: str = 'develop') -> Iterator[dict]:
        max_page = 10000
        for page in range(1, max_page):
            log.debug(f'Scraping posts page {page}')
            posts = self.get_posts(flow=flow, page=page)
            if not posts:
                return

            yield from posts
        else:
            raise TooManyIterations()

    def get_posts(self, flow: str, page: int = 1) -> List[dict]:
        response = self.get(
            f'{self.API_URL}/articles/',
            params={
                'flow': flow,
                'sort': 'all',
                'page': page,
                'fl': 'ru',
                'hl': 'ru',
            },
        )
        if not response.ok:
            if response.status_code == requests.codes.not_found:
                return []

            response.raise_for_status()

        result = response.json()
        return result['articleRefs'].values()

    def get_post(self, id_: int | str) -> str:
        return self.get(f'{self.SITE_URL}/ru/post/{id_}')

    def get_post_content(self, id_: int | str) -> BeautifulSoup:
        html = self.get_post(id_).text
        soup = BeautifulSoup(html, features='html.parser')
        content = soup.find('div', {'id': 'post-content-body'})
        assert content
        return content
