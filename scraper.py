import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator, Optional

from bs4 import BeautifulSoup
from tqdm import tqdm

from habr import Habr

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

DATA_DIR = Path('data')

POSTS_LIST_PATH = DATA_DIR / 'posts.jsonl'
POSTS_DIR = DATA_DIR / 'posts'
SNIPPETS_DELIMITER = '\n\n' + '# ' + '-' * 40 + '\n\n'

habr = Habr()


def fetch_posts(posts_list_path: Path = POSTS_LIST_PATH):
    with posts_list_path.open('w') as out_file:
        for post in tqdm(habr.iter_posts(flow='develop')):
            out_file.write(json.dumps(post) + '\n')
            out_file.flush()


def iter_posts(posts_list_path: Path = POSTS_LIST_PATH) -> Iterator[dict]:
    with posts_list_path.open('r') as in_file:
        for line in in_file:
            yield json.loads(line)


def get_metadata_mapping(posts_list_path: Path = POSTS_LIST_PATH) -> dict[int, str]:
    return {int(post['id']): post for post in iter_posts()}


def extract_python_code_snippets(content: BeautifulSoup) -> list[str]:
    python_codes = content.find_all('code', {'class': 'python'})
    return [code.text for code in python_codes]


def process_post(post_id: int | str, posts_dir: Path = POSTS_DIR):
    out_file = posts_dir / f'{post_id}.py'
    if out_file.exists():
        return

    post_content = habr.get_post_content(post_id)
    codes = extract_python_code_snippets(post_content)
    codes = [code for code in codes if code.count('\n') > 4]
    if not codes:
        return

    out_file.write_text(SNIPPETS_DELIMITER.join(codes))


def iter_python_posts(posts_list_path: Path = POSTS_LIST_PATH) -> Iterator[dict]:
    for post in iter_posts(posts_list_path):
        if any(hub['alias'] == 'python' for hub in post['hubs']):
            yield post


def download_python_snippets(posts_dir: Path = POSTS_DIR, num_threads: Optional[int] = None):
    posts_dir.mkdir(exist_ok=True)
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [
            pool.submit(process_post, post['id'], posts_dir)
            for post in tqdm(iter_python_posts(), desc='submitting jobs')
        ]
        for _ in tqdm(as_completed(futures), desc='processing posts'):
            pass


if __name__ == '__main__':
    download_python_snippets()
