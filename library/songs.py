import urllib3
import re
from .util import library_request
from bs4 import BeautifulSoup

__all__ = ['get_songs_list']


def get_base_url(category):
    if category == 'ori':
        return 'http://tianyi.biliran.moe/songs'
    elif category == 'cov':
        return 'http://tianyi.biliran.moe/cov_songs'
    else:
        return None


def get_songs_list_page_num(category):
    if category in ['ori', 'cov']:
        base_url = get_base_url(category)
        if base_url:
            try:
                response = library_request('GET', base_url)
                html = response.data.decode()
                soup = BeautifulSoup(html, 'lxml')
                page_text = soup.find_all("span", "page_text")[0].get_text()
                page_num = int(re.findall(r"(\d+)", page_text)[0])
                return page_num
            except Exception as e:
                print(e)
                return None
    else:
        return None


def get_songs_list(category):
    if category in ['ori', 'cov']:
        base_url = get_base_url(category)
        page_num = get_songs_list_page_num()
        songs_list = []
        if base_url and page_num:
            for p in range(1, page_num + 1):
                response = library_request('GET', '{0}/p/{1}'.format(base_url, p))
                if response:
                    html = response.data.decode()
                    soup = BeautifulSoup(html, 'lxml')
                    trs = soup.find_all("tr")
                    for i in range(1, len(trs)):  # start from 1 to ignore header line of table
                        tds = trs[i].find_all("td")
                        id_ = int(tds[0].get_text())
                        aid = int(tds[1].get_text()[2:])
                        title = tds[2].get_text()
                        songs_list.append([id_, aid, title])
                else:
                    # TODO change to avoid so many return None
                    pass
        else:
            return None
    else:
        return None
