import re
import time
from bs4 import BeautifulSoup
from .util import library_request

__all__ = ['SongsHtmlData', 'get_base_url', 'abstract_data_from_songs_html', 'get_songs_list_page_total',
           'get_songs_list', 'get_songs_lists']


class SongsHtmlData:

    def __init__(self):
        self.songs_list = []
        self.page_now = -1
        self.page_total = -1
        self.record_total = -1
        self.record_now_start = -1
        self.record_now_end = -1


def get_base_url(category):
    if category == 'ori':
        return 'http://tianyi.biliran.moe/songs'
    elif category == 'cov':
        return 'http://tianyi.biliran.moe/cov_songs'
    else:
        return None


def abstract_data_from_songs_html(html):
    data = SongsHtmlData()
    soup = BeautifulSoup(html, 'lxml')

    # songs_list
    songs_list = []
    try:
        trs = soup.find_all("tr")
        for i in range(1, len(trs)):  # start from 1 to ignore header line of table
            tds = trs[i].find_all("td")
            id_ = int(tds[0].get_text())
            aid = int(tds[1].get_text()[2:])
            title = tds[2].get_text()
            songs_list.append([id_, aid, title])
    except Exception as e:
        print(e)
    finally:
        data.songs_list = songs_list

    # page_now
    try:
        input_page = soup.find("input", "input_page")
        page_now = int(input_page['value'])
        data.page_now = page_now
    except Exception as e:
        print(e)

    # page_total
    try:
        page_text = soup.find("span", "page_text")
        page_total = int(re.findall(r"(\d+)", page_text.get_text())[0])
        data.page_total = page_total
    except Exception as e:
        print(e)

    try:
        system_information = soup.find("div", "systemInformation")

        # record_total
        record_total = int(re.findall(r"共有(\d+)条记录", system_information.get_text())[0])
        data.record_total = record_total

        # record_now_start record_now_end
        if record_total > 0:
            (record_now_start, record_now_end) = re.findall(r"当前显示第(\d+)条-第(\d+)条", system_information.get_text())[0]
            (record_now_start, record_now_end) = (int(record_now_start), int(record_now_end))
            data.record_now_start = record_now_start
            data.record_now_end = record_now_end
    except Exception as e:
        print(e)

    return data


def get_songs_list_page_total(category):
    if category in ['ori', 'cov']:
        base_url = get_base_url(category)
        if base_url:
            response = library_request('GET', base_url)
            if response:
                html = response.data.decode()
                data = abstract_data_from_songs_html(html)
                page_total = data.page_total
                return page_total
    else:
        return None


def get_songs_list(category, p, title=None, aid=None):
    if category in ['ori', 'cov']:
        base_url = get_base_url(category)
        if base_url:
            if title or aid:
                fields = {
                    'uft8': '✓',
                    'authenticity_token': 'some_token_unknown',  # TODO finish POST query mode
                    'is_crp_sch': 't',
                    'name': title if title else '',
                    'av': aid if aid else '',
                }
                response = library_request('POST', '{0}/p/{1}'.format(base_url, p), fields)
            else:
                response = library_request('GET', '{0}/p/{1}'.format(base_url, p))
            if response:
                html = response.data.decode()
                data = abstract_data_from_songs_html(html)
                songs_list = data.songs_list
                return songs_list
    else:
        return None


def get_songs_lists(category, title=None, aid=None):
    if category in ['ori', 'cov']:
        base_url = get_base_url(category)
        page_total = get_songs_list_page_total(category)
        songs_lists = []
        if base_url and page_total:
            for p in range(1, page_total + 1):
                songs_list = get_songs_list(category, p, title, aid)
                songs_lists += songs_list
                time.sleep(0.2)
            return songs_lists
    else:
        return None
