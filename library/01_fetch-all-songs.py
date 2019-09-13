import urllib3
import time
import re
import sys
import os
from bs4 import BeautifulSoup


def fetch_pages(song_type, http, header):
    if song_type == 'ori':
        base_url = 'http://tianyi.biliran.moe/songs/p/'
    elif song_type == 'cov':
        base_url = 'http://tianyi.biliran.moe/cov_songs/p/'
    else:
        raise Exception("Invalid param song_type: {0}!".format(type))

    response = http.request('GET',
                            base_url + "1",
                            headers=header)
    html = response.data.decode()
    soup = BeautifulSoup(html, 'lxml')
    soup = soup.find_all("span", "page_text")[0]
    pages_text = soup.get_text()
    pages = int(re.findall(r"(\d+)", pages_text)[0])
    return pages


def fetch_song(song_type, http, header, min_aid):
    print('Now start fetch {0} song with limit {1}...'.format(song_type, min_aid))

    if song_type == 'ori':
        base_url = 'http://tianyi.biliran.moe/songs/p/'
    elif song_type == 'cov':
        base_url = 'http://tianyi.biliran.moe/cov_songs/p/'
    else:
        raise Exception("Invalid param song_type: {0}!".format(song_type))

    # get pages
    pages = fetch_pages(song_type, http, header)
    print('Total {0} pages found!'.format(pages))

    # create file
    filename = '01_{0}-songs.csv'.format(song_type)
    if os.path.exists(filename):
        os.remove(filename)
    f = open(filename, "a")
    print('Result will be stored in {0}.'.format(filename))

    print('Now start fetching {0} songs...'.format(song_type))
    count = 0
    # for each page
    for p in range(1, pages + 1):
        print('Page {0}/{1}'.format(p, pages))
        response = http.request('GET',
                                base_url + str(p),
                                headers=header)
        html = response.data.decode()
        soup = BeautifulSoup(html, 'lxml')
        trs = soup.find_all("tr")
        # process each song
        for i in range(1, len(trs)):
            tds = trs[i].find_all("td")
            aid = tds[1].get_text()[2:]
            if int(aid) <= min_aid:
                print("Meet aid {0} <= {1} limit, stop".format(aid, min_aid))
                break
            title = tds[2].get_text()
            line = '{0},"{1}"'.format(aid, title)
            # line = eval(repr(line).replace('\\', '\\\\'))
            print(line)
            f.write(line + "\n")
            count += 1
        else:
            print("{0} / {1} done!".format(p, pages))
            time.sleep(0.2)
            continue
        break
    f.close()
    print("Finish fetching {0} {1} songs from {2} pages with min aid {3}!".format(count, song_type, pages, min_aid))
    return count


def main():
    start_time = time.time()

    print('Now start 01_fetch-all-songs at {0}...'.format(
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))))

    # set params
    cookie = '_luo_session=BAh7CSIJYXBwc3sGaQZ7CDoIY21uIhHljobmrKHnu5Pmnpw6DW1lbnVfaWRzWxRpFGkQaRhpDWkPaRdpE2kWaRlpCGkMaRVpDmkHaQo6CGNtaWkWIhBfY3NyZl90b2tlbiIxSFBULzZGLzFGektjS1U3SFBoU1RQUzhtb3hFU05TUlRlblgzcmN4OGlpTT0iCXVzZXJ7CDoJbmFtZSIO54mb5aW25rqQOgdpZGlVOg1yb2xlX2lkc1sJaQdpCGkJaQoiD3Nlc3Npb25faWQiJWZhODlhNmNkYWRhZjNiZjcyMzI1ZTE5NDJiNGE4NTc0--139161867e3596bf29867a57dc5f8c6ce58e296e'
    if len(sys.argv) > 1:
        cookie = sys.argv[1]
    http = urllib3.PoolManager()
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)\
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
        # reset cookie when it become invalid
        'Cookie': cookie
    }

    # test cookie validity
    print('Now test validity of cookie...')
    response = http.request('GET',
                            'http://tianyi.biliran.moe/yuezheng',
                            headers=header)
    url = response.geturl()
    if url == 'http://tianyi.biliran.moe/yuezheng':
        print('Passed!')
    else:
        print('Error!\nCookie {0} is invalid, please assign a new cookie.'.format(cookie[:30] + '...'))
        exit(-1)

    # fetch song
    ori_min_aid = 0
    cov_min_aid = 0
    ori_count = fetch_song('ori', http, header, ori_min_aid)
    cov_count = fetch_song('cov', http, header, cov_min_aid)

    end_time = time.time()

    print("Finish fetching all songs at {0}!".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))))
    print("Total {0} songs get, including {1} ori and {2} cov.".format(ori_count + cov_count, ori_count, cov_count))
    print("Time cost: {0}".format(end_time - start_time))


if __name__ == '__main__':
    main()
