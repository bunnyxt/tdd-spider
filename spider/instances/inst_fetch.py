# _*_ coding: utf-8 _*_

"""
inst_fetch.py by xianhu
"""

import time
import random


class Fetcher(object):
    """
    class of Fetcher, must include function working()
    """

    def __init__(self, sleep_time=0, max_repeat=3):
        """
        constructor
        :param sleep_time: default 0, sleeping time after a fetching
        :param max_repeat: default 3, maximum repeat count of a fetching
        """
        self._sleep_time = sleep_time
        self._max_repeat = max_repeat
        self._fail_urls = None  # by bunnyxt
        return

    def working(self, priority: int, url: str, keys: dict, deep: int, repeat: int, proxies=None) -> (int, object, int):
        """
        working function, must "try, except" and don't change the parameters and returns
        :return fetch_state: can be -1(fetch failed), 0(need repeat), 1(fetch success)
        :return content: can be any object, or exception information[class_name, excep]
        :return proxies_state: can be -1(unavailable), 0(return to queue), 1(available)
        """
        # time.sleep(random.randint(0, self._sleep_time))
        # time.sleep(random.random() * self._sleep_time)  # by bunnyxt
        time.sleep(self._sleep_time)  # by bunnyxt

        try:
            fetch_state, content, proxies_state = self.url_fetch(priority, url, keys, deep, repeat, proxies=proxies)
        except Exception as excep:
            fetch_state, content, proxies_state = (-1 if repeat >= self._max_repeat else 0), [self.__class__.__name__, str(excep)], -1
            # by bunnyxt
            if fetch_state == -1 and self._fail_urls is not None:
                self._fail_urls.append(url)

        return fetch_state, content, proxies_state

    def url_fetch(self, priority: int, url: str, keys: dict, deep: int, repeat: int, proxies=None) -> (int, object, int):
        """
        fetch the content of a url, you must overwrite this function, parameters and returns refer to self.working()
        """
        raise NotImplementedError
