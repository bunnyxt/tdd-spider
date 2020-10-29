from pybiliapi import BiliApi
import math
from util import get_ts_s
from common import get_valid, test_archive_rank_by_partion
import logging  # TODO refactor total logging system
import time

__all__ = ['iter_get_archive_rank_by_partion']


def iter_get_archive_rank_by_partion(tid, iter_func, colddown=0.1):
    """

    :param tid: api param, category id
    :param iter_func: function to process each iter item, format: iter_func(iter_item, **iter_context)
    :param colddown: api request interval colddown
    :return:
    """
    bapi = BiliApi()

    last_page_aids = []  # aids occurred in last page
    this_page_aids = []  # aids occurred in this page

    # get page total
    page_obj = bapi.get_archive_rank_by_partion(tid, 1, 50)
    page_total = math.ceil(page_obj['data']['page']['count'] / 50)
    logging.info('%d page(s) found' % page_total)

    # iter loop
    page_num = 1
    while page_num <= page_total:
        try:
            # get page_obj via get_archive_rank_by_partion api
            page_obj = get_valid(bapi.get_archive_rank_by_partion, (30, page_num, 50), test_archive_rank_by_partion)
            if page_obj is None:
                logging.warning('Page num %d fail! Cannot get valid obj.' % page_num)
                page_num += 1
                continue

            added = get_ts_s()  # api response ts (approximate)

            # page_obj items iter loop
            for iter_item in page_obj['data']['archives']:
                aid = iter_item['aid']
                if aid in last_page_aids:
                    # aid occurred in last page, skip
                    logging.warning('Aid %d occurred in last page (page_num = %d).' % (aid, page_num - 1))
                    continue

                iter_func(iter_item, added=added)  # iter_item, **iter_context

                this_page_aids.append(aid)

            # assign this page aids to last page aids and reset it
            last_page_aids = this_page_aids
            this_page_aids = []

            if page_num % 100 == 0:
                logging.info('Awesome api fetch %d / %d done' % (page_num, page_total))
        except Exception as e:
            logging.error('Awesome api fetch %d / %d error, Exception caught. Detail: %s' % (page_num, page_total, e))
        finally:
            page_num += 1
            time.sleep(colddown)
    logging.info('Awesome api fetch %d / %d done' % (page_num - 1, page_total))
