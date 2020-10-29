from pybiliapi import BiliApi
import math
from util import get_ts_s
from common import get_valid, test_archive_rank_by_partion
import logging  # TODO refactor total logging system
import time

__all__ = ['iter_get_archive_rank_by_partion']


def iter_get_archive_rank_by_partion(tid, iter_func, colddown=0.1):
    bapi = BiliApi()

    last_page_aids = []  # aids occurred in last page
    this_page_aids = []  # aids occurred in this page

    # get page total
    obj = bapi.get_archive_rank_by_partion(tid, 1, 50)
    page_total = math.ceil(obj['data']['page']['count'] / 50)
    logging.info('%d page(s) found' % page_total)

    # iter loop
    page_num = 1
    while page_num <= page_total:
        try:
            # get obj via get_archive_rank_by_partion api
            obj = get_valid(bapi.get_archive_rank_by_partion, (30, page_num, 50), test_archive_rank_by_partion)
            if obj is None:
                logging.warning('Page num %d fail! Cannot get valid obj.' % page_num)
                page_num += 1
                continue

            added = get_ts_s()
            for item in obj['data']['archives']:
                aid = item['aid']
                if aid in last_page_aids:
                    # aid occurred in last page, continue
                    logging.warning('Aid %d occurred in last page (page_num = %d).' % (aid, page_num - 1))
                    continue

                iter_func(item, {added: added})  # iter_item, iter_context

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
