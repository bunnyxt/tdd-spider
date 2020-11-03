from pybiliapi import BiliApi
import math
from util import get_ts_s
from common import get_valid, test_archive_rank_by_partion
import logging  # TODO refactor total logging system
import time

__all__ = ['iter_get_archive_rank_by_partion']


def iter_get_archive_rank_by_partion(tid, iter_func, context, start_page_num=1, colddown=0):
    """
    iter get archive rank by partion api
    :param tid: api param, category id
    :param iter_func: function to process each iter item, format: iter_func(iter_item, context, **iter_context)
    :param context: context passed into iter_func
    :param start_page_num: iter loop start page num
    :param colddown: api request interval colddown
    :return:
    """
    logging.info('Now iter get archive rank by partion api...')
    logging.info('args: ')
    logging.info('-- tid: %d' % tid)
    logging.info('-- iter_func: %r' % iter_func)
    logging.info('-- context: %r' % context)
    logging.info('-- start_page_num: %d' % start_page_num)
    logging.info('-- colddown: %d' % colddown)

    bapi = BiliApi()

    last_page_aids = []  # aids occurred in last page
    this_page_aids = []  # aids occurred in this page

    # get page total
    page_obj = bapi.get_archive_rank_by_partion(tid, 1, 50)
    page_total = math.ceil(page_obj['data']['page']['count'] / 50)
    logging.info('Found %d page(s) in total' % page_total)

    # iter loop
    page_num = start_page_num
    while page_num <= page_total:
        try:
            # get page_obj via get_archive_rank_by_partion api
            page_obj = get_valid(bapi.get_archive_rank_by_partion, (30, page_num, 50), test_archive_rank_by_partion)
            if page_obj is None:
                logging.warning('Page num %d fail! Cannot get valid obj' % page_num)
                page_num += 1
                continue

            added = get_ts_s()  # api response ts (approximate)

            # update page total
            page_total_new = math.ceil(page_obj['data']['page']['count'] / 50)
            if page_total_new > page_total:
                logging.info('Update page total num from %d to %d' % (page_total, page_total_new))
                page_total = page_total_new

            # page_obj items iter loop
            for iter_item in page_obj['data']['archives']:
                aid = iter_item['aid']
                if aid in last_page_aids:
                    # aid occurred in last page, skip
                    logging.info('Aid %d occurred in last page (page_num = %d)' % (aid, page_num - 1))
                    continue

                iter_func(iter_item, context, added=added)  # iter_item, context, **iter_context

                this_page_aids.append(aid)

            # assign this page aids to last page aids and reset it
            last_page_aids = this_page_aids
            this_page_aids = []

            logging.debug('Awesome api page %d / %d done' % (page_num, page_total))
            if page_num % 100 == 0:
                logging.info('Awesome api page %d / %d done' % (page_num, page_total))
        except Exception as e:
            logging.error('Awesome api page %d / %d error, Exception caught. Detail: %s' % (page_num, page_total, e))
        finally:
            page_num += 1
            time.sleep(colddown)

    if (page_num - 1) % 100 != 0:
        logging.info('Awesome api fetch %d / %d done' % (page_num - 1, page_total))
    logging.info('Finish iter get archive rank by partion api')
