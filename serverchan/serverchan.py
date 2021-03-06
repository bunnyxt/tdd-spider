import urllib3
import json
from conf import get_sckey
import logging
logger = logging.getLogger('serverchan')

__all__ = ['sc_send']

http = urllib3.PoolManager()


def sc_send(text, desp=None):
    sc_response = None

    # assemble message
    message = {'text': text}
    if desp:
        message['desp'] = desp

    try:
        sckey = get_sckey()
        url = 'http://sc.ftqq.com/%s.send' % sckey
        response = http.request('GET', url, fields=message)
        if response.status == 200:
            html = response.data.decode()
            sc_response = json.loads(html)
            if sc_response['errno'] == 0:
                logger.debug('Successfully send message %s. sc_response: %s' % (message, sc_response))
            else:
                logger.warning('Fail to send message %s. sc_response: %s' % (message, sc_response))
    except Exception as e:
        logger.warning('Exception occurred when send message %s. Detail: %s' % (message, e))
    finally:
        return sc_response
