import urllib3
import json
from conf import get_sckey
from timer import Timer
from job import JobStat
from util import get_ts_s_str
import logging

logger = logging.getLogger('serverchan')

__all__ = ['sc_send', 'sc_send_summary', 'sc_send_critical']

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
        response = http.request('POST', url, body=json.dumps(message), headers={'Content-Type': 'application/json'})
        if response.status == 200:
            html = response.data.decode()
            sc_response = json.loads(html)
            if type(sc_response) == dict \
                    and 'data' in sc_response.keys() and type(sc_response['data']) == dict \
                    and 'errno' in sc_response['data'].keys() and sc_response['data']['errno'] == 0:
                logger.debug('Successfully send message %s. sc_response: %s' % (message, sc_response))
            else:
                logger.warning('Fail to send message %s. sc_response: %s' % (message, sc_response))
    except Exception as e:
        logger.warning('Exception occurred when send message %s. Detail: %s' % (message, e))
    finally:
        return sc_response


def sc_send_summary(script_id: str, script_name: str, timer: Timer, stat: JobStat):
    title = f'SUMMARY: {script_id} - {script_name})'
    desc = f'# {script_id} - {script_name} done!\n\n' \
           f'{timer.get_summary()}\n\n' \
           f'{stat.get_summary()}\n\n' \
           f'by bunnyxt, {get_ts_s_str()}'
    sc_send(title, desc)


def sc_send_critical(title, message, file_name, line_no):
    desc = f'{message}\n\n' \
           f'file: {file_name}, line: {line_no}'
    sc_send('CRITICAL: %s' % title, desc)
