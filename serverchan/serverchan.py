import requests
import json
from conf import get_sckey
from timer import Timer
from job import JobStat
from typing import Optional
from util import get_ts_s_str
import logging

logger = logging.getLogger('serverchan')

__all__ = ['sc_send', 'sc_send_summary', 'sc_send_critical']

send_url = f'http://sc.ftqq.com/{get_sckey()}.send'


def sc_send(text: str, desp: Optional[str] = None):
    response = None

    # assemble message
    message = {'text': text}
    if desp:
        message['desp'] = desp

    try:
        # send message
        r = requests.post(send_url, headers={'Content-Type': 'application/json'}, data=json.dumps(message))
        r.raise_for_status()

        # check response
        response = r.json()
        if type(response) == dict \
                and 'data' in response.keys() and type(response['data']) == dict \
                and 'errno' in response['data'].keys() and response['data']['errno'] == 0:
            logger.debug(f'SC send successfully! message: {message}, response: {response}')
        else:
            logger.warning(f'SC send failed! Unexpected response got. message: {message}, response: {response}')
    except Exception as e:
        logger.warning(f'SC send failed! Exception caught. message: {message}, exception: {e}')
    finally:
        return response


def sc_send_summary(script_fullname: str, timer: Timer, stat: JobStat):
    title = f'SUMMARY: {script_fullname}'
    desc = '\n\n'.join([
        f'# {script_fullname} done!',
        timer.get_summary(),
        stat.get_summary('\n\n'),
        f'by bunnyxt, {get_ts_s_str()}'
    ])
    sc_send(title, desc)


def sc_send_critical(script_fullname: str, message: str, file_name: str, line_no: str):
    title = f'CRITICAL: {script_fullname}'
    desc = '\n\n'.join([
        message,
        f'file: {file_name}, line: {line_no}',
        f'by bunnyxt, {get_ts_s_str()}'
    ])
    sc_send(title, desc)
