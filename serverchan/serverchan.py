import urllib3
import json
from conf import get_sckey

__all__ = ['sc_send']

http = urllib3.PoolManager()


def sc_send(text, desp=None):
    obj = None
    try:
        sckey = get_sckey()
        url = 'http://sc.ftqq.com/%s.send' % sckey
        fields = {'text': text}
        if desp:
            fields['desp'] = desp
        response = http.request('GET', url, fields=fields)
        if response.status == 200:
            html = response.data.decode()
            obj = json.loads(html)
    except Exception as e:
        print(e)
    return obj
