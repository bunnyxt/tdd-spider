import requests
from conf import get_proxy_pool_url
from typing import Optional
import logging

logger = logging.getLogger('proxypool')

__all__ = ['proxy_pool_url', 'get_proxy_url']

# connect to your own deployed ProxyPool
# ref: https://github.com/Python3WebSpider/ProxyPool
proxy_pool_url = get_proxy_pool_url()


def get_proxy_url(https: bool = False) -> Optional[str]:
    try:
        r = requests.get(proxy_pool_url)
        if r.status_code != 200:
            r.raise_for_status()
        proxy_url = r.text
        return 'http%s://%s' % ('s' if https else '', proxy_url)
    except Exception as e:
        logger.warning(f'Fail to get proxy url from proxy pool {proxy_pool_url}. Error: {e}')
        return None


# test when import
if get_proxy_url() is None:
    logger.critical(f'Fail to get proxy url from proxy pool {proxy_pool_url}. Please check your proxy pool.')
    exit(1)
