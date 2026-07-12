import logging
import os
import time
from threading import Thread

__all__ = ['SysStatLogger']

logger = logging.getLogger('sysstat')


class SysStatLogger(Thread):
    """
    Sample system-level stats (network throughput, load, available memory)
    every `interval_s` seconds and emit one greppable DEBUG line per sample:

        SYSSTAT rx=2.13Mbps tx=0.41Mbps load1=1.25 mem_avail=812MB

    Reads /proc directly (Linux only; exits silently elsewhere). Daemon thread:
    start() and forget, it dies with the process. Intended to be started only
    when debug logging is enabled, to correlate fetch slowdowns with network /
    CPU / memory pressure.
    """

    def __init__(self, interval_s: float = 10.0):
        super().__init__(daemon=True)
        self.interval_s = interval_s

    @staticmethod
    def _read_net_bytes():
        # sum rx/tx bytes over all interfaces except loopback
        rx, tx = 0, 0
        with open('/proc/net/dev') as f:
            for line in f.readlines()[2:]:
                iface, data = line.split(':', 1)
                if iface.strip() == 'lo':
                    continue
                fields = data.split()
                rx += int(fields[0])
                tx += int(fields[8])
        return rx, tx

    @staticmethod
    def _read_load1():
        with open('/proc/loadavg') as f:
            return float(f.read().split()[0])

    @staticmethod
    def _read_mem_avail_mb():
        with open('/proc/meminfo') as f:
            for line in f:
                if line.startswith('MemAvailable:'):
                    return int(line.split()[1]) // 1024
        return -1

    def run(self):
        if not os.path.exists('/proc/net/dev'):
            logger.debug('SYSSTAT unsupported on this platform (no /proc), sampler exits.')
            return

        prev_rx, prev_tx = self._read_net_bytes()
        prev_ts = time.time()
        while True:
            time.sleep(self.interval_s)
            try:
                rx, tx = self._read_net_bytes()
                now = time.time()
                dt = max(0.001, now - prev_ts)
                rx_mbps = (rx - prev_rx) * 8 / dt / 1e6
                tx_mbps = (tx - prev_tx) * 8 / dt / 1e6
                prev_rx, prev_tx, prev_ts = rx, tx, now
                logger.debug(f'SYSSTAT rx={rx_mbps:.2f}Mbps tx={tx_mbps:.2f}Mbps '
                             f'load1={self._read_load1():.2f} mem_avail={self._read_mem_avail_mb()}MB')
            except Exception as e:
                logger.debug(f'SYSSTAT sample failed: {e}')
