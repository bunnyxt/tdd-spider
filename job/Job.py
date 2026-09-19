from threading import Thread
from .JobStat import JobStat
import logging

logger = logging.getLogger('Job')

__all__ = ['Job']


class Job(Thread):
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self.stat = JobStat()
        self.logger = logging.getLogger(f'{self.__class__.__name__}.{self.name}')

    def run(self):
        self.logger.info('Job start.')

        self.process()

        self.logger.info('Job end.')

        self.cleanup()

    def process(self):
        self.logger.debug('Override this method for customizing job process.')

    def cleanup(self):
        self.logger.debug('Override this method for customizing clean up, e.g. close session.')
