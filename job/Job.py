from threading import Thread
import logging

logger = logging.getLogger('Job')

__all__ = ['Job']


class Job(Thread):
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self.logger = logging.getLogger(f'{self.__class__.__name__}.{self.name}')

    def run(self):
        self.logger.info(f'Job start.')
        self.logger.info(f'Job end.')
