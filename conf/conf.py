import configparser
import os

__all__ = ['CONFIG_PATH', 'CONFIG',
           'get_db_args', 'get_sckey',
           'get_video_view_trimmed_batch_conf']

# hard client-side cap on batch size, mirroring the batch worker's default
# MAX_AIDS (aws_lambda_batch.mjs): a larger configured value would make the
# worker answer 400 on every batch and burn the whole run down to fallback
VIDEO_VIEW_TRIMMED_BATCH_SIZE_MAX = 50

# use config parser to load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'conf.ini')
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)


def get_db_args():
    return dict(CONFIG.items('db_mysql'))


def get_sckey():
    return CONFIG.get('serverchan', 'sckey')


def get_video_view_trimmed_batch_conf() -> tuple[int, float]:
    """
    (batch_size, batch_fraction) for the trimmed video_view batch path.

    Fail-safe by construction: a missing conf.ini, missing section, missing
    option, or unparsable/out-of-range value all come back as the disabled
    (0, 0.0) -- the batch path must never turn on by accident. batch_size is
    capped at the batch worker's MAX_AIDS default; batch_fraction is clamped
    into [0, 1].
    """
    try:
        batch_size = CONFIG.getint(
            'video_view_trimmed_batch', 'batch_size', fallback=0)
        batch_fraction = CONFIG.getfloat(
            'video_view_trimmed_batch', 'batch_fraction', fallback=0.0)
    except ValueError:
        return 0, 0.0
    if batch_size <= 0 or batch_fraction <= 0.0:
        return 0, 0.0
    return min(batch_size, VIDEO_VIEW_TRIMMED_BATCH_SIZE_MAX), min(batch_fraction, 1.0)
