import configparser
import os

__all__ = ['CONFIG_PATH', 'CONFIG',
           'get_db_args', 'get_sckey',
           'get_video_view_trimmed_batch_conf']

# hard client-side cap on batch size, mirroring the batch worker's default
# MAX_AIDS (aws_lambda_batch.mjs): a larger configured value would make the
# worker answer 400 on every batch and burn the whole run down to fallback
VIDEO_VIEW_TRIMMED_BATCH_SIZE_MAX = 50

# default pool-wide cap on simultaneous batch invocations when the conf
# option is absent. An INITIAL SAFE ASSUMPTION for calibration, not a chosen
# concurrency: it keeps batch-path upstream in-flight (batch_size x this) at
# or below today's ~300 for batch_size <= 10.
VIDEO_VIEW_TRIMMED_BATCH_MAX_CONCURRENT_DEFAULT = 30

# use config parser to load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'conf.ini')
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_PATH)


def get_db_args():
    return dict(CONFIG.items('db_mysql'))


def get_sckey():
    return CONFIG.get('serverchan', 'sckey')


def get_video_view_trimmed_batch_conf() -> tuple[int, float, int]:
    """
    (batch_size, batch_fraction, max_concurrent_batches) for the trimmed
    video_view batch path.

    Fail-safe by construction: a missing conf.ini, missing section, missing
    option, or unparsable/out-of-range value all come back as the disabled
    (0, 0.0, 0) -- the batch path must never turn on by accident, and a
    nonsensical concurrency cap (<= 0 would deadlock the gate) disables it
    rather than guessing. batch_size is capped at the batch worker's MAX_AIDS
    default; batch_fraction is clamped into [0, 1].
    """
    disabled = (0, 0.0, 0)
    try:
        batch_size = CONFIG.getint(
            'video_view_trimmed_batch', 'batch_size', fallback=0)
        batch_fraction = CONFIG.getfloat(
            'video_view_trimmed_batch', 'batch_fraction', fallback=0.0)
        max_concurrent_batches = CONFIG.getint(
            'video_view_trimmed_batch', 'max_concurrent_batches',
            fallback=VIDEO_VIEW_TRIMMED_BATCH_MAX_CONCURRENT_DEFAULT)
    except ValueError:
        return disabled
    if batch_size <= 0 or batch_fraction <= 0.0 or max_concurrent_batches <= 0:
        return disabled
    return (min(batch_size, VIDEO_VIEW_TRIMMED_BATCH_SIZE_MAX),
            min(batch_fraction, 1.0),
            max_concurrent_batches)
