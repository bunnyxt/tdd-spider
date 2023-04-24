from service import Service, ServiceError
from sqlalchemy.orm.session import Session
from db import DBOperation, TddVideoRecord
from util import get_ts_s

__all__ = ['add_video_record']


def add_video_record(aid: int, service: Service, session: Session) -> TddVideoRecord:
    # get video stat
    try:
        video_stat = service.get_video_stat({'aid': aid})
    except ServiceError as e:
        raise e

    # assemble video record
    new_video_record = TddVideoRecord(
        aid=aid,
        added=get_ts_s(),
        view=-1 if video_stat.view == '--' else video_stat.view,
        danmaku=video_stat.danmaku,
        reply=video_stat.reply,
        favorite=video_stat.favorite,
        coin=video_stat.coin,
        share=video_stat.share,
        like=video_stat.like,
        dislike=video_stat.dislike,
        now_rank=video_stat.now_rank,
        his_rank=video_stat.his_rank,
        vt=video_stat.vt,
        vv=video_stat.vv,
    )

    # add to db
    # TODO: use new db operation which can raise exception
    DBOperation.add(new_video_record, session)

    return new_video_record




def add_staff(added: int, aid: int, mid: int, title: str, session: Session, test_exist=True):
    bvid = a2b(aid)

    # test exist
    if test_exist:
        staff = DBOperation.query_video_staff_via_aid_mid(aid, mid, session)
        if staff is not None:
            # staff already exist
            raise AlreadyExistError(table='video_staff', params={'aid': aid, 'mid': mid})

    # assemble staff
    new_staff = TddVideoStaff(
        added=added,
        aid=aid,
        bvid=bvid,
        mid=mid,
        title=title
    )

    # add to db
    # TODO: use new db operation which can raise exception
    DBOperation.add(new_staff, session)

    return new_staff
