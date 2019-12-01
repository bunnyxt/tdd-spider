from db import DBOperation, Session
from common import add_member
from pybiliapi import BiliApi
import time


def add_not_added_members(mids, bapi, session):
    total_count = len(mids)
    success_count = 0
    fail_count = 0
    fail_mids = []
    for mid in mids:
        result = add_member(mid, bapi, session, test_exist=True)
        if result == 0:
            print('Mid %d: Success!' % mid)
            success_count += 1
        else:
            if result == 1:
                print('Mid %d: Fail! Member already exist!' % mid)
            elif result == 2:
                print('Mid %d: Fail! Fail to get valid member_obj!' % mid)
            elif result == 3:
                print('Mid %d: Fail! member_obj code != 0' % mid)
            else:
                print('Mid %d: Fail! Unknown result code %d' % (mid, result))
            fail_count += 1
            fail_mids.append(mid)
        time.sleep(0.2)

    print('Done! Total: %d Success: %d Fail: %d' % (total_count, success_count, fail_count))
    if fail_count > 0:
        print('Fail mids: %r' % fail_mids)


def main():
    bapi = BiliApi()
    session = Session()

    # 01 add video members
    print('Now get not added video members...')
    mids = DBOperation.query_not_added_video_member_mids(session)

    print('%d mid(s) get.' % len(mids))
    print(mids)

    add_not_added_members(mids, bapi, session)

    # 02 add video staffs
    print('Now get not added video staffs...')
    mids = DBOperation.query_not_added_video_staff_mids(session)

    print('%d mid(s) get.' % len(mids))
    print(mids)

    add_not_added_members(mids, bapi, session)

    session.close()


if __name__ == '__main__':
    main()
