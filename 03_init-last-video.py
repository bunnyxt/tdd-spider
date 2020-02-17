from db import Session, DBOperation


def main():
    session = Session()
    mids = DBOperation.query_all_member_mids(session)

    # method 1
    # for mid in mids:
    #     session.execute(
    #         'update tdd_member set last_video = (select id from tdd_video where aid in ( select distinct(v.aid) from tdd_video v left join tdd_video_staff s on v.aid = s.aid where v.mid = %d || s.mid = %d ) order by pubdate desc limit 1 ) where mid = %d;' % (
    #         mid, mid, mid))
    #     session.commit()
    #     print(mid)

    # method 2
    ws = {}
    fail_mids = set()
    for mid in mids:
        ws[mid] = [0, 0]  # mid, id(tdd_video), pubdate
    result = session.execute('select v.id, v.mid, v.pubdate, s.mid from tdd_video v left join tdd_video_staff s on v.aid = s.aid;')
    result = list([(r[0], r[1], r[2], r[3]) for r in result])
    for r in result:
        id = r[0]
        pubdate = r[2]
        mids = [r[1]]
        if r[3]:
            mids.append(r[3])
        for mid in mids:
            if mid not in ws.keys():
                fail_mids.add(mid)
                continue
            if pubdate is not None and pubdate > ws[mid][1]:
                ws[mid][1] = pubdate
                ws[mid][0] = id

    print(fail_mids)

    for key, value in ws.items():
        # if key <= 1930014:
        #     continue
        id = value[0]
        pubdate = value[1]
        if id != 0 and pubdate != 0:
            session.execute('update tdd_member set last_video = %d where mid = %d' % (value[0], key))
            session.commit()
            print(key, value[0])

    session.close()


if __name__ == '__main__':
    main()
