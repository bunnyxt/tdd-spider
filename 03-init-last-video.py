from db import Session, DBOperation


def main():
    session = Session()
    mids = DBOperation.query_all_member_mids(session)

    for mid in mids:
        session.execute(
            'update tdd_member set last_video = (select id from tdd_video where aid in ( select distinct(v.aid) from tdd_video v left join tdd_video_staff s on v.aid = s.aid where v.mid = %d || s.mid = %d ) order by pubdate desc limit 1 ) where mid = %d;' % (
            mid, mid, mid))
        session.commit()
        print(mid)

    session.close()


if __name__ == '__main__':
    main()
