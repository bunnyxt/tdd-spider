from db import Session, DBOperation, TddMemberTotalStatRecord
from util import ts_s_to_str, get_ts_s


def main():
    session = Session()

    # start_ts = 1573761600  # 2019.11.15 04:00:00
    start_ts = 1573848000  # 2019.11.16 04:00:00

    while start_ts <= 1581883200:  # 2020-02-17 04:00:00
        end_ts = start_ts + 30 * 60
        print(ts_s_to_str(get_ts_s()), ts_s_to_str(start_ts), 'to', ts_s_to_str(end_ts))

        # sql = 'select v.aid, v.mid as up_mid, r.view, r.danmaku, r.reply, r.favorite, r.coin, r.share, r.like, ' + \
        #         's.mid as staff_mid from tdd_video v left join tdd_video_staff s on v.aid = s.aid left join ' + \
        #         '(select * from tdd_video_record where added >= %d && added < %d) r on v.aid = r.aid where v.pubdate < %d; ' \
        #             % (start_ts, end_ts, start_ts)
        # result = session.execute(sql)
        # result = list([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]] for r in result)

        sql1 = 'create table tmp_r SELECT * FROM tdd_video_record WHERE added >= %d && added < %d;' % (start_ts, end_ts)
        session.execute(sql1)
        print(ts_s_to_str(get_ts_s()), 'tmp_r page created')

        sql2 = 'select v.aid, v.mid as up_mid, r.view, r.danmaku, r.reply, r.favorite, r.coin, r.share, r.like, ' + \
                's.mid as staff_mid from tdd_video v left join tmp_r r on v.aid = r.aid left join tdd_video_staff s on v.aid = s.aid ' + \
                'where v.pubdate < %d; ' % start_ts
        result = session.execute(sql2)
        result = list([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]] for r in result)

        print(ts_s_to_str(get_ts_s()), 'got aid: ', len(result))
        # remove duplicate aid
        aids = []
        for r in result:
            aid = r[0]
            if aid in aids:
                result.remove(r)
        print(ts_s_to_str(get_ts_s()), 'after remove duplicate aid: ', len(result))

        ws = {}  # key: mid, value: TddMemberTotalStatRecord
        for r in result:
            aid = r[0]
            up_mid = r[1]
            staff_mid = r[9]

            view = r[2]
            danmaku = r[3]
            reply = r[4]
            favorite = r[5]
            coin = r[6]
            share = r[7]
            like = r[8]

            mid = staff_mid if staff_mid else up_mid

            if mid not in ws.keys():
                ws[mid] = TddMemberTotalStatRecord(end_ts, mid)

            ws[mid].video_count += 1
            if view:
                ws[mid].view += view
            if danmaku:
                ws[mid].danmaku += danmaku
            if reply:
                ws[mid].reply += reply
            if favorite:
                ws[mid].favorite += favorite
            if coin:
                ws[mid].coin += coin
            if share:
                ws[mid].share += share
            if like:
                ws[mid].like += like

            # print(aid, mid)

        print(ts_s_to_str(get_ts_s()), 'finish make ws')
        count = 0

        for record in ws.values():
            try:
                session.add(record)
                count += 1
                if count % 100 == 0:
                    session.commit()
                    print(ts_s_to_str(get_ts_s()), count, len(ws))
            except Exception as e:
                print(e)

        sql3 = 'drop table tmp_r;'
        session.execute(sql3)
        session.commit()
        print(ts_s_to_str(get_ts_s()), 'tmp_r page dropped')

        start_ts += 7 * 24 * 60 * 60

        input()

    session.close()


if __name__ == '__main__':
    main()
