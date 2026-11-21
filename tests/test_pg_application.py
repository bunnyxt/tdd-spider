"""Opt-in PostgreSQL application writes against a disposable database clone.

Set TDD_PG_INTEGRATION=1 and select a database INI with TDD_CONFIG_PATH. Tests append
synthetic observations and intentionally leave their manifests for inspection.
They never contact an HTTP endpoint.
"""
import json
import logging
import os
from pathlib import Path
from queue import Queue
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
from uuid import uuid4

from core import MemberFollowerRecord,VideoRecord
from db import TddVideo, TddVideoStaff
from db.basic import engine
from db.queries import query_video_via_aid, query_member_via_mid
from db.profiles import insert_video
from psycopg.rows import dict_row
from db.values import timestamp, epoch
from job import FetchMemberFollowerRecordJob,BatchInsertMemberFollowerRecordJob
from service import CodeError,MemberCard,VideoView,VideoViewOwner,VideoViewStaffItem,VideoViewStat,VideoTags
from task import add_video,update_video,add_member,update_member,commit_video_records_batch,commit_member_follower_records_batch
from util import a2b
from util.member_total_stat import aggregate_member_stats
from util.member_total_stat_writer import write_member_stats

ADDED = 1794600000
LOG = logging.getLogger('test.pg.application')


def read_rows(statement, params=()):
    # Test-only SQL for independent inspection, after application commit.
    with engine.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(statement, params)
        return [{name: epoch(value) for name, value in row.items()} for row in cursor.fetchall()]


def read_value(statement, params=()):
    with engine.connection() as conn:
        return epoch(conn.execute(statement, params).fetchone()[0])


def read_staff(aid):
    return [TddVideoStaff(**row) for row in read_rows('SELECT * FROM tdd_video_staff WHERE aid=%s', (aid,))]



@unittest.skipUnless(os.environ.get('TDD_PG_INTEGRATION') == '1','requires an explicitly selected disposable PostgreSQL database')
class PostgresApplicationTest(unittest.TestCase):
    def setUp(self):
        self.run_id = uuid4()
        self.aid = 800000000000000 + self.run_id.int % 1000000000000
        self.mid = 9000000000000 + self.run_id.int % 1000000000
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.environment = mock.patch.dict(os.environ,{'TDD_TRIAL_RUN_ID':str(self.run_id),'TDD_FAILURE_DIR':self.directory.name})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.clock = mock.patch('task.task.get_ts_s',return_value=ADDED)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.service = mock.Mock()
        self.service.get_member_card.side_effect = lambda params,**kwargs: MemberCard(params['mid'],'member','','','sign\x00')
        self.service.get_video_tags.return_value = VideoTags([])
        self.service.get_video_view.return_value = self.view()

    def expected_manifest(self, counts):
        # The writer can append to the manifest, but cannot read or delete it.
        # An independent privileged observer verifies these expected counts.
        path = os.environ.get('TDD_PG_EVIDENCE_FILE')
        result = {'test':self._testMethodName,'run_id':str(self.run_id),'expected_manifest':counts}
        if path:
            with open(path,'a',encoding='utf-8') as stream:
                stream.write(json.dumps(result,sort_keys=True)+'\n')

    def view(self,aid=None,mid=None,view=10,staff=True,pubdate=100):
        aid = self.aid if aid is None else aid
        mid = self.mid if mid is None else mid
        return VideoView(bvid='BV'+a2b(aid),aid=aid,videos=1,tid=30,tname='music',copyright=1,
                         pic='',title='ti\x00tle',pubdate=pubdate,ctime=pubdate,desc='de\x00sc\\u0000',state=0,duration=60,
                         owner=VideoViewOwner(mid,'owner',''),
                         stat=VideoViewStat(aid,view,1,2,3,4,5,0,0,6,0,None,None),attribute=0,forward=None,
                         staff=[VideoViewStaffItem(mid+1,'作\x00曲','staff','')] if staff else None)

    def test_profile_relationship_log_and_derived_reference_share_one_transaction(self):
        add_video(self.aid,self.service)
        video = query_video_via_aid(self.aid)
        self.assertEqual((video.title,video.desc,video.added),('title','desc\\u0000',ADDED))
        for mid in (self.mid,self.mid+1):
            self.assertEqual(query_member_via_mid(mid).last_video_aid,self.aid)
        self.assertEqual(read_value('SELECT added FROM tdd_video_record_latest WHERE aid=%s',(self.aid,)),ADDED)
        self.service.get_video_view.return_value = self.view()._replace(title='changed',state=-4,staff=[])
        with mock.patch('db.profiles._append_objects',side_effect=RuntimeError('injected log failure')):
            with self.assertRaisesRegex(RuntimeError,'injected log failure'):
                update_video(self.aid,self.service)
        current = query_video_via_aid(self.aid)
        self.assertEqual((current.title,current.state),('title',0))
        self.assertEqual(len(read_staff(self.aid)),1)
        self.assertEqual(query_member_via_mid(self.mid).last_video_aid,self.aid)
        pending = list(Path(self.directory.name).glob('*.json'))
        self.assertEqual(len(pending),1)
        # The injected log failure proved rollback; retry the original business
        # operation explicitly. The failure diagnostic is retained for review.
        update_video(self.aid,self.service)
        self.assertTrue(pending[0].exists())
        current = query_video_via_aid(self.aid)
        self.assertEqual((current.title,current.state),('changed',-4))
        self.assertEqual(read_staff(self.aid),[])
        for mid in (self.mid,self.mid+1):
            self.assertIsNone(query_member_via_mid(mid).last_video_aid)
        self.service.get_video_view.return_value = self.view()._replace(title='changed')
        update_video(self.aid,self.service)
        self.assertEqual(query_member_via_mid(self.mid+1).last_video_aid,self.aid)
        rows = read_rows('SELECT attr,oldval,newval FROM tdd_video_log WHERE aid=%s',(self.aid,))
        self.assertTrue(any(r['attr']=='state' and r['newval']=='-4' for r in rows))
        self.assertNotIn('\x00',str(rows))
        history = read_value('SELECT count(*) FROM tdd_video_record WHERE aid=%s',(self.aid,))
        self.expected_manifest({'tdd_video_log':len(rows),'tdd_video_record':history})

    def test_code_403_does_not_invalidate_video_and_optional_keys_do_not_clear(self):
        add_video(self.aid,self.service)
        self.service.get_video_view.side_effect = CodeError('get_video_view',VideoView,{'aid':self.aid},{},-403)
        self.assertEqual(update_video(self.aid,self.service),[])
        self.assertEqual(query_video_via_aid(self.aid).code,0)
        self.service.get_video_view.side_effect = None
        self.service.get_video_view.return_value = self.view()._replace(staff=None,attribute=None,
            response_has_attribute=False,response_has_forward=False)
        logs = update_video(self.aid,self.service)
        self.assertEqual({row.attr:(row.oldval,row.newval) for row in logs},{'hasstaff':('1','0')})
        current = query_video_via_aid(self.aid)
        self.assertEqual((current.hasstaff,current.attribute),(0,0))
        self.assertEqual(len(read_staff(self.aid)),1)
        self.expected_manifest({'tdd_video_record':1,'tdd_video_log':1})

    def test_member_null_empty_optional_missing_and_late_profile(self):
        insert_video(TddVideo(added=ADDED,aid=self.aid,bvid=a2b(self.aid),mid=self.mid,
                              code=0,state=0,title='late profile',pubdate=100),[],[])
        add_member(self.mid,self.service)
        self.assertEqual(query_member_via_mid(self.mid).last_video_aid,self.aid)
        self.service.get_member_card.side_effect = None
        self.service.get_member_card.return_value = MemberCard(self.mid,'member',None,'','\x00')
        logs = update_member(self.mid,self.service)
        self.assertEqual({r.attr:(r.oldval,r.newval) for r in logs},{'sex':('',None),'sign':('sign','')})
        self.assertEqual(query_member_via_mid(self.mid).sign,'')
        self.expected_manifest({'tdd_member_log':2})

    def test_same_second_independent_observations_and_bigint_total(self):
        self.service.get_video_view.return_value = self.view(view=2147483647,staff=False)
        add_video(self.aid,self.service)
        self.service.get_video_view.return_value = self.view(aid=self.aid+1,view=2147483647,staff=False)
        add_video(self.aid+1,self.service)
        record = VideoRecord(ADDED,self.aid,a2b(self.aid),2147483647,1,2,3,4,5,6)
        first = commit_video_records_batch([record])
        independent = commit_video_records_batch([record])
        self.assertNotEqual(first[0][2],independent[0][2])
        self.assertEqual(read_value('SELECT id FROM tdd_video_record_latest WHERE aid=%s',(self.aid,)),independent[0][2])
        rows = read_rows('SELECT v.aid,v.mid AS owner_mid,r.view,r.danmaku,r.reply,r.favorite,r.coin,r.share,r.like,'
                                    's.mid AS staff_mid,v.code,v.state,r.id FROM tdd_video v '
                                    'JOIN tdd_video_record_latest r ON v.aid=r.aid '
                                    'LEFT JOIN tdd_video_staff s ON v.aid=s.aid WHERE v.aid=ANY(%s)',([self.aid,self.aid+1],))
        totals = aggregate_member_stats([tuple(r.values()) for r in rows],{},ADDED,LOG)
        self.assertEqual(totals[self.mid]['view'],4294967294)
        self.assertEqual(write_member_stats(totals.values(),LOG),1)
        self.assertEqual(read_value('SELECT view FROM tdd_member_total_stat_record_latest WHERE mid=%s',(self.mid,)),4294967294)
        with engine.connection() as conn, conn.transaction():
            conn.execute('UPDATE tdd_video SET state=%s WHERE aid=ANY(%s)',(-4,[self.aid,self.aid+1]))
        rows = read_rows('SELECT v.aid,v.mid AS owner_mid,r.view,r.danmaku,r.reply,r.favorite,r.coin,r.share,r.like,'
                                    's.mid AS staff_mid,v.code,v.state,r.id FROM tdd_video v '
                                    'JOIN tdd_video_record_latest r ON v.aid=r.aid '
                                    'LEFT JOIN tdd_video_staff s ON v.aid=s.aid WHERE v.aid=ANY(%s)',([self.aid,self.aid+1],))
        zeros = aggregate_member_stats([tuple(r.values()) for r in rows],{self.mid:1},ADDED,LOG)
        write_member_stats(zeros.values(),LOG)
        self.assertEqual(read_rows('SELECT view,video_count FROM tdd_member_total_stat_record_latest WHERE mid=%s',(self.mid,))[0]['view'],0)
        self.expected_manifest({'tdd_video_record':4,'tdd_member_total_stat_record':2})

    def test_repeated_staff_and_nullable_sprint_preserve_the_observation(self):
        from job import AddSprintVideoRecordJob
        self.service.get_video_view.return_value = self.view()._replace(staff=[
            VideoViewStaffItem(self.mid+1,'first','',''),VideoViewStaffItem(self.mid+1,'second','','')])
        add_video(self.aid,self.service)
        self.assertEqual([(item.mid,item.title) for item in read_staff(self.aid)],[(self.mid+1,'first')])
        self.service.get_video_view.return_value = self.view()._replace(title='changed',staff=[
            VideoViewStaffItem(self.mid+1,'first-existing','',''),VideoViewStaffItem(self.mid+1,'second-existing','',''),
            VideoViewStaffItem(self.mid+2,'first-new','',''),VideoViewStaffItem(self.mid+2,'second-new','','')])
        with self.assertLogs('db.profiles',level='WARNING'):
            logs = update_video(self.aid,self.service)
        staff = read_staff(self.aid)
        self.assertEqual({item.mid:item.title for item in staff},{self.mid+1:'first-existing',self.mid+2:'first-new'})
        self.assertEqual(len([log for log in logs if log.attr == 'staff']),2)
        self.assertEqual(query_video_via_aid(self.aid).title,'changed')
        with engine.connection() as conn, conn.transaction():
            conn.execute('INSERT INTO tdd_sprint_video (added,aid,title,created,status) VALUES (%s,%s,%s,%s,%s)',
                                 (timestamp(ADDED),self.aid,'nullable sprint',100,'processing'))
        nullable = self.view()._replace(stat=self.view().stat._replace(view=None))
        self.service.get_video_view_trimmed.return_value = nullable
        queue = Queue()
        queue.put(self.aid)
        job = AddSprintVideoRecordJob('nullable',queue,self.service)
        self.addCleanup(job.cleanup)
        job.process()
        self.assertEqual(job.stat.condition['persisted'],1)
        self.assertEqual(job.stat.condition['million_success'],0)
        rows = read_rows('SELECT view FROM tdd_sprint_video_record WHERE aid=%s',(self.aid,))
        self.assertEqual(len(rows),1)
        self.assertIsNone(rows[0]['view'])
        self.assertEqual(read_value('SELECT status FROM tdd_sprint_video WHERE aid=%s',(self.aid,)),'processing')
        self.assertEqual(list(Path(self.directory.name).glob('*.json')),[])
        self.expected_manifest({'tdd_video_record':1,'tdd_video_log':3,'tdd_sprint_video_record':1})

    def test_csv_analysis_persists_anomaly_with_original_snapshots(self):
        from test_remaining_script_run_records import _load
        script = _load('51_hourly-video-record-add.py')
        add_video(self.aid,self.service)
        records = [VideoRecord(ADDED-7200,self.aid,a2b(self.aid),1000,1,1,1,1,1,1),
                   VideoRecord(ADDED-3600,self.aid,a2b(self.aid),1100,1,1,1,1,1,1),
                   VideoRecord(ADDED,self.aid,a2b(self.aid),3000,1,1,1,1,1,1)]
        for record in records[:-1]:
            label = script.ts_s_to_str(record.added)[:13]+':00'
            path = Path(self.directory.name)/(label+'.csv')
            path.write_text(','.join(VideoRecord._fields)+'\n'+','.join(map(str,record))+'\n')
        runner = script.RecentRecordsAnalystRunner(records[-1:],script.ts_s_to_str(ADDED)[:13]+':00',data_folder=self.directory.name)
        runner.process()
        actual = read_rows('SELECT * FROM tdd_video_record_abnormal_change WHERE aid=%s',(self.aid,))
        self.assertEqual(len(actual),1)
        self.assertEqual((actual[0]['attr'],actual[0]['this_added'],actual[0]['last_added']),('view',ADDED,ADDED-3600))
        self.assertEqual((actual[0]['this_view'],actual[0]['last_view'],actual[0]['speed_now']),(3000,1100,1900.0))
        self.assertIsNone(actual[0]['this_record_id'])
        self.assertIsNone(actual[0]['last_record_id'])
        self.expected_manifest({'tdd_video_record':1,'tdd_video_record_abnormal_change':1})

    def test_sprint_million_status_and_daily_legacy_windows_and_sentinels(self):
        import datetime
        from db import TddSprintVideoRecord
        from job import AddSprintVideoRecordJob
        from test_remaining_script_run_records import _load
        cst = datetime.timezone(datetime.timedelta(hours=8))
        end = int(datetime.datetime(6000+self.run_id.int%1000,2,7,6,tzinfo=cst).timestamp())
        start,now = end-86400,end+1800
        aids = [self.aid,self.aid+1,self.aid+2]
        with engine.connection() as conn, conn.transaction():
            for aid in aids:
                conn.execute('INSERT INTO tdd_sprint_video (added,aid,title,created,status) VALUES (%s,%s,%s,%s,%s)',
                                     (timestamp(start),aid,'sprint',end-3*86400,'processing'))
        def record(aid,added,view):
            return TddSprintVideoRecord(added=added,aid=aid,view=view,danmaku=1,reply=1,favorite=1,coin=1,share=1,like=1)
        from db.sprint import insert_sprint_video_record
        for item in [record(aids[0],start+60,1000),record(aids[0],end+60,1000),
                     record(aids[1],end-3600,200),record(aids[1],end+60,500),
                     record(aids[2],start+60,999000)]:
            insert_sprint_video_record(item)
        queue = Queue()
        queue.put(aids[2])
        self.service.get_video_view_trimmed.return_value = self.view(aid=aids[2],view=1000000)
        job = AddSprintVideoRecordJob('sprint',queue,self.service)
        self.addCleanup(job.cleanup)
        with mock.patch('task.task.get_ts_s',return_value=end-3600):
            job.process()
        self.assertEqual(job.stat.condition['persisted'],1)
        self.assertEqual(read_value('SELECT status FROM tdd_sprint_video WHERE aid=%s',(aids[2],)),'finished')
        previous = read_value('SELECT viewincr FROM tdd_sprint_daily WHERE viewincr IS NOT NULL ORDER BY id DESC LIMIT 1')
        script = _load('72_add-sprint-daily.py')
        with mock.patch.object(script,'get_ts_s',return_value=now):
            script.add_sprint_daily()
        date = datetime.datetime.fromtimestamp(now,cst).strftime('%Y%m%d')
        details = read_rows('SELECT * FROM tdd_sprint_daily_record WHERE date=%s AND aid=ANY(%s)',(date,aids))
        self.assertEqual(len(details),2)
        values = {r['aid']:r for r in details}
        self.assertEqual((values[aids[0]]['viewincr'],values[aids[0]]['lday'],values[aids[0]]['pday']),(0,9999999,3))
        self.assertEqual((values[aids[1]]['viewincr'],values[aids[1]]['lday']),(300,3331))
        daily = read_rows('SELECT * FROM tdd_sprint_daily WHERE date=%s AND added=%s ORDER BY id DESC LIMIT 1',(date,timestamp(now)))[0]
        self.assertEqual((daily['vidnum'],daily['viewincr'],daily['viewincrincr']),(2,1300,1300-previous))
        self.assertEqual((daily['newvids'],daily['millvids']),(str(aids[1])+';',str(aids[2])+';'))
        self.expected_manifest({'tdd_sprint_video_record':6})

    def test_archive_manual_file_and_rank_paths_share_the_append_protocol(self):
        from job import AddVideoFromArchiveJob
        from test_remaining_script_run_records import _load
        archive_queue = Queue()
        archive_queue.put((ADDED-10,SimpleNamespace(aid=self.aid,stat=self.view().stat._replace(aid=0))))
        self.service.get_video_view.side_effect = lambda params:self.view(aid=params['aid'])
        self.service.get_video_view_trimmed.side_effect = lambda params:self.view(aid=params['aid'])
        archive = AddVideoFromArchiveJob('archive',archive_queue,self.service)
        self.addCleanup(archive.cleanup)
        archive.process()
        self.assertEqual(archive.stat.condition['new_video_record'],1)
        self.assertEqual(read_value('SELECT added FROM tdd_video_record_latest WHERE aid=%s',(self.aid,)),ADDED-10)
        path = Path(self.directory.name)/'aids.txt'
        path.write_text(str(self.aid+1)+'\n')
        for filename,function in (('41_add-video-from-file.py','add_video_from_file'),
                                  ('42_add-video-record-from-file.py','add_video_record_from_file')):
            script = _load(filename)
            with mock.patch.object(script,'Service',return_value=self.service):
                getattr(script,function)(str(path))
        rank = _load('62_add-evocalrank-video.py')
        response = mock.Mock()
        response.json.return_value = {key:[] for key in ('main_rank','second_rank','super_hit','pick_up','oth_pickup',
                                                        'Vocaloid_pick_up','history-1-year','history-10-year','ed','op')}
        response.json.return_value['main_rank'] = [{'avid':'av'+str(self.aid+1)},{'avid':'av'+str(self.aid+2)}]
        with mock.patch.object(rank,'Service',return_value=self.service),mock.patch.object(rank.requests,'get',return_value=response):
            rank.add_evocalrank_video(1)
        aids = [self.aid,self.aid+1,self.aid+2]
        self.assertEqual(read_value('SELECT count(*) FROM tdd_video WHERE aid=ANY(%s)',(aids,)),3)
        self.assertEqual(read_value('SELECT count(*) FROM tdd_video_record WHERE aid=ANY(%s)',(aids,)),4)
        self.assertEqual(read_value('SELECT count(*) FROM tdd_video_record_latest WHERE aid=ANY(%s)',(aids,)),3)
        self.assertEqual(list(Path(self.directory.name).glob('*.json')),[])
        self.expected_manifest({'tdd_video_record':4})

    def test_aggregate_thousand_row_commit_survives_failure_then_manual_retry(self):
        from db.ingest import run_transaction
        calls = []
        def fail_second(factory,callback,*,run_id):
            calls.append(run_id)
            if len(calls) == 2:
                raise RuntimeError('injected second aggregate batch failure')
            return run_transaction(factory,callback,run_id=run_id)
        rows = [dict(added=ADDED,mid=self.mid+i,video_count=1,view=2147483648,
                     danmaku=0,reply=0,favorite=0,coin=0,share=0,like=0) for i in range(1001)]
        with mock.patch('db.ingest.run_transaction',side_effect=fail_second), \
             self.assertLogs(LOG,level='CRITICAL'), self.assertRaisesRegex(RuntimeError,'second aggregate'):
            write_member_stats(rows,LOG)
        mids = [row['mid'] for row in rows]
        self.assertEqual(read_value('SELECT count(*) FROM tdd_member_total_stat_record WHERE mid=ANY(%s)',(mids,)),1000)
        self.assertEqual(read_value('SELECT count(*) FROM tdd_member_total_stat_record_latest WHERE mid=ANY(%s)',(mids,)),1000)
        pending = list(Path(self.directory.name).glob('*.json'))
        self.assertEqual(len(pending),1)
        self.assertEqual(read_value('SELECT count(*) FROM tdd_member_total_stat_record WHERE mid=%s',(rows[-1]['mid'],)),0)
        # The injected second failure occurred before its transaction started.
        # Submit just that verified-missing row; no generic replay is involved.
        self.assertEqual(write_member_stats(rows[-1:],LOG),1)
        self.assertTrue(pending[0].exists())
        self.assertEqual(read_value('SELECT count(*) FROM tdd_member_total_stat_record WHERE mid=ANY(%s)',(mids,)),1001)
        self.expected_manifest({'tdd_member_total_stat_record':1001})

    def test_follower_fetch_queue_writer_commits_history_latest_and_manifest(self):
        mids,records = Queue(),Queue(maxsize=20000)
        for mid in (self.mid,self.mid+1,self.mid+2):
            mids.put(mid)
        service = mock.Mock()
        service.get_member_relation.side_effect = lambda params: SimpleNamespace(follower=params['vmid']%1000)
        fetch = FetchMemberFollowerRecordJob('follower-fetch',mids,records,service)
        writer = BatchInsertMemberFollowerRecordJob('follower-writer',records,batch_size=2)
        self.addCleanup(writer.cleanup)
        fetch.process()
        records.put(None)
        writer.process()
        self.assertEqual(fetch.stat.condition['success'],3)
        self.assertEqual(writer.stat.condition['persisted'],3)
        actual = read_rows('SELECT mid,follower FROM tdd_member_follower_record_latest WHERE mid=ANY(%s)',([self.mid,self.mid+1,self.mid+2],))
        self.assertEqual({r['mid']:r['follower'] for r in actual},{mid:mid%1000 for mid in (self.mid,self.mid+1,self.mid+2)})
        record = MemberFollowerRecord(ADDED,self.mid,123)
        keys = commit_member_follower_records_batch([record,record])
        self.assertEqual([key[:2] for key in keys],[(self.mid,timestamp(ADDED))]*2)
        self.assertNotEqual(keys[0][2],keys[1][2])
        self.assertEqual(read_value('SELECT id FROM tdd_member_follower_record_latest WHERE mid=%s',
                                             (self.mid,)),keys[1][2])
        self.expected_manifest({'tdd_member_follower_record':5})
