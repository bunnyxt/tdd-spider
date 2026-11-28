import copy
import logging
import unittest

from util.member_total_stat import aggregate_member_stats, STAT_FIELDS


def row(aid=1, mid=10, stats=(10, 1, 2, 3, 4, 5, 6), staff=None,
        code=0, state=0, record=100):
    return (aid, mid, *stats, staff, code, state, record)


class MemberTotalStatTest(unittest.TestCase):
    def calculate(self, rows, members=None):
        return aggregate_member_stats(rows, members or {}, 123, logging.getLogger('test.aggregate'))

    def test_unknown_contributes_zero_without_changing_inputs(self):
        rows = [row(stats=(-1, 1, 2, -1, 4, 5, 6)), row(aid=2)]
        original = copy.deepcopy(rows)
        result = self.calculate(rows, {10: 7})[10]
        self.assertEqual(result, dict(added=123, mid=10, video_count=2,
                                     view=10, danmaku=2, reply=4, favorite=3,
                                     coin=8, share=10, like=12))
        self.assertEqual(rows, original)

    def test_qualification_and_zero_latest(self):
        rows = [row(mid=10, code=-403), row(mid=11, state=-4),
                row(mid=12, state=1), row(mid=13, state=None), row(mid=14)]
        with self.assertLogs('test.aggregate', level='WARNING') as logs:
            result = self.calculate(rows, {mid: mid for mid in range(10, 16)})
        self.assertTrue(any('null_state' in line for line in logs.output))
        for mid in (10, 11, 12, 13, 15):
            self.assertEqual(result[mid]['video_count'], 0)
            self.assertTrue(all(result[mid][f] == 0 for f in STAT_FIELDS))
        self.assertEqual(result[14]['video_count'], 1)

    def test_staff_full_attribution_missing_profile_and_large_mid(self):
        mids = (2147483647, 2147483648, 9000000000000)
        result = self.calculate([row(staff=mid) for mid in mids])
        self.assertNotIn(10, result)
        self.assertEqual(set(result), set(mids))
        for mid in mids:
            self.assertEqual(result[mid]['view'], 10)
            self.assertEqual(result[mid]['video_count'], 1)

    def test_invalid_staff_does_not_fall_back_to_owner(self):
        with self.assertLogs('test.aggregate', level='WARNING') as logs:
            result = self.calculate([row(mid=0), row(staff=0), row(staff=-1)])
        self.assertEqual(result, {})
        self.assertIn('count=3', logs.output[0])

    def test_unrecognized_values_and_missing_latest_fail(self):
        for value in (None, -2, '10', 1.5, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.calculate([row(stats=(value, 0, 0, 0, 0, 0, 0))])
        with self.assertRaisesRegex(ValueError, 'Missing latest'):
            self.calculate([row(record=None)])
        result = self.calculate([row(stats=(2147483647, 0, 0, 0, 0, 0, 0)), row()])
        self.assertEqual(result[10]['view'], 2147483657)

    def test_warning_samples_are_bounded(self):
        with self.assertLogs('test.aggregate', level='WARNING') as logs:
            self.calculate([row(aid=n, mid=0) for n in range(100)])
        self.assertEqual(len(logs.output), 1)
        self.assertIn('count=100', logs.output[0])
        self.assertNotIn('(5,', logs.output[0])

    def test_empty_inputs_clear_only_previous_latest_members(self):
        self.assertEqual(set(self.calculate([], {10: 5, 20: None})), {10})


if __name__ == '__main__':
    unittest.main()
