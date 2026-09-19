"""
`format_duration_summary` renders the one run-span line an entry point logs
and `sc_send_summary` puts in its message body. These pin that content, so
the line keeps reporting a start, an end and a duration.
"""
import unittest

from util import format_duration_summary, ts_ms_to_str


class FormatDurationSummaryTest(unittest.TestCase):
    def test_reports_start_end_and_duration(self):
        start = 1_700_000_000_000
        end = start + 3 * 3600 * 1000 + 25 * 60 * 1000 + 6 * 1000 + 78

        line = format_duration_summary(start, end)

        self.assertEqual(
            line,
            f'start: {ts_ms_to_str(start)}, '
            f'end: {ts_ms_to_str(end)}, '
            f'duration: 3h 25m 6s 78ms')

    def test_sub_second_span_reports_milliseconds_only(self):
        start = 1_700_000_000_000
        self.assertTrue(
            format_duration_summary(start, start + 250).endswith('duration: 250ms'))


if __name__ == '__main__':
    unittest.main()
