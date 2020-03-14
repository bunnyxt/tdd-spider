from db import Session
import time
import sys
from util import get_ts_s, ts_s_to_str
from serverchan import sc_send


class Log:
    remote_host = ''
    remote_login_name = ''
    remote_user_name = ''
    time_string = ''
    time_stamp = 0
    request_method = ''
    request_url = ''
    request_param = ''
    response_status = 0
    response_size = 0
    referer = ''
    user_agent = ''

    def timestring_to_timestamp(self, time_string):
        month_dict = {
            'Jan': '01',
            'Feb': '02',
            'Mar': '03',
            'Apr': '04',
            'May': '05',
            'Jun': '06',
            'Jul': '07',
            'Aug': '08',
            'Sep': '09',
            'Oct': '10',
            'Nov': '11',
            'Dec': '12'
        }
        month = month_dict[time_string[3:6]]
        time_string = time_string[:3] + month + time_string[6:]

        time_array = time.strptime(time_string, "%d/%m/%Y:%H:%M:%S +0800")
        time_stamp = int(time.mktime(time_array))
        return time_stamp

    def __init__(self, s):
        lens = len(s)
        i, j = 0, 0
        while j < lens:
            j += 1
            if s[j] == ' ':
                break
        self.remote_host = s[i:j]
        i = j + 1

        while j < lens:
            j += 1
            if s[j] == ' ':
                break
        self.remote_login_name = s[i:j]
        i = j + 1

        while j < lens:
            j += 1
            if s[j] == ' ':
                break
        self.remote_user_name = s[i:j]
        i = j + 2

        while j < lens:
            j += 1
            if s[j] == ']':
                break
        self.time_string = s[i:j]
        j += 2
        i = j + 1
        self.time_stamp = self.timestring_to_timestamp(self.time_string)

        while j < lens:
            j += 1
            if s[j] == ' ':
                break
        self.request_method = s[i:j]
        i = j + 1

        has_param = False
        while j < lens:
            j += 1
            if s[j] == '?':
                has_param = True
                break
            if s[j] == ' ':
                break
        self.request_url = s[i:j]
        i = j + 1

        if has_param:
            while j < lens:
                j += 1
                if s[j] == ' ':
                    break
            self.request_param = s[i:j]
            i = j + 1

        while i < lens:
            i += 1
            if s[i] == '\"':
                break
        i += 2
        j = i

        while j < lens:
            j += 1
            if s[j] == ' ':
                break
        self.response_status = int(s[i:j])
        i = j + 1

        while j < lens:
            j += 1
            if s[j] == ' ':
                break
        self.response_size = int(s[i:j])
        i = j + 2
        j = i

        while j < lens:
            j += 1
            if s[j] == '\"':
                break
        self.referer = s[i:j]
        i = j + 3
        j = i

        while j < lens:
            j += 1
            if s[j] == '\"':
                break
        self.user_agent = s[i:j]


def main():
    filename = '/var/log/apache2/api/access.log'
    last_line = 0

    if len(sys.argv) >= 2:
        filename = sys.argv[1]
        print('set filename to %s' % filename)
    if len(sys.argv) >= 3:
        last_line = int(sys.argv[2])
        print('set last_line to %d' % last_line)

    while True:
        try:
            session = Session()

            # read lines
            with open(filename) as f:
                lines = f.readlines()

            # file removed, reset to 0, lose some logs
            if len(lines) < last_line:
                print('%s last_line %d -> 0' % (ts_s_to_str(get_ts_s()), last_line))
                last_line = 0

            # no new log
            if last_line == len(lines):
                session.close()
                time.sleep(30)
                continue

            # process new log
            for line in lines[last_line:]:
                try:
                    log = Log(line)
                    sql = 'insert into tdd_api_log values ("{0}", {1}, "{2}", "{3}", "{4}", {5}, {6})'.format(
                        log.remote_host, log.time_stamp, log.request_method, log.request_url[:100], log.request_param,
                        log.response_status, log.response_size)
                    session.execute(sql)
                    session.commit()
                except Exception as e:
                    print(ts_s_to_str(get_ts_s()), e)
                    session.rollback()

            # update last_line
            last_line = len(lines)
            session.close()
            time.sleep(5)
        except Exception as e:
            print(ts_s_to_str(get_ts_s()), e)
            print('%s last_line %d' % (ts_s_to_str(get_ts_s()), last_line))
            sc_send('41_load-apache-api-log-into-db error!',
                    '%s %r last_line %d' % (ts_s_to_str(get_ts_s()), e, last_line))
            break


if __name__ == '__main__':
    main()
