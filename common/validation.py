import time

__all__ = ['get_valid', 'test_video_view', 'test_video_stat', 'test_member', 'test_archive_rank_by_partion']


def get_valid(get_obj_func, get_obj_func_args, test_obj_func, repeat_count=5, colddown=1):
    result = None
    request_count = 1
    while request_count <= repeat_count:
        try:
            obj = get_obj_func(*get_obj_func_args)
            err = test_obj_func(obj)
            if err is None:
                result = obj
                return result
            else:
                # print(err, request_count)
                time.sleep(colddown)
        except Exception as e:
            # print(e, request_count)
            time.sleep(colddown)
        finally:
            request_count += 1
    return result


def test_video_view(obj):
    try:
        # obj should be a dictionary
        if type(obj) is not dict:
            return 'Obj should be a dictionary.'

        # contains key code, message, ttl
        for key in ['code', 'message', 'ttl']:
            if key not in obj.keys():
                return 'Obj should contain key %s.' % key

        # contains key data if code == 0
        if obj['code'] == 0:
            if 'data' not in obj.keys():
                return 'Obj should contain key data when obj[\'code\'] == 0.'

        return None
    except Exception as e:
        return str(e)


def test_video_stat(obj):
    try:
        # obj should be a dictionary
        if type(obj) is not dict:
            return 'Obj should be a dictionary.'

        # contains key code, message, ttl
        for key in ['code', 'message', 'ttl']:
            if key not in obj.keys():
                return 'Obj should contain key %s.' % key

        # contains key data if code == 0
        if obj['code'] == 0:
            if 'data' not in obj.keys():
                return 'Obj should contain key data when obj[\'code\'] == 0.'

        return None
    except Exception as e:
        return str(e)


def test_member(obj):
    try:
        # obj should be a dictionary
        if type(obj) is not dict:
            return 'Obj should be a dictionary.'

        # contains key code, message, ttl
        for key in ['code', 'message', 'ttl']:
            if key not in obj.keys():
                return 'Obj should contain key %s.' % key

        # contains key data if code == 0
        if obj['code'] == 0:
            if 'data' not in obj.keys():
                return 'Obj should contain key data when obj[\'code\'] == 0.'

        return None
    except Exception as e:
        return str(e)


def test_archive_rank_by_partion(obj):
    try:
        # obj should be a dictionary
        if type(obj) is not dict:
            return 'Obj should be a dictionary.'

        # contains key code, message, ttl, data
        for key in ['code', 'message', 'ttl', 'data']:
            if key not in obj.keys():
                return 'Obj should contain key %s.' % key

        # data contains archives and page
        for key in ['archives', 'page']:
            if key not in obj['data'].keys():
                return 'Obj[\'data\'] should contain key %s.' % key

        # data page contain count, num, size
        for key in ['count', 'num', 'size']:
            if key not in obj['data']['page'].keys():
                return 'Obj[\'data\'][\'page\'] should contain key %s.' % key

        # test each archive
        for archive in obj['data']['archives']:
            pass

        return None
    except Exception as e:
        return str(e)
