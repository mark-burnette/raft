# every reply a server sends a client is one of these


def ok(result):
    return {'status': 'ok', 'result': result}


def redirect(leader):
    # leader is None while an election is underway
    return {'status': 'redirect', 'leader': leader}


def error(msg):
    return {'status': 'error', 'msg': msg}
