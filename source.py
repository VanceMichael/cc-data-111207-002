import json
from urllib.parse import parse_qs, urlsplit

class Source:
    def __init__(self, fixture):
        self.fixture = fixture
        self.now = 0
        self.history = []
        self.cooldown = None

    def handle(self, path, headers):
        query = parse_qs(urlsplit(path).query)
        if urlsplit(path).path != '/quote':
            return 404, {}, b''
        tenant = query.get('tenant',[''])[0]
        credential = headers.get('X-Credential','')
        if not tenant or not credential:
            return 400, {}, b''
        if self.cooldown:
            scope, value = self.cooldown
            return 429, {'Retry-After':value,'X-Quota-Scope':scope}, b'{}'
        keys = {'egress':'all','credential':credential,'tenant':tenant}
        for scope, identity in keys.items():
            limit = self.fixture['limits'][scope]
            count = sum(1 for entry in self.history
                        if self.now-limit['window'] < entry['at'] <= self.now
                        and entry[scope] == identity)
            if count >= limit['capacity']:
                return 429, {'Retry-After':str(limit['window']),
                             'X-Quota-Scope':scope}, b'{}'
        self.history.append({'at':self.now,**keys})
        return 200, {'Content-Type':'application/json'}, json.dumps({'tenant':tenant,'value':17}).encode()
