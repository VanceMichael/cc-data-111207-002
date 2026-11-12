from urllib.parse import urlencode
from lab import fetch
import json

def send(base, request):
    status, headers, body = fetch(base+'/quote?'+urlencode({'tenant':request['tenant']}),
                                  {'X-Credential':request['credential']})
    return {'status':status,'headers':dict(headers),'data':json.loads(body)}

def inspect(base, fixture):
    return {'单次请求':send(base, fixture['requests'][0])}
