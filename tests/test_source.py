import json
import unittest
from pathlib import Path
from reader import inspect
from lab import serving
from source import Source


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads((Path(__file__).parents[1] / "fixtures/source.json").read_text(encoding="utf-8"))
        self.source = Source(self.fixture)

    def test_credential_budget_shared(self):
        from reader import send
        source = Source(self.fixture)
        with serving(source) as base:
            statuses = [send(base, {'tenant':str(index),'credential':'group'})['status'] for index in range(4)]
        self.assertEqual(statuses, [200,200,200,429])

    def test_window_expires(self):
        from reader import send
        source = Source(self.fixture)
        with serving(source) as base:
            request = self.fixture['requests'][0]
            send(base,request)
            send(base,request)
            self.assertEqual(send(base,request)['status'],429)
            source.now=10
            self.assertEqual(send(base,request)['status'],200)

    def test_cooldown_scope(self):
        from reader import send
        source=Source(self.fixture)
        source.cooldown=('egress','Thu, 12 Nov 2026 01:00:10 GMT')
        with serving(source) as base:
            result=send(base,self.fixture['requests'][0])
        self.assertEqual(result['headers']['X-Quota-Scope'],'egress')
        self.assertEqual(result['status'],429)


if __name__ == "__main__":
    unittest.main()
