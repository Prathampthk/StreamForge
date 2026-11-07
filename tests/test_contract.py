import json
import unittest
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker
ROOT = Path(__file__).resolve().parents[1]
class ContractTests(unittest.TestCase):
    def test_fixtures(self):
        schema = json.loads((ROOT / 'schemas/event-v1.schema.json').read_text())
        self.assertIn("date-time", FormatChecker().checkers)
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        for path in (ROOT / 'tests/fixtures').glob('*.json'):
            if not path.name.startswith(('valid-', 'invalid-')):
                continue
            with self.subTest(fixture=path.name):
                errors = list(validator.iter_errors(json.loads(path.read_text())))
                self.assertEqual(bool(errors), path.name.startswith('invalid-'))
