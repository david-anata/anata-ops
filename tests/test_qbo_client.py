import tempfile
import unittest
from pathlib import Path
from unittest import mock

import qbo_client


class QboClientTests(unittest.TestCase):
    def test_build_vendor_aliases_uses_display_and_check_names(self):
        aliases = qbo_client.build_vendor_aliases(
            [
                {
                    "DisplayName": "Bear River",
                    "FullyQualifiedName": "Bear River",
                    "PrintOnCheckName": "Bear River Mutual",
                }
            ]
        )
        self.assertEqual(aliases["Bear River"], "Bear River")
        self.assertEqual(aliases["Bear River Mutual"], "Bear River")

    def test_refresh_access_token_persists_rotated_refresh_token(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "qbo_tokens.json"
            with mock.patch.dict(
                "os.environ",
                {
                    "QBO_CLIENT_ID": "client-id",
                    "QBO_CLIENT_SECRET": "client-secret",
                    "QBO_REALM_ID": "12345",
                    "QBO_REFRESH_TOKEN": "seed-token",
                },
                clear=False,
            ):
                payload = {
                    "access_token": "access-1",
                    "refresh_token": "refresh-2",
                    "expires_in": 3600,
                }

                class FakeResponse:
                    def __enter__(self):
                        return self

                    def __exit__(self, exc_type, exc, tb):
                        return False

                    def read(self):
                        import json

                        return json.dumps(payload).encode("utf-8")

                with mock.patch("urllib.request.urlopen", return_value=FakeResponse()):
                    stored = qbo_client.refresh_access_token(token_path)

                self.assertEqual(stored["refresh_token"], "refresh-2")
                saved = qbo_client.load_token_store(token_path)
                self.assertEqual(saved["refresh_token"], "refresh-2")


if __name__ == "__main__":
    unittest.main()
