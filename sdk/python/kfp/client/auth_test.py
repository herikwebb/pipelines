# Copyright 2023 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import stat
import tempfile
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from absl.testing import parameterized
from kfp.client import auth


def _mode(path: str) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


class TestAuth(parameterized.TestCase):

    @unittest.skipIf(os.name == 'nt', 'POSIX file permissions only')
    def test_write_private_json_creates_owner_only_dir_and_file(self):
        with tempfile.TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, 'kfp', 'credentials.json')
            auth.write_private_json(path, {'client': {'refresh_token': 't'}})
            self.assertEqual(_mode(os.path.dirname(path)), 0o700)
            self.assertEqual(_mode(path), 0o600)
            with open(path) as f:
                self.assertEqual(
                    json.load(f), {'client': {
                        'refresh_token': 't'
                    }})

    @unittest.skipIf(os.name == 'nt', 'POSIX file permissions only')
    def test_write_private_json_tightens_existing_file(self):
        with tempfile.TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, 'credentials.json')
            with open(path, 'w') as f:
                json.dump({'stale': True}, f)
            os.chmod(path, 0o644)
            auth.write_private_json(path, {'fresh': True})
            self.assertEqual(_mode(path), 0o600)
            with open(path) as f:
                self.assertEqual(json.load(f), {'fresh': True})

    @unittest.skipIf(os.name == 'nt', 'POSIX file permissions only')
    @patch('kfp.client.auth.id_token_from_refresh_token',
           lambda *args: 'id-token')
    @patch('kfp.client.auth.get_refresh_token_from_client_id', lambda *args:
           ('refresh-token', True))
    def test_get_auth_token_stores_credentials_owner_only(self):
        with tempfile.TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, 'kfp', 'credentials.json')
            with patch('kfp.client.auth.LOCAL_KFP_CREDENTIAL', path):
                token, is_refresh_token = auth.get_auth_token(
                    'client-id', 'other-client-id', 'other-client-secret')
            self.assertEqual(token, 'id-token')
            self.assertTrue(is_refresh_token)
            self.assertEqual(_mode(path), 0o600)
            with open(path) as f:
                self.assertEqual(
                    json.load(f)['client-id']['refresh_token'], 'refresh-token')

    def test_is_ipython_return_false(self):
        mock = MagicMock()
        with patch.dict('sys.modules', IPython=mock):
            mock.get_ipython.return_value = None
            self.assertFalse(auth.is_ipython())

    def test_is_ipython_return_true(self):
        mock = MagicMock()
        with patch.dict('sys.modules', IPython=mock):
            mock.get_ipython.return_value = 'Something'
            self.assertTrue(auth.is_ipython())

    def test_is_ipython_should_raise_error(self):
        mock = MagicMock()
        with patch.dict('sys.modules', mock):
            mock.side_effect = ImportError
            self.assertFalse(auth.is_ipython())

    @patch(
        'builtins.input', lambda *args:
        'https://oauth2.example.com/auth?code=4/P7q7W91a-oMsCeLvIaQm6bTrgtp7')
    @patch('kfp.client.auth.is_ipython', lambda *args: True)
    @patch.dict(os.environ, dict(), clear=True)
    def test_get_auth_code_from_ipython(self):
        token, redirect_uri = auth.get_auth_code('sample-client-id')
        self.assertEqual(token, '4/P7q7W91a-oMsCeLvIaQm6bTrgtp7')
        self.assertEqual(redirect_uri, 'http://localhost:9901')

    @patch(
        'builtins.input', lambda *args:
        'https://oauth2.example.com/auth?code=4/P7q7W91a-oMsCeLvIaQm6bTrgtp7')
    @patch('kfp.client.auth.is_ipython', lambda *args: False)
    @patch.dict(os.environ, {'SSH_CONNECTION': 'ENABLED'}, clear=True)
    def test_get_auth_code_from_remote_connection(self):
        token, redirect_uri = auth.get_auth_code('sample-client-id')
        self.assertEqual(token, '4/P7q7W91a-oMsCeLvIaQm6bTrgtp7')
        self.assertEqual(redirect_uri, 'http://localhost:9901')

    @patch(
        'builtins.input', lambda *args:
        'https://oauth2.example.com/auth?code=4/P7q7W91a-oMsCeLvIaQm6bTrgtp7')
    @patch('kfp.client.auth.is_ipython', lambda *args: False)
    @patch.dict(os.environ, {'SSH_CLIENT': 'ENABLED'}, clear=True)
    def test_get_auth_code_from_remote_client(self):
        token, redirect_uri = auth.get_auth_code('sample-client-id')
        self.assertEqual(token, '4/P7q7W91a-oMsCeLvIaQm6bTrgtp7')
        self.assertEqual(redirect_uri, 'http://localhost:9901')

    @patch('builtins.input', lambda *args: 'https://oauth2.example.com/auth')
    @patch('kfp.client.auth.is_ipython', lambda *args: False)
    @patch.dict(os.environ, {'SSH_CLIENT': 'ENABLED'}, clear=True)
    def test_get_auth_code_from_remote_client_missing_code(self):
        self.assertRaises(KeyError, auth.get_auth_code, 'sample-client-id')

    @patch(
        'kfp.client.auth.get_auth_response_local', lambda *args:
        'https://oauth2.example.com/auth?code=4/P7q7W91a-oMsCeLvIaQm6bTrgtp7')
    @patch('kfp.client.auth.is_ipython', lambda *args: False)
    @patch.dict(os.environ, dict(), clear=True)
    def test_get_auth_code_from_local(self):
        token, redirect_uri = auth.get_auth_code('sample-client-id')
        self.assertEqual(token, '4/P7q7W91a-oMsCeLvIaQm6bTrgtp7')
        self.assertEqual(redirect_uri, 'http://localhost:9901')

    @patch('kfp.client.auth.get_auth_response_local', lambda *args: None)
    @patch('kfp.client.auth.is_ipython', lambda *args: False)
    @patch.dict(os.environ, dict(), clear=True)
    def test_get_auth_code_from_local_empty_response(self):
        self.assertRaises(ValueError, auth.get_auth_code, 'sample-client-id')

    @patch('kfp.client.auth.get_auth_response_local',
           lambda *args: 'this-is-an-invalid-response')
    @patch('kfp.client.auth.is_ipython', lambda *args: False)
    @patch.dict(os.environ, dict(), clear=True)
    def test_get_auth_code_from_local_invalid_response(self):
        self.assertRaises(KeyError, auth.get_auth_code, 'sample-client-id')
