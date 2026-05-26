# Copyright 2026 The Kubeflow Authors
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

import unittest

from safe_target_lambda import build_safe_target_lambda


class TestSafeTargetLambda(unittest.TestCase):

    def test_allows_row_arithmetic_and_comparison(self):
        target_lambda = build_safe_target_lambda(
            "lambda row: row['target'] > row['fare'] * 0.2")
        self.assertTrue(target_lambda({'target': 3, 'fare': 10}))
        self.assertFalse(target_lambda({'target': 1, 'fare': 10}))

    def test_rejects_function_calls(self):
        with self.assertRaisesRegex(ValueError, 'Call'):
            build_safe_target_lambda(
                "lambda row: __import__('os').system('touch /tmp/pwned')")

    def test_rejects_attribute_access(self):
        with self.assertRaisesRegex(ValueError, 'Attribute'):
            build_safe_target_lambda("lambda row: row.__class__")

    def test_rejects_non_lambda_expressions(self):
        with self.assertRaisesRegex(ValueError, 'lambda expression'):
            build_safe_target_lambda("__import__('os').system('id')")


if __name__ == "__main__":
    unittest.main()
