# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

"""Regression for the evidence tooling itself (issue #11).

The failure this guards against is evidence-shaped: a check whose stdout carries the
failure assertion and whose stderr is longer, merged and truncated, leaves a manifest that
proves a failure happened but not what failed. The fake `cargo` below reproduces exactly
that shape, and the test asserts the full streams are on disk and hashed in the manifest.

Runs on POSIX; the fake tool is a shell script. The adapter CI runs it on Linux.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent


@unittest.skipIf(os.name == "nt", "the fake tool is a POSIX shell script")
class NativeChecksLogTest(unittest.TestCase):
    def test_failure_assertion_survives_a_long_stderr(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fake_bin = Path(tmp) / "bin"
            fake_bin.mkdir()
            fake_cargo = fake_bin / "cargo"
            fake_cargo.write_text(
                "#!/bin/sh\n"
                "printf 'assertion failed: the stdout diagnosis\\n'\n"
                "i=0\n"
                "while [ \"$i\" -lt 3000 ]; do\n"
                "  printf 'stderr noise %s\\n' \"$i\" >&2\n"
                "  i=$((i + 1))\n"
                "done\n"
                "exit 101\n",
                encoding="utf-8",
            )
            fake_cargo.chmod(fake_cargo.stat().st_mode | stat.S_IEXEC)

            output = Path(tmp) / "evidence.json"
            env = dict(os.environ)
            env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
            result = subprocess.run(
                [
                    sys.executable,
                    str(HERE / "native_checks.py"),
                    "--output",
                    str(output),
                    "--skip-clippy",
                ],
                cwd=HERE,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 1, "blocking checks failed, so the script must fail")
            manifest = json.loads(output.read_text(encoding="utf-8"))
            test_check = next(check for check in manifest["checks"] if check["name"] == "test")

            stdout_text = Path(test_check["stdout"]["path"]).read_text(encoding="utf-8")
            stderr_text = Path(test_check["stderr"]["path"]).read_text(encoding="utf-8")

            self.assertIn("assertion failed: the stdout diagnosis", stdout_text)
            self.assertGreater(len(stderr_text), 4_000)
            self.assertEqual(
                test_check["stdout"]["sha256"],
                hashlib.sha256(stdout_text.encode()).hexdigest(),
            )
            self.assertEqual(
                test_check["stderr"]["sha256"],
                hashlib.sha256(stderr_text.encode()).hexdigest(),
            )
            self.assertNotIn(
                "assertion failed",
                test_check["stderr"]["tail"],
                "the failure belongs to stdout and must not be claimed by the other stream",
            )


if __name__ == "__main__":
    unittest.main()
