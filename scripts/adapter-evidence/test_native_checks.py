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

import _identity
import native_checks

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

            stdout_bytes = Path(test_check["stdout"]["path"]).read_bytes()
            stderr_bytes = Path(test_check["stderr"]["path"]).read_bytes()

            self.assertIn(b"assertion failed: the stdout diagnosis", stdout_bytes)
            self.assertGreater(len(stderr_bytes), 4_000)
            # The manifest must describe the bytes on disk, not a re-encoding of them.
            self.assertEqual(
                test_check["stdout"]["sha256"],
                hashlib.sha256(stdout_bytes).hexdigest(),
            )
            self.assertEqual(test_check["stdout"]["bytes"], len(stdout_bytes))
            self.assertEqual(
                test_check["stderr"]["sha256"],
                hashlib.sha256(stderr_bytes).hexdigest(),
            )
            self.assertEqual(test_check["stderr"]["bytes"], len(stderr_bytes))
            self.assertNotIn(
                "assertion failed",
                test_check["stderr"]["tail"],
                "the failure belongs to stdout and must not be claimed by the other stream",
            )


class LogBytesTest(unittest.TestCase):
    """The log hash and length describe the file as written, on every platform."""

    def test_a_crlf_payload_is_hashed_as_written(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            entry = native_checks._write_log(
                Path(tmp), "check", "stderr", "assertion failed\r\nsecond line\r\n"
            )
            payload = Path(entry["path"]).read_bytes()

            self.assertEqual(payload, b"assertion failed\r\nsecond line\r\n")
            self.assertEqual(entry["bytes"], len(payload))
            self.assertEqual(entry["sha256"], hashlib.sha256(payload).hexdigest())


class UntrackedIdentityTest(unittest.TestCase):
    """Untracked names git would quote still contribute their contents to the digest."""

    def test_special_filenames_change_the_digest_with_their_contents(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)

            names = ["ordinary.rs", "new module.rs", "策略.rs"]
            if os.name != "nt":
                # NTFS refuses control characters and double quotes in a filename.
                names.extend(['quote"name.rs', "line\nbreak.rs"])

            for name in names:
                (root / name).write_text("VALUE=1", encoding="utf-8")
            first = _identity._untracked_digest(root)

            for name in names:
                (root / name).write_text("VALUE=2", encoding="utf-8")
            second = _identity._untracked_digest(root)
            self.assertNotEqual(first, second)

            # Each name contributes on its own: a change behind a quoted path is still seen.
            current = second
            for name in names:
                (root / name).write_text("VALUE=3", encoding="utf-8")
                changed = _identity._untracked_digest(root)
                self.assertNotEqual(
                    current, changed, f"a content change in {name!r} must move the digest"
                )
                current = changed

    def test_a_listed_path_that_cannot_be_read_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            broken = root / "broken.rs"
            broken.write_text("VALUE=1", encoding="utf-8")
            if os.name == "nt":
                self.skipTest("a dangling symlink needs developer mode on Windows")
            broken.unlink()
            broken.symlink_to(root / "missing.rs")

            with self.assertRaises(OSError):
                _identity._untracked_digest(root)


if __name__ == "__main__":
    unittest.main()
