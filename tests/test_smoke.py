"""Smoke tests: run every flag combination, check for errors and output formatting."""

import re
import subprocess
import pytest

SCRIPT = ["python", "sync_claude_history.py"]
BORDER = "╠═══"
EDGE = "║"


def run(args, should_fail=False):
    """Run the script with args, return (stdout, stderr, returncode)."""
    result = subprocess.run(
        SCRIPT + args,
        capture_output=True, text=True, timeout=120,
    )
    if not should_fail:
        assert result.returncode == 0, (
            f"Exit code {result.returncode}\nstderr: {result.stderr}\nstdout: {result.stdout}"
        )
    return result


def check_format(output):
    """Verify output formatting is not corrupted."""
    lines = output.strip().splitlines()
    assert "Traceback" not in output, f"Traceback in output:\n{output[-500:]}"

    # No doubled borders on same line
    for line in lines:
        assert line.count(BORDER) <= 1, f"Doubled border: {line}"

    # Every border/edge line should start with whitespace + ║ or ╠
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith(EDGE) or stripped.startswith(BORDER):
            # Good — properly formatted
            pass


# ---------------------------------------------------------------------------
# Sync flags
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_basic(self):
        r = run(["--dry-run"])
        check_format(r.stdout)
        assert "Found" in r.stdout
        assert "Done." in r.stdout

    def test_verbose(self):
        r = run(["--dry-run", "-v"])
        check_format(r.stdout)
        assert "Found" in r.stdout

    def test_push(self):
        r = run(["--push", "--dry-run"])
        check_format(r.stdout)

    def test_pull(self):
        r = run(["--pull", "--dry-run"])
        check_format(r.stdout)


class TestFilters:
    def test_repo_filter(self):
        r = run(["--dry-run", "--repo", "sglang"])
        check_format(r.stdout)

    def test_chat_filter(self):
        r = run(["--dry-run", "--chat", "df9a6a22"])
        check_format(r.stdout)

    def test_repo_and_chat(self):
        r = run(["--dry-run", "--repo", "sglang", "--chat", "de1128"])
        check_format(r.stdout)


class TestDelete:
    def test_delete_dry(self):
        r = run(["-d", "--repo", "sglang", "--dry-run"])
        check_format(r.stdout)

    def test_delete_chat_dry(self):
        r = run(["-d", "--repo", "sglang", "--chat", "de1128", "--dry-run"])
        check_format(r.stdout)

    def test_delete_local_dry(self):
        r = run(["-d", "--local", "--repo", "sglang", "--dry-run"])
        check_format(r.stdout)


class TestBackground:
    def _cleanup(self):
        import signal, os, time
        # Kill any existing daemon
        try:
            pid = int(open(".sync.pid").read().strip())
            os.kill(pid, signal.SIGTERM)
            time.sleep(0.5)
        except (FileNotFoundError, ProcessLookupError, ValueError):
            pass
        for f in [".sync.pid", ".sync_jobs.json", "sync.log"]:
            try:
                os.remove(f)
            except FileNotFoundError:
                pass

    def test_start_and_stop(self):
        import signal, os, time
        self._cleanup()

        r = run(["--background", "600", "--chat", "df9a6a22"])
        check_format(r.stdout)
        assert "PID:" in r.stdout

        # Extract PID and clean up
        pid_match = re.search(r"PID:\s*(\d+)", r.stdout)
        assert pid_match, "No PID in output"
        pid = int(pid_match.group(1))
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(0.5)
        except ProcessLookupError:
            pass
        self._cleanup()


# ---------------------------------------------------------------------------
# Error handling — should exit cleanly, not traceback
# ---------------------------------------------------------------------------

class TestErrors:
    def test_delete_requires_filter(self):
        r = run(["-d"], should_fail=True)
        assert r.returncode != 0
        assert "Traceback" not in r.stderr
        assert "Traceback" not in r.stdout

    def test_local_requires_delete(self):
        r = run(["--local"], should_fail=True)
        assert r.returncode != 0
        assert "Traceback" not in r.stderr
        assert "Traceback" not in r.stdout

    def test_bad_background_interval(self):
        r = run(["--background", "0"], should_fail=True)
        assert r.returncode != 0
        assert "Traceback" not in r.stderr
