"""notify.log JSONL append / read 单测。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest


@pytest.fixture()
def tmp_log_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """重定向 settings.log_dir 到 tmp_path，避免污染真实 logs/。"""
    from rquant.config import settings

    monkeypatch.setattr(settings, "log_dir", tmp_path)
    return tmp_path


class TestAppend:
    def test_append_creates_file_and_writes_jsonl(self, tmp_log_dir: Path) -> None:
        from rquant.notify import log

        log.append("daily_summary", "pushdeer", "abcd1234", True, None, "标题 X")

        path = tmp_log_dir / "notification_log.jsonl"
        assert path.exists()
        entry = json.loads(path.read_text().strip())
        assert entry["scene"] == "daily_summary"
        assert entry["channel"] == "pushdeer"
        assert entry["target"] == "abcd1234"
        assert entry["success"] is True
        assert entry["error_msg"] is None
        assert entry["title"] == "标题 X"
        assert "sent_at" in entry

    def test_append_appends_not_truncates(self, tmp_log_dir: Path) -> None:
        from rquant.notify import log

        log.append("a", "pushdeer", "t1", True, None, "1")
        log.append("b", "pushplus", "t2", False, "boom", "2")

        path = tmp_log_dir / "notification_log.jsonl"
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["scene"] == "a"
        assert json.loads(lines[1])["scene"] == "b"
        assert json.loads(lines[1])["error_msg"] == "boom"

    def test_append_failure_does_not_raise(
        self, tmp_log_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """文件不可写时 append 应记 error 不抛。"""
        from rquant.notify import log

        def boom(*_, **__):
            raise OSError("disk full")

        monkeypatch.setattr(Path, "open", boom)
        log.append("a", "pushdeer", "t", True, None, "x")  # should not raise


class TestReadRecent:
    def test_empty_file_returns_empty_df(self, tmp_log_dir: Path) -> None:
        from rquant.notify import log

        df = log.read_recent()
        assert df.empty

    def test_returns_sorted_desc_with_limit(self, tmp_log_dir: Path) -> None:
        from rquant.notify import log

        # 写 5 条，read_recent(limit=3) 返回最新 3 条
        for i in range(5):
            log.append(f"s{i}", "pushdeer", "t", True, None, f"title {i}")

        df = log.read_recent(limit=3)
        assert len(df) == 3
        # 因为 append 顺序写入 sent_at 单调递增，最新 = s4
        assert df.iloc[0]["scene"] == "s4"
        assert df.iloc[1]["scene"] == "s3"
        assert df.iloc[2]["scene"] == "s2"

    def test_skips_corrupted_lines(self, tmp_log_dir: Path) -> None:
        """JSONL 文件中坏行不应破坏读取。"""
        from rquant.notify import log

        path = tmp_log_dir / "notification_log.jsonl"
        path.write_text(
            json.dumps({
                "sent_at": "2026-05-22T13:00:00",
                "scene": "a", "channel": "pushdeer",
                "target": "t", "success": True,
                "error_msg": None, "title": "x",
            }) + "\n"
            + "this is not json\n"
            + json.dumps({
                "sent_at": "2026-05-22T14:00:00",
                "scene": "b", "channel": "pushdeer",
                "target": "t", "success": True,
                "error_msg": None, "title": "y",
            }) + "\n"
        )

        df = log.read_recent()
        assert len(df) == 2
        assert set(df["scene"]) == {"a", "b"}


class TestReadSince:
    def test_filters_by_hours(
        self, tmp_log_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from rquant.notify import log

        path = tmp_log_dir / "notification_log.jsonl"
        now = datetime.now()
        lines = [
            json.dumps({
                "sent_at": (now - timedelta(hours=48)).isoformat(timespec="seconds"),
                "scene": "old", "channel": "pushdeer",
                "target": "t", "success": True, "error_msg": None, "title": "x",
            }),
            json.dumps({
                "sent_at": (now - timedelta(hours=2)).isoformat(timespec="seconds"),
                "scene": "recent", "channel": "pushdeer",
                "target": "t", "success": True, "error_msg": None, "title": "y",
            }),
        ]
        path.write_text("\n".join(lines) + "\n")

        df = log.read_since(hours=24)
        assert len(df) == 1
        assert df.iloc[0]["scene"] == "recent"
