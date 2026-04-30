"""user_presets/*.json → ScreenPreset 加载测试。"""

import json
from pathlib import Path

import pytest

from rquant.presets import load_user_presets


class TestLoadUserPresets:
    def test_empty_dir_returns_empty(self, tmp_path: Path) -> None:
        result = load_user_presets(tmp_path)
        assert result == {}

    def test_nonexistent_dir_returns_empty(self, tmp_path: Path) -> None:
        result = load_user_presets(tmp_path / "not_there")
        assert result == {}

    def test_load_single_preset(self, tmp_path: Path) -> None:
        (tmp_path / "突破新高.json").write_text(json.dumps({
            "name": "突破新高",
            "description": "今日突破 60 日新高",
            "rules": [
                {"name": "not_st", "args": {}},
                {"name": "above_ma", "args": {"period": 60, "offset": 0}},
            ],
            "include_columns": ["CIRC_MV[0]"],
            "created_at": "2026-04-30T15:00:00",
            "source": "nl_input",
        }, ensure_ascii=False))
        result = load_user_presets(tmp_path)
        assert "user/突破新高" in result
        preset = result["user/突破新高"]
        assert preset.name == "user/突破新高"
        assert len(preset.rules) == 2

    def test_load_skips_invalid_json(self, tmp_path: Path) -> None:
        (tmp_path / "bad.json").write_text("not valid json")
        result = load_user_presets(tmp_path)
        assert result == {}  # 不抛，记 warning 后跳过

    def test_load_skips_unknown_rule(self, tmp_path: Path) -> None:
        (tmp_path / "p.json").write_text(json.dumps({
            "name": "p",
            "description": "x",
            "rules": [{"name": "bogus_rule", "args": {}}],
            "created_at": "2026-04-30T00:00:00",
            "source": "nl_input",
        }))
        result = load_user_presets(tmp_path)
        assert result == {}  # 不抛，跳过此 preset
