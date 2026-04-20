"""ScreenPreset 注册表单测。"""

from rquant.presets import PRESET_SCREENS, ScreenPreset


class TestScreenPreset:
    def test_pool1_registered(self) -> None:
        assert "n-shape-pool1" in PRESET_SCREENS

    def test_pool2_registered(self) -> None:
        assert "n-shape-pool2" in PRESET_SCREENS

    def test_pool1_has_11_rules(self) -> None:
        p = PRESET_SCREENS["n-shape-pool1"]
        assert len(p.rules) == 11

    def test_pool1_no_dependency(self) -> None:
        p = PRESET_SCREENS["n-shape-pool1"]
        assert p.depends_on is None

    def test_pool2_depends_on_pool1(self) -> None:
        p = PRESET_SCREENS["n-shape-pool2"]
        assert p.depends_on == "n-shape-pool1"
        assert p.offset_days == 2

    def test_pool2_has_3_rules(self) -> None:
        p = PRESET_SCREENS["n-shape-pool2"]
        assert len(p.rules) == 3

    def test_all_rules_callable(self) -> None:
        for name, preset in PRESET_SCREENS.items():
            for i, rule in enumerate(preset.rules):
                assert callable(rule), f"{name} rule[{i}] not callable"

    def test_pool1_include_columns(self) -> None:
        p = PRESET_SCREENS["n-shape-pool1"]
        assert "CIRC_MV[0]" in p.include_columns
        assert "BODY_UPPER[0]" in p.include_columns

    def test_preset_is_dataclass(self) -> None:
        p = PRESET_SCREENS["n-shape-pool1"]
        assert isinstance(p, ScreenPreset)

    def test_depends_on_target_exists(self) -> None:
        """所有 depends_on 指向的预设必须存在。"""
        for name, preset in PRESET_SCREENS.items():
            if preset.depends_on is not None:
                assert preset.depends_on in PRESET_SCREENS, (
                    f"{name}.depends_on='{preset.depends_on}' not in registry"
                )
