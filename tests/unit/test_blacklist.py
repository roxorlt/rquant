"""风险黑名单：解析 / 入库 / 过滤 / 标签 单测。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from rquant.risk.blacklist import (
    BlacklistEntry,
    annotate_blacklist,
    export_blacklist_parquet,
    filter_blacklist,
    import_blacklist,
    load_active_blacklist,
    load_blacklist_parquet,
    normalize_ts_code,
    parse_blacklist_pdf,
)
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture
def tmp_store(tmp_path: Path) -> DuckDBStore:
    return DuckDBStore(path=tmp_path / "test.duckdb")


class TestNormalizeTsCode:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("000016", "000016.SZ"),
            ("16", "000016.SZ"),       # 缺前导 0
            ("000056", "000056.SZ"),
            ("56", "000056.SZ"),       # PDF 第 2 页常见
            ("002269", "002269.SZ"),
            ("300027", "300027.SZ"),
            ("301139", "301139.SZ"),
            ("600340", "600340.SH"),
            ("601798", "601798.SH"),
            ("603023", "603023.SH"),
            ("605336", "605336.SH"),
            ("688066", "688066.SH"),
            ("920023", "920023.BJ"),   # 北交所
            ("920575", "920575.BJ"),
        ],
    )
    def test_suffix_mapping(self, raw, expected):
        assert normalize_ts_code(raw) == expected

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            normalize_ts_code("abc123")


class TestParseBlacklistPDF:
    PDF = Path.home() / "Downloads" / "风险控制名单.pdf"

    @pytest.mark.skipif(
        not (Path.home() / "Downloads" / "风险控制名单.pdf").exists(),
        reason="需要本地 PDF",
    )
    def test_parses_real_pdf(self):
        entries = parse_blacklist_pdf(self.PDF)
        # 187 raw 行 → 去重约 147 个独立标的
        assert 130 <= len(entries) <= 160

        codes = {e.ts_code for e in entries}
        # 第 2 页代码缺前导 0 但应被补齐
        assert "000056.SZ" in codes
        # 多类别合并：华夏幸福同时命中"净资产为负"+"年报审计"+"两年利润为负"
        hx = next(e for e in entries if e.name == "华夏幸福")
        assert len(hx.sub_categories) >= 2
        assert hx.ts_code == "600340.SH"


class TestImportAndQuery:
    def _entries(self) -> list[BlacklistEntry]:
        return [
            BlacklistEntry(
                ts_code="000016.SZ",
                name="深康佳A",
                risk_type="ST预警",
                sub_categories=["净资产为负风险", "两年利润为负"],
            ),
            BlacklistEntry(
                ts_code="600340.SH",
                name="华夏幸福",
                risk_type="ST预警",
                sub_categories=["净资产为负风险"],
            ),
        ]

    def test_import_and_load_active(self, tmp_store):
        n = import_blacklist(
            self._entries(),
            list_label="430黑名单",
            source_file="test.pdf",
            store=tmp_store,
            imported_at=date(2026, 4, 30),
        )
        assert n == 2

        active = load_active_blacklist(tmp_store, today=date(2026, 5, 1))
        assert len(active) == 2
        kj = active["000016.SZ"]
        assert kj.name == "深康佳A"
        assert kj.list_label == "430黑名单"
        assert "净资产为负风险" in kj.sub_categories
        assert kj.expires_at == date(2027, 4, 30)

    def test_replace_clears_old_entries(self, tmp_store):
        import_blacklist(
            self._entries(),
            list_label="430黑名单",
            source_file="v1.pdf",
            store=tmp_store,
        )
        # 第二次只导一条
        import_blacklist(
            [self._entries()[0]],
            list_label="430黑名单",
            source_file="v2.pdf",
            store=tmp_store,
        )
        active = load_active_blacklist(tmp_store, include_expired=True)
        assert len(active) == 1
        assert "000016.SZ" in active

    def test_expired_excluded_by_default(self, tmp_store):
        import_blacklist(
            self._entries(),
            list_label="430黑名单",
            source_file="x.pdf",
            store=tmp_store,
            imported_at=date(2026, 4, 30),
            validity_days=30,
        )
        # 31 天后查
        active = load_active_blacklist(tmp_store, today=date(2026, 6, 1))
        assert len(active) == 0
        all_inc = load_active_blacklist(
            tmp_store, today=date(2026, 6, 1), include_expired=True
        )
        assert len(all_inc) == 2


class TestFilterAndAnnotate:
    @pytest.fixture
    def populated_store(self, tmp_store):
        import_blacklist(
            [
                BlacklistEntry("000016.SZ", "深康佳A", "ST预警", ["净资产为负"]),
                BlacklistEntry("600340.SH", "华夏幸福", "ST预警", ["年报审计"]),
            ],
            list_label="430黑名单",
            source_file="t.pdf",
            store=tmp_store,
            imported_at=date.today(),
        )
        return tmp_store

    def test_filter_removes_blacklisted(self, populated_store):
        codes = ["000016.SZ", "600340.SH", "002415.SZ"]
        clean, removed = filter_blacklist(codes, store=populated_store)
        assert clean == ["002415.SZ"]
        assert {h.ts_code for h in removed} == {"000016.SZ", "600340.SH"}

    def test_annotate_keeps_all(self, populated_store):
        codes = ["000016.SZ", "002415.SZ"]
        out = annotate_blacklist(codes, store=populated_store)
        assert len(out) == 2
        assert out[0][1] is not None  # hit
        assert out[0][1].label_short == "[430黑名单]"
        assert out[1][1] is None      # clean

    def test_filter_empty_blacklist(self, tmp_store):
        # 空黑名单时不应误剔除
        clean, removed = filter_blacklist(["000016.SZ"], store=tmp_store)
        assert clean == ["000016.SZ"]
        assert removed == []


class TestParquetRoundtrip:
    """parquet 导入/导出闭环——v0.10.0 upload-api 推送后 mac → 云端。"""

    def _entries(self) -> list[BlacklistEntry]:
        return [
            BlacklistEntry(
                ts_code="000016.SZ", name="深康佳A",
                risk_type="ST预警",
                sub_categories=["净资产为负风险", "两年利润为负"],
            ),
            BlacklistEntry(
                ts_code="600340.SH", name="华夏幸福",
                risk_type="ST预警",
                sub_categories=["净资产为负风险"],
            ),
        ]

    def test_export_then_load_roundtrip(self, tmp_store, tmp_path: Path):
        # mac 端：导入 PDF（这里用 entries 模拟）→ export-parquet
        import_blacklist(
            self._entries(),
            list_label="430黑名单",
            source_file="test.pdf",
            store=tmp_store,
            imported_at=date(2026, 4, 30),
        )

        out = tmp_path / "blacklist.parquet"
        n = export_blacklist_parquet(tmp_store, out)
        assert n == 2
        assert out.exists()
        assert out.stat().st_size > 0

        # 云端：用全新 store 模拟，load-parquet → 验证内容一致
        with DuckDBStore(path=tmp_path / "cloud.duckdb") as cloud_store:
            n_loaded = load_blacklist_parquet(out, cloud_store)
            assert n_loaded == 2
            active = load_active_blacklist(cloud_store, today=date(2026, 5, 1))
            assert len(active) == 2
            kj = active["000016.SZ"]
            assert kj.name == "深康佳A"
            assert kj.list_label == "430黑名单"
            assert "净资产为负风险" in kj.sub_categories
            assert kj.expires_at == date(2027, 4, 30)

    def test_load_replaces_only_specified_label(self, tmp_store, tmp_path: Path):
        # 先填两个 label
        import_blacklist(
            self._entries(),
            list_label="430黑名单",
            source_file="a.pdf",
            store=tmp_store,
        )
        import_blacklist(
            [BlacklistEntry(
                ts_code="002415.SZ", name="海康威视",
                risk_type="其他", sub_categories=["其他"],
            )],
            list_label="自定义名单",
            source_file="b.pdf",
            store=tmp_store,
        )

        # 导出"430黑名单" → 模拟云端 load-parquet --label 430黑名单
        out = tmp_path / "430.parquet"
        export_blacklist_parquet(tmp_store, out, list_label="430黑名单")

        # 在云端 store load 回去（用 label 限定）
        with DuckDBStore(path=tmp_path / "cloud.duckdb") as cloud_store:
            # 先种一个不同 label 模拟云端已有数据
            import_blacklist(
                [BlacklistEntry(
                    ts_code="999999.SZ", name="不该被删",
                    risk_type="其他", sub_categories=["x"],
                )],
                list_label="other-list",
                source_file="seed.pdf",
                store=cloud_store,
            )
            load_blacklist_parquet(out, cloud_store, list_label="430黑名单")

            all_inc = load_active_blacklist(cloud_store, include_expired=True)
            # 应该有 430 的 2 条 + other-list 的 1 条 = 3
            assert len(all_inc) == 3
            assert "999999.SZ" in all_inc
            assert all_inc["999999.SZ"].list_label == "other-list"

    def test_load_full_table_replaces_all(self, tmp_store, tmp_path: Path):
        import_blacklist(
            self._entries(),
            list_label="430黑名单",
            source_file="a.pdf",
            store=tmp_store,
        )
        out = tmp_path / "all.parquet"
        export_blacklist_parquet(tmp_store, out)

        with DuckDBStore(path=tmp_path / "cloud.duckdb") as cloud_store:
            # 云端原本有干扰数据
            import_blacklist(
                [BlacklistEntry(
                    ts_code="999999.SZ", name="将被全表覆盖删除",
                    risk_type="其他", sub_categories=["x"],
                )],
                list_label="other-list",
                source_file="seed.pdf",
                store=cloud_store,
            )
            # 不传 list_label = 全表覆盖
            n = load_blacklist_parquet(out, cloud_store, list_label=None)
            assert n == 2
            all_inc = load_active_blacklist(cloud_store, include_expired=True)
            assert len(all_inc) == 2
            assert "999999.SZ" not in all_inc

    def test_load_missing_parquet_raises(self, tmp_store, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            load_blacklist_parquet(tmp_path / "nonexistent.parquet", tmp_store)
