"""Build an indexed single-stock history generation outside the web process."""

from __future__ import annotations

import argparse
from pathlib import Path

from rquant.screen.formula_history_projection import publish_formula_history_projection
from rquant.screen.replica_source import VerifiedReplicaScreenSource


def main() -> None:
    parser = argparse.ArgumentParser(description="离线构建单股公式预览历史")
    parser.add_argument("--primary", type=Path, required=True, help="主库身份路径（不会读取）")
    parser.add_argument("--replica", type=Path, required=True, help="已核验只读副本")
    parser.add_argument("--output", type=Path, required=True, help="独立历史投影目录")
    args = parser.parse_args()
    catalog = publish_formula_history_projection(
        VerifiedReplicaScreenSource(
            primary_path=args.primary, replica_path=args.replica,
        ),
        args.output,
    )
    print(f"已发布公式预览历史：{len(catalog.dates)} 个可选日期")


if __name__ == "__main__":
    main()
