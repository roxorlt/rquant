"""Crawl Tushare official docs and rank A-share interfaces for rQuant.

This script reads `TUSHARE_COOKIE` from `.env`. It intentionally writes to a
separate DuckDB file instead of the main rQuant DB to avoid monitor lock issues.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from dotenv import dotenv_values

from rquant.tushare_docs import (
    TushareAuditRow,
    TushareDocLink,
    TushareMcpTool,
    classify_tushare_interface,
    derive_interface_catalog_rows,
    parse_mcp_tools,
    parse_tushare_account_score,
    parse_tushare_activity_packages,
    parse_tushare_doc_page,
    parse_tushare_menu,
    parse_tushare_purchase_goods,
    render_audit_markdown,
    write_account_score,
    write_activity_packages,
    write_audit_rows,
    write_interface_catalog_rows,
    write_purchase_goods,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_DIR = PROJECT_ROOT / "data" / "tushare_doc_cache"
DEFAULT_DB = PROJECT_ROOT / "data" / "tushare_interface_audit.duckdb"
DEFAULT_REPORT = PROJECT_ROOT / "docs" / "analysis" / "2026-06-26-tushare-interface-audit.md"


def _run_curl(args: list[str], *, timeout: int = 45) -> str:
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"curl timed out after {timeout}s") from exc
    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise RuntimeError(f"curl failed ({result.returncode}): {stderr[:300]}")
    return result.stdout


def fetch_doc_html(
    doc_id: int,
    cookie: str,
    cache_dir: Path,
    *,
    refresh: bool = False,
) -> str:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{doc_id}.html"
    if cache_path.exists() and not refresh:
        return cache_path.read_text(encoding="utf-8")

    url = f"https://tushare.pro/document/2?doc_id={doc_id}"
    last_error: Exception | None = None
    html = ""
    for attempt in range(1, 4):
        try:
            html = _run_curl(
                [
                    "curl",
                    "-sS",
                    "-L",
                    "--http1.1",
                    "--retry",
                    "2",
                    "--retry-delay",
                    "1",
                    "--max-time",
                    "18",
                    "-H",
                    "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
                    "-H",
                    f"Cookie: {cookie}",
                    url,
                ],
                timeout=22,
            )
            break
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(attempt)
    if not html and last_error is not None:
        raise last_error
    if "/weborder/#/login" in html or "登录注册" in html:
        raise RuntimeError(f"doc_id={doc_id} redirected to login")
    cache_path.write_text(html, encoding="utf-8")
    return html


def fetch_mcp_tools(
    token: str | None,
    cache_path: Path,
    *,
    refresh: bool = False,
) -> dict[str, TushareMcpTool]:
    if cache_path.exists() and not refresh:
        tools = parse_mcp_tools(cache_path.read_text(encoding="utf-8"))
        return {tool.name: tool for tool in tools}
    if not token:
        return {}
    payload = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        ensure_ascii=False,
    )
    try:
        body = _run_curl(
            [
                "curl",
                "-sS",
                "--max-time",
                "30",
                "-H",
                "Content-Type: application/json",
                "--data",
                payload,
                f"https://api.tushare.pro/mcp/?token={token}",
            ],
            timeout=35,
        )
    except Exception as exc:
        if cache_path.exists():
            print(f"MCP fetch failed, using cache: {type(exc).__name__}")
            tools = parse_mcp_tools(cache_path.read_text(encoding="utf-8"))
            return {tool.name: tool for tool in tools}
        print(f"MCP fetch failed, continuing without MCP: {type(exc).__name__}")
        return {}
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(body, encoding="utf-8")
    tools = parse_mcp_tools(body)
    return {tool.name: tool for tool in tools}


def fetch_weborder_json(
    path: str,
    cookie: str,
    cache_path: Path,
    *,
    refresh: bool = False,
) -> dict[str, object]:
    if cache_path.exists() and not refresh:
        return json.loads(cache_path.read_text(encoding="utf-8"))

    url = f"https://tushare.pro{path}"
    last_error: Exception | None = None
    body = ""
    for attempt in range(1, 4):
        try:
            body = _run_curl(
                [
                    "curl",
                    "-sS",
                    "-L",
                    "--http1.1",
                    "--retry",
                    "2",
                    "--retry-delay",
                    "1",
                    "--max-time",
                    "18",
                    "-H",
                    "Accept: application/json, text/plain, */*",
                    "-H",
                    "Referer: https://tushare.pro/weborder/",
                    "-H",
                    "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
                    "-H",
                    f"Cookie: {cookie}",
                    url,
                ],
                timeout=22,
            )
            break
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(attempt)
    if not body and last_error is not None:
        raise last_error
    parsed = json.loads(body)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
    return parsed


def load_env() -> tuple[str, str | None]:
    values = dotenv_values(PROJECT_ROOT / ".env")
    cookie = values.get("TUSHARE_COOKIE") or ""
    token = values.get("TUSHARE_TOKEN_MAIN") or values.get("tushare_token_main")
    if not cookie:
        raise RuntimeError("missing TUSHARE_COOKIE in .env")
    return cookie, token


def _build_one_row(
    link: TushareDocLink,
    *,
    cookie: str,
    mcp_tools: dict[str, TushareMcpTool],
    cache_dir: Path,
    refresh: bool,
    fetched_at: datetime,
) -> TushareAuditRow:
    html = fetch_doc_html(link.doc_id, cookie, cache_dir, refresh=refresh)
    page = parse_tushare_doc_page(html, link)
    mcp_tool = mcp_tools.get(page.api_name or "")
    return classify_tushare_interface(page, mcp_tool, fetched_at=fetched_at)


def build_audit_rows(
    *,
    seed_doc_id: int,
    cookie: str,
    mcp_tools: dict[str, TushareMcpTool],
    cache_dir: Path,
    limit: int | None = None,
    refresh: bool = False,
    sleep_seconds: float = 0.05,
    workers: int = 6,
) -> list[TushareAuditRow]:
    seed_html = fetch_doc_html(seed_doc_id, cookie, cache_dir, refresh=refresh)
    links = parse_tushare_menu(seed_html)
    if limit is not None:
        links = links[:limit]

    fetched_at = datetime.now()
    rows: list[TushareAuditRow] = []
    failures: list[tuple[int, str]] = []
    if workers <= 1:
        for index, link in enumerate(links, start=1):
            try:
                rows.append(
                    _build_one_row(
                        link,
                        cookie=cookie,
                        mcp_tools=mcp_tools,
                        cache_dir=cache_dir,
                        refresh=refresh,
                        fetched_at=fetched_at,
                    )
                )
            except Exception as exc:
                failures.append((link.doc_id, str(exc)))
            if index % 25 == 0:
                print(
                    f"fetched {index}/{len(links)} docs; failures={len(failures)}",
                    flush=True,
                )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _build_one_row,
                    link,
                    cookie=cookie,
                    mcp_tools=mcp_tools,
                    cache_dir=cache_dir,
                    refresh=refresh,
                    fetched_at=fetched_at,
                ): link
                for link in links
            }
            for index, future in enumerate(as_completed(futures), start=1):
                link = futures[future]
                try:
                    rows.append(future.result())
                except Exception as exc:
                    failures.append((link.doc_id, str(exc)))
                if index % 25 == 0:
                    print(
                        f"fetched {index}/{len(links)} docs; failures={len(failures)}",
                        flush=True,
                    )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

    if failures:
        print("fetch failures:")
        for doc_id, reason in failures[:20]:
            print(f"- doc_id={doc_id}: {reason[:180]}")
        if len(failures) > 20:
            print(f"- ... {len(failures) - 20} more")
    return rows


def write_report(rows: list[TushareAuditRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_audit_markdown(rows), encoding="utf-8")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed-doc-id", type=int, default=374)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--out-db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-md", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    cookie, token = load_env()
    print("TUSHARE_COOKIE: SET")
    print("TUSHARE_TOKEN_MAIN:", "SET" if token else "EMPTY")
    mcp_tools = fetch_mcp_tools(
        token,
        args.cache_dir / "mcp_tools.json",
        refresh=args.refresh,
    )
    print(f"mcp_tools={len(mcp_tools)}")

    rows = build_audit_rows(
        seed_doc_id=args.seed_doc_id,
        cookie=cookie,
        mcp_tools=mcp_tools,
        cache_dir=args.cache_dir,
        limit=args.limit,
        refresh=args.refresh,
        sleep_seconds=args.sleep,
        workers=args.workers,
    )
    catalog_rows = derive_interface_catalog_rows(rows)
    count = write_audit_rows(rows, args.out_db)
    catalog_count = write_interface_catalog_rows(catalog_rows, args.out_db)

    price_fetched_at = datetime.now()
    score_payload = fetch_weborder_json(
        "/wctapi/user_center/score",
        cookie,
        args.cache_dir / "weborder_user_score.json",
        refresh=args.refresh,
    )
    point_goods_payload = fetch_weborder_json(
        "/wctapi/goods?type=1",
        cookie,
        args.cache_dir / "weborder_goods_type1.json",
        refresh=args.refresh,
    )
    permission_goods_payload = fetch_weborder_json(
        "/wctapi/goods?type=2",
        cookie,
        args.cache_dir / "weborder_goods_type2.json",
        refresh=args.refresh,
    )
    packages_payload = fetch_weborder_json(
        "/wctapi/activity_packages?channel=PC",
        cookie,
        args.cache_dir / "weborder_activity_packages.json",
        refresh=args.refresh,
    )
    purchase_goods = [
        *parse_tushare_purchase_goods(point_goods_payload, fetched_at=price_fetched_at),
        *parse_tushare_purchase_goods(permission_goods_payload, fetched_at=price_fetched_at),
    ]
    score_count = write_account_score(
        parse_tushare_account_score(score_payload, fetched_at=price_fetched_at),
        args.out_db,
    )
    goods_count = write_purchase_goods(purchase_goods, args.out_db)
    package_count = write_activity_packages(
        parse_tushare_activity_packages(packages_payload, fetched_at=price_fetched_at),
        args.out_db,
    )
    write_report(rows, args.out_md)

    a_share = sum(1 for row in rows if row.is_a_share_related)
    priority = sum(
        1
        for row in rows
        if row.is_a_share_related and row.priority <= 2 and not row.is_section
    )
    print(
        f"rows={count} catalog_rows={catalog_count} "
        f"a_share_related={a_share} priority_1_2={priority}"
    )
    print(
        f"score_rows={score_count} purchase_goods={goods_count} "
        f"activity_packages={package_count}"
    )
    print(f"db={args.out_db}")
    print(f"report={args.out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
