"""09:27 simple attribution for the 竞价选股 picks (rquant.research.auction_pool).

For today's final picks plus the top of the raw 同花顺 screen it gathers
  * concepts   Tushare ths_member -> ths_index names (同花顺 概念/行业)
  * news       Tushare major_news titles since T-1 15:00 mentioning the stock, plus the
               public Eastmoney news search (fallback when Tushare news is not permitted)
  * anns       Eastmoney announcements of the last few days (Tushare anns_d not permitted)
and asks DeepSeek (DEEPSEEK_* env, the web secrets file) for a 1-2 sentence common-driver
explanation. Without a key it falls back to the shared concepts. The result is stored as
``attribution`` in ``pools/auction/YYYY-MM-DD.json`` (shown on the 池子 page) and pushed via
PushDeer once (``pushed_at`` guards re-runs).

    python -m rquant.research.auction_attrib [--day D] [--no-push] [--force-push]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Any

from rquant.research.auction_pool import TZ, _now, _write, read_pool, research_root

log = logging.getLogger("rquant.auction_attrib")
ENV_FILE = os.environ.get("AUCTION_ATTRIB_ENV_FILE") or os.path.expanduser("~/rquant/.env")
MAX_TARGETS = 6
UA = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.eastmoney.com/"}
# broad index/membership tags that explain nothing
NOISE = ("样本股", "成份股", "成分股", "融资融券", "沪股通", "深股通", "标普", "MSCI", "富时",
         "转融券", "预增", "预盈", "同花顺漂亮", "破净", "指数", "证金持股", "汇金持股")
SYSTEM = ("你是A股盘前竞价复盘助手。根据给出的今日竞价选股结果、各股同花顺概念、"
          "当日新闻与公告，用1-2句中文（不超过80字）概括这些股票竞价走强的共同驱动因素，"
          "格式类似“AI电影即将上映消息带动影视传媒”。只依据给出的材料，材料不足时说明"
          "“暂无明确共同消息，或为X板块情绪延续”。不要编造数字，不要投资建议。")


def _get(url: str, timeout: float = 8.0) -> str:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 - fixed hosts
        return r.read().decode("utf-8", "ignore")


# --------------------------------------------------------------------------- sources
def ths_concepts(codes: list[str]) -> dict[str, list[str]]:
    from rquant.research.snapshots import tushare_call, tushare_token

    tok = tushare_token()
    if not tok:
        return {}
    names: dict[str, str] = {}
    for typ in ("N",):  # 概念 only; industry comes from limit_list
        try:
            idx = tushare_call("ths_index", {"exchange": "A", "type": typ},
                               ["ts_code", "name"], tok, timeout=15)
            names |= dict(zip(idx["ts_code"], idx["name"], strict=False))
        except Exception as exc:  # noqa: BLE001
            log.info("ths_index %s: %s", typ, str(exc)[:120])
    out: dict[str, list[str]] = {}
    for c in codes:
        try:
            m = tushare_call("ths_member", {"con_code": c}, ["ts_code", "con_code"], tok,
                             timeout=15)
        except Exception as exc:  # noqa: BLE001
            log.info("ths_member %s: %s", c, str(exc)[:120])
            continue
        out[c] = list(dict.fromkeys(n for n in (names.get(t) for t in m["ts_code"]) if n
                                    and not any(x in n for x in NOISE)))
    return out


def major_news(since: datetime, until: datetime) -> list[dict]:
    from rquant.research.snapshots import tushare_call, tushare_token

    tok = tushare_token()
    if not tok:
        return []
    try:
        df = tushare_call("major_news", {"start_date": f"{since:%Y-%m-%d %H:%M:%S}",
                                         "end_date": f"{until:%Y-%m-%d %H:%M:%S}"},
                          ["title", "pub_time", "src"], tok, timeout=20)
    except Exception as exc:  # noqa: BLE001
        log.info("major_news: %s", str(exc)[:120])
        return []
    return df.to_dict("records")


def em_news(name: str, since: datetime, until: datetime, n: int = 4) -> list[dict]:
    param = {"uid": "", "keyword": name, "type": ["cmsArticleWebOld"], "client": "web",
             "clientType": "web", "clientVersion": "curr",
             "param": {"cmsArticleWebOld": {"searchScope": "default", "sort": "time",
                                            "pageIndex": 1, "pageSize": 10,
                                            "preTag": "", "postTag": ""}}}
    url = ("https://search-api-web.eastmoney.com/search/jsonp?cb=x&param="
           + urllib.parse.quote(json.dumps(param, ensure_ascii=False)))
    try:
        raw = _get(url)
        data = json.loads(raw[raw.index("(") + 1:raw.rindex(")")])
        items = (data.get("result") or {}).get("cmsArticleWebOld") or []
    except Exception as exc:  # noqa: BLE001
        log.info("em news %s: %s", name, str(exc)[:120])
        return []
    lo, hi = f"{since:%Y-%m-%d %H:%M:%S}", f"{until:%Y-%m-%d %H:%M:%S}"
    out = [{"time": i.get("date"), "title": i.get("title"), "src": i.get("mediaName"),
            "snippet": (i.get("content") or "")[:120]}
           for i in items if lo <= str(i.get("date") or "") <= hi]
    return out[:n]


def em_anns(code: str, since: date, n: int = 3) -> list[dict]:
    url = ("https://np-anotice-stock.eastmoney.com/api/security/ann?sr=-1&page_size=10"
           f"&page_index=1&ann_type=A&f_node=0&s_node=0&stock_list={code.split('.')[0]}")
    try:
        items = (json.loads(_get(url)).get("data") or {}).get("list") or []
    except Exception as exc:  # noqa: BLE001
        log.info("em anns %s: %s", code, str(exc)[:120])
        return []
    out = [{"date": str(i.get("notice_date"))[:10], "title": i.get("title")}
           for i in items if str(i.get("notice_date"))[:10] >= f"{since:%Y-%m-%d}"]
    return out[:n]


# --------------------------------------------------------------------------- llm
def deepseek() -> tuple[Callable[[list[dict[str, str]]], str], str] | None:
    from rquant.research.surge import _env_file_values

    vals = _env_file_values(ENV_FILE, ("DEEPSEEK_API_KEY", "DEEPSEEK_BASE_URL",
                                       "DEEPSEEK_MODEL"))
    key = vals.get("DEEPSEEK_API_KEY")
    if not key:
        return None
    base = (vals.get("DEEPSEEK_BASE_URL") or "https://api.deepseek.com").rstrip("/")
    model = vals.get("DEEPSEEK_MODEL") or "deepseek-chat"

    def complete(messages: list[dict[str, str]]) -> str:
        body = json.dumps({"model": model, "messages": messages, "temperature": 0.2,
                           "max_tokens": 4000}).encode()
        req = urllib.request.Request(f"{base}/chat/completions", body, {
            "Content-Type": "application/json", "Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310
            msg = json.loads(r.read())["choices"][0]["message"]
        text = (msg.get("content") or "").strip()
        if not text:
            log.warning("deepseek: empty content (reasoning used the token budget?)")
        return text

    return complete, model


def common_concepts(concepts: dict[str, list[str]], codes: list[str]) -> list[str]:
    cnt = Counter(c for code in codes for c in set(concepts.get(code) or []))
    need = 2 if len(codes) > 1 else 1
    return [c for c, k in cnt.most_common(8) if k >= need]


def build_prompt(pool: dict, stocks: list[dict]) -> str:
    lines = [f"日期 {pool['date']}，规则：{pool.get('rule', '')}"]
    for s in stocks:
        tag = "终选" if s["is_pick"] else "同花顺榜"
        lines.append(f"\n[{tag}] {s['name']}({s['ts_code']}) 竞价涨幅{s['gap']:.2%}"
                     f" 行业:{s.get('industry') or '-'}")
        lines.append("  概念: " + "、".join((s.get("concepts") or [])[:12]))
        for x in s.get("news") or []:
            lines.append(f"  新闻 {x.get('time', '')} {x.get('title', '')} {x.get('snippet', '')}")
        for x in s.get("anns") or []:
            lines.append(f"  公告 {x['date']} {x['title']}")
    if pool.get("_market_news"):
        lines.append("\n今晨要闻：" + "；".join(pool["_market_news"][:15]))
    return "\n".join(lines)[:6000]


def attribute(pool: dict, now: datetime | None = None,
              llm: tuple[Callable, str] | None | bool = True) -> dict[str, Any]:
    now = now or datetime.now(TZ)
    picks = pool.get("picks") or []
    ths = pool.get("ths_screen") or []
    seen: dict[str, dict] = {}
    for r in picks:
        seen[r["ts_code"]] = {**r, "is_pick": True}
    for r in ths:
        if len(seen) >= MAX_TARGETS:
            break
        seen.setdefault(r["ts_code"], {**r, "is_pick": False})
    stocks = list(seen.values())
    if not stocks:
        return {"text": "今日无竞价选股结果", "stocks": [], "at": _now()}
    prior = date.fromisoformat(str(pool.get("prior_date") or pool["date"])[:10])
    since = datetime(prior.year, prior.month, prior.day, 15, 0, tzinfo=TZ)
    concepts = ths_concepts([s["ts_code"] for s in stocks])
    mnews = major_news(since, now)
    for s in stocks:
        s["concepts"] = concepts.get(s["ts_code"], [])
        nm = s.get("name") or ""
        hits = [{"time": m["pub_time"], "title": m["title"], "src": m["src"]}
                for m in mnews if nm and nm in str(m["title"])]
        s["news"] = (hits + em_news(nm, since, now))[:4] if nm else hits
        s["anns"] = em_anns(s["ts_code"], prior - timedelta(days=3))
    pick_codes = [s["ts_code"] for s in stocks if s["is_pick"]] or [s["ts_code"] for s in stocks]
    common = common_concepts(concepts, pick_codes)
    pool = {**pool, "_market_news": [m["title"] for m in mnews[:15]]}
    text, model = "", None
    client = deepseek() if llm is True else (llm or None)
    if client:
        complete, model = client
        try:
            text = complete([{"role": "system", "content": SYSTEM},
                             {"role": "user", "content": build_prompt(pool, stocks)}])
        except Exception as exc:  # noqa: BLE001
            log.warning("deepseek failed: %s", str(exc)[:160])
    if not text:
        text = ("共同概念：" + "、".join(common[:4])) if common else "暂无明确共同驱动"
        model = None
    keep = ("ts_code", "name", "is_pick", "gap", "concepts", "news", "anns")
    return {"text": text, "model": model, "common_concepts": common,
            "stocks": [{k: s.get(k) for k in keep} for s in stocks], "at": _now()}


# --------------------------------------------------------------------------- push
def format_push(pool: dict) -> tuple[str, str]:
    picks = pool.get("picks") or []
    att = pool.get("attribution") or {}
    title = f"竞价选股 {pool['date'][5:]}：" + ("/".join(p["name"] for p in picks) or "无")
    lines = [f"**归因**：{att.get('text', '-')}", ""]
    for p in picks:
        lines.append(f"{p['rank']}. {p['name']} {p['ts_code']} 竞价 {p['gap']:+.2%} "
                     f"量 {p['auction_vol'] / 1e4:.0f}万股")
    ths = [t for t in pool.get("ths_screen") or []]
    if ths:
        lines += ["", "同花顺榜(>4%,含一字)：" + "、".join(
            f"{t['name']}{'(一字)' if t.get('limit_open') else ''}" for t in ths)]
    if att.get("common_concepts"):
        lines.append("共同概念：" + "、".join(att["common_concepts"][:6]))
    return title, "\n".join(lines)


def push(title: str, body: str) -> int:
    from rquant.notify.client import PushDeerClient
    from rquant.research.surge import _env_file_values

    vals = _env_file_values(ENV_FILE, ("PUSHDEER_KEYS", "PUSHDEER_ENDPOINT"))
    keys = [k.strip() for k in vals.get("PUSHDEER_KEYS", "").split(",") if k.strip()]
    if not keys:
        log.info("no PUSHDEER_KEYS; push skipped")
        return 0
    res = PushDeerClient(keys, vals.get("PUSHDEER_ENDPOINT")
                         or "https://api2.pushdeer.com/message/push").push(title, body)
    return sum(ok for ok, _ in res)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(prog="python -m rquant.research.auction_attrib")
    p.add_argument("--day", type=date.fromisoformat, default=None)
    p.add_argument("--no-push", action="store_true")
    p.add_argument("--force-push", action="store_true")
    a = p.parse_args(argv)
    root, day = research_root(), a.day or datetime.now(TZ).date()
    pool = read_pool(root, day)
    if not pool or pool.get("stage") != "final":
        log.info("%s: no final pool; skip", day)
        return 0
    pool["attribution"] = attribute(pool)
    _write(root, day, pool)
    log.info("attribution %s: %s", day, pool["attribution"]["text"])
    # 2026-10-14: the pool is a watchlist now, not a buy signal -> its push is paused
    # unless AUCTION_POOL_PUSH=1 (or --force-push).
    from rquant.research.promotion_gate import gate_passed

    paused = not a.force_push and (os.environ.get("AUCTION_POOL_PUSH", "0") != "1"
                                   or not gate_passed(root, "auction-ths"))
    if paused:
        log.info("auction-pick push paused (watchlist; needs AUCTION_POOL_PUSH=1 + gate)")
    if not a.no_push and not paused and (a.force_push or not pool.get("pushed_at")):
        title, body = format_push(pool)
        if push(title, body):
            pool["pushed_at"] = _now()
            _write(root, day, pool)
    return 0


if __name__ == "__main__":
    sys.exit(main())
