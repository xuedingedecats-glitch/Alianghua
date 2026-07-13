#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股量化推荐网站：展示全市场扫描、战法分类、每日推荐与历史报告。"""
from __future__ import annotations

import argparse, csv, datetime as dt, hmac, html, json, os, re, secrets, subprocess, sys, threading, time, traceback
from concurrent import futures

import requests
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, unquote, urlparse

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("QUANT_WEB_DATA_DIR", str(BASE_DIR))).expanduser().resolve()
ENGINE = BASE_DIR / "a_share_daily.py"
REPORT_DIR = DATA_DIR / "a_share_daily_reports"
LOG_DIR = DATA_DIR / "logs"
REPORT_DIR.mkdir(parents=True, exist_ok=True); LOG_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_SCHEDULE = os.environ.get("QUANT_WEB_SCHEDULE", "15:35,21:00")
OPENING_SCHEDULE = os.environ.get("QUANT_WEB_OPENING_SCHEDULE", "09:35,09:50,10:15,10:30")
OPENING_CHECK_LIMIT = int(os.environ.get("QUANT_WEB_OPENING_CHECK_LIMIT", "40"))
OPENING_CACHE_TTL = int(os.environ.get("QUANT_WEB_OPENING_CACHE_TTL", "90"))
OPENING_HISTORY_RETENTION_DAYS = 3
OPENING_SNAPSHOT_MAX_BYTES = 2 * 1024 * 1024
MAX_OPENING_DETAIL_ROWS = 100
DEFAULT_TOP = int(os.environ.get("QUANT_WEB_TOP", "80"))
DEFAULT_MAX_STOCKS = int(os.environ.get("QUANT_WEB_MAX_STOCKS", "1200"))
DEFAULT_FULL = os.environ.get("QUANT_WEB_FULL", "1").lower() not in ("0","false","no")
DEFAULT_WORKERS = int(os.environ.get("QUANT_WEB_WORKERS", "12"))
WEB_TOKEN = os.environ.get("QUANT_WEB_TOKEN", "").strip()
FUNDAMENTAL_CACHE_TTL = int(os.environ.get("QUANT_WEB_FUNDAMENTAL_CACHE_TTL", "900"))
FUNDAMENTAL_LOCK = threading.Lock()
FUNDAMENTAL_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
KLINE_CACHE_TTL = int(os.environ.get("QUANT_WEB_KLINE_CACHE_TTL", "300"))
KLINE_ACTIVE_CACHE_TTL = int(os.environ.get("QUANT_WEB_KLINE_ACTIVE_CACHE_TTL", "20"))
KLINE_CACHE_LOCK = threading.Lock()
KLINE_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
OPENING_LOCK = threading.Lock()
OPENING_REFRESH_LOCK = threading.Lock()
OPENING_CACHE: Dict[str, Any] = {"at": 0.0, "source": "", "payload": {}}
WATCHLIST_FILE = DATA_DIR / "opening_watchlist.json"
WATCHLIST_LOCK = threading.Lock()
MAX_WATCHLIST = int(os.environ.get("QUANT_WEB_MAX_WATCHLIST", "100"))
TRADE_JOURNAL_FILE = DATA_DIR / "trade_journal.json"
TRADE_JOURNAL_LOCK = threading.Lock()
MAX_TRADE_RECORDS = max(10, min(int(os.environ.get("QUANT_WEB_MAX_TRADE_RECORDS", "500")), 5000))
MAX_TRADE_FILE_BYTES = 2 * 1024 * 1024
AUTO_SYNC_DEFAULT_LIMIT = int(os.environ.get("QUANT_WEB_AUTO_SYNC_LIMIT", "12"))
AUTO_SYNC_MIN_SCORE = max(72.0, min(float(os.environ.get("QUANT_WEB_AUTO_SYNC_MIN_SCORE", "72")), 99.0))
AUTO_SYNC_MAX_FAILURE_RATE = max(0.0, min(float(os.environ.get("QUANT_WEB_AUTO_SYNC_MAX_FAILURE_RATE", "0.10")), 1.0))
MAX_CONCURRENT_REQUESTS = max(8, min(int(os.environ.get("QUANT_WEB_MAX_CONCURRENT_REQUESTS", "64")), 256))

RUN_LOCK = threading.Lock()
RUN_STATE: Dict[str, Any] = {"running": False, "last_started": None, "last_finished": None, "last_returncode": None, "last_log": None, "last_error": None}
OPENING_RUN_STATE: Dict[str, Any] = {"running": False, "last_started": None, "last_finished": None, "last_error": None, "last_file": None}
RATE_LOCK = threading.Lock()
RATE_BUCKETS: Dict[str, List[float]] = {}
GROUP_ORDER = ["全部", "趋势动量类", "突破类", "回踩低吸类", "K线形态类", "涨停情绪类", "超跌反转类", "综合类"]
STRATEGY_BOOK = [
    {"group":"趋势动量类","items":["RPS强势新高", "均线多头动量", "MACD零轴二次金叉"], "rule":"相对强度排名靠前、20/60/120日趋势向上，优先做强者恒强与趋势延续。", "risk":"忌追长阳，偏离20日线过大或RSI过热降级。"},
    {"group":"突破类","items":["放量平台突破", "唐奇安60日突破", "VCP波动收缩突破"], "rule":"箱体/通道上沿被有效突破，并有量能或波动收缩后的扩张确认。", "risk":"突破后跌回平台立即放弃；高开过多不追。"},
    {"group":"回踩低吸类","items":["趋势回踩20日线", "突破回踩前高", "涨停后缩量回踩", "强势股首阴低吸"], "rule":"只在原趋势未坏、缩量回踩关键均线/前高时等待分时转强。", "risk":"低吸不是抄底，放量破位或板块走弱就退出。"},
    {"group":"K线形态类","items":["一剑封喉", "仙人指路", "老鸭头启动", "红三兵趋势续涨", "反包阳线修复"], "rule":"用实体、影线、连续K线及均线位置确认走势变化；形态本身不等于买点。", "risk":"必须结合趋势、量能与关键价位确认，避免把末端加速或假突破当机会。"},
    {"group":"涨停情绪类","items":["首板强势观察", "涨停回马枪"], "rule":"只记录情绪强度和次日观察条件，不把涨停当天当作确定买点。", "risk":"风险最高，必须结合竞价承接、板块持续性人工确认。"},
    {"group":"超跌反转类","items":["超跌企稳反弹"], "rule":"大幅回撤后触及布林下轨并站回短均线，作为快进快出的修复型机会。", "risk":"弱势股反弹持续性差，评分权重低于趋势类。"},
]

def now_cn() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).astimezone(dt.timezone(dt.timedelta(hours=8))).replace(tzinfo=None)

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x in (None, "", "-", "nan"):
            return default
        return float(x)
    except Exception:
        return default

def consume_rate(key: str, limit: int, window_seconds: int) -> Tuple[bool, int]:
    now = time.monotonic()
    with RATE_LOCK:
        recent = [stamp for stamp in RATE_BUCKETS.get(key, []) if now - stamp < window_seconds]
        if len(recent) >= limit:
            RATE_BUCKETS[key] = recent
            retry = max(1, int(window_seconds - (now - recent[0])))
            return False, retry
        recent.append(now); RATE_BUCKETS[key] = recent
        # Bound memory if arbitrary client addresses hit the service over a long period.
        if len(RATE_BUCKETS) > 4096:
            stale = [name for name, stamps in RATE_BUCKETS.items() if not stamps or now - stamps[-1] > window_seconds]
            for name in stale: RATE_BUCKETS.pop(name, None)
            if len(RATE_BUCKETS) > 4096:
                oldest = sorted(RATE_BUCKETS, key=lambda name: RATE_BUCKETS[name][-1] if RATE_BUCKETS[name] else -1)
                for name in oldest[:len(RATE_BUCKETS) - 4096]: RATE_BUCKETS.pop(name, None)
        return True, 0

def script_json(value: Any) -> str:
    """Serialize JSON for an inline <script> without allowing </script> or Unicode separators to break context."""
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            .replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")
            .replace("\u2028", "\\u2028").replace("\u2029", "\\u2029"))

def public_run_state(state: Dict[str, Any], lock: Optional[threading.Lock] = None) -> Dict[str, Any]:
    if lock is None:
        out = dict(state)
    else:
        with lock:
            out = dict(state)
    for key in ("last_log", "last_file"):
        if out.get(key):
            out[key] = Path(str(out[key])).name
    return out

def read_csv_dict(path: Path) -> List[Dict[str, Any]]:
    if not path.exists(): return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def latest_signal_file() -> Optional[Path]:
    files = sorted(REPORT_DIR.glob("signals_*.csv"), reverse=True)
    return files[0] if files else None

def date_from_signal(p: Path) -> str:
    m = re.search(r"signals_(\d{8})\.csv", p.name)
    return m.group(1) if m else ""

def report_for_date(ymd: str) -> Optional[Path]:
    p = REPORT_DIR / f"report_{ymd}.md"
    return p if p.exists() else None

def meta_for_date(ymd: str) -> Dict[str, Any]:
    p = REPORT_DIR / f"meta_{ymd}.json"
    if not p.exists(): return {}
    try: return json.loads(p.read_text(encoding="utf-8"))
    except Exception: return {}

def strategy_group_name(strategy: str) -> str:
    if any(k in strategy for k in ["RPS", "新高", "多头", "动量", "MACD"]): return "趋势动量类"
    if any(k in strategy for k in ["一剑封喉", "仙人指路", "老鸭头", "红三兵", "反包"]): return "K线形态类"
    if any(k in strategy for k in ["VCP", "平台", "唐奇安", "突破"]): return "突破类"
    if any(k in strategy for k in ["回踩", "低吸", "首阴", "回马枪"]): return "回踩低吸类"
    if any(k in strategy for k in ["首板", "涨停", "龙头"]): return "涨停情绪类"
    if any(k in strategy for k in ["超跌", "反弹", "布林"]): return "超跌反转类"
    return "综合类"

def consolidate_signal_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """按股票代码合并旧版/异常报告中的重复记录，始终保留最高分方案。"""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for raw in rows:
        try:
            code = clean_stock_code(raw.get("code"))
        except ValueError:
            continue
        row = dict(raw); row["code"] = code
        grouped.setdefault(code, []).append(row)
    out: List[Dict[str, Any]] = []
    for code, items in grouped.items():
        items.sort(key=lambda item: safe_float(item.get("score")), reverse=True)
        best = dict(items[0])
        strategies: List[str] = []
        groups: List[str] = []
        for item in items:
            for value in str(item.get("strategy") or "").split("+"):
                value = value.strip()
                if value and value not in strategies: strategies.append(value)
            labels = str(item.get("strategy_group") or strategy_group_name(str(item.get("strategy") or "")))
            for value in labels.split("+"):
                value = value.strip()
                if value and value not in groups: groups.append(value)
        if strategies: best["strategy"] = "+".join(strategies[:5])
        if groups: best["strategy_group"] = "+".join(groups)
        out.append(best)
    return sorted(out, key=lambda item: (-safe_float(item.get("score")), str(item.get("code") or "")))


def level_for(row: Dict[str, Any]) -> Tuple[str, str]:
    score = safe_float(row.get("score")); strategy = str(row.get("strategy", "")); group = row.get("strategy_group") or strategy_group_name(strategy)
    stage = str(row.get("execution_stage") or "")
    if stage == "防守观察": return "防守观察", "risk"
    if "涨停情绪类" in group or any(k in strategy for k in ["首板", "涨停回马枪"]): return ("情绪需确认", "risk") if score >= 68 else ("情绪高风险", "risk")
    if score >= 80: return "核心推荐", "strong"
    if score >= 72: return "优先关注", "good"
    if score >= 64: return "观察池", "watch"
    return "谨慎观察", "risk"

def clean_stock_code(value: Any) -> str:
    """只接受沪深 A 股六位代码，避免把查询接口变成任意外部请求代理。"""
    code = re.sub(r"\D", "", str(value or ""))
    if len(code) != 6 or not code.startswith(("00", "30", "60", "68")):
        raise ValueError("请输入沪深A股六位代码，例如 000938、600000 或 688xxx。")
    return code


def code_market(code: str) -> Tuple[str, str, str]:
    if code.startswith(("60", "68")):
        return "SH", f"1.{code}", f"SH{code}"
    return "SZ", f"0.{code}", f"SZ{code}"


def nullable_float(value: Any, digits: int = 4) -> Optional[float]:
    try:
        if value in (None, "", "-", "--"):
            return None
        value = float(value)
        if value != value:  # NaN
            return None
        return round(value, digits)
    except Exception:
        return None


def latest_fundamental_assessment(finance: Dict[str, Any], industry: str) -> List[Dict[str, str]]:
    """把公开财务字段转换成可解释的结构标签；不输出买卖结论。"""
    flags: List[Dict[str, str]] = []
    revenue_yoy = nullable_float(finance.get("TOTALOPERATEREVETZ"))
    profit_yoy = nullable_float(finance.get("PARENTNETPROFITTZ"))
    roe = nullable_float(finance.get("ROEJQ"))
    debt = nullable_float(finance.get("ZCFZL"))
    cash_ratio = nullable_float(finance.get("JYXJLYYSR"))
    financial = any(x in (industry or "") for x in ("银行", "保险", "证券", "金融"))
    if revenue_yoy is not None:
        flags.append({"type": "up" if revenue_yoy >= 0 else "down", "title": "营收同比", "text": f"{revenue_yoy:+.2f}%（最近披露报告期）"})
    if profit_yoy is not None:
        flags.append({"type": "up" if profit_yoy >= 0 else "down", "title": "归母净利同比", "text": f"{profit_yoy:+.2f}%（最近披露报告期）"})
    if roe is not None:
        flags.append({"type": "neutral", "title": "加权ROE", "text": f"{roe:.2f}%（报告期口径，非年化）"})
    if not financial and debt is not None:
        debt_text = "偏高，需结合行业和现金流" if debt >= 70 else ("中等" if debt >= 50 else "相对可控")
        flags.append({"type": "warn" if debt >= 70 else "neutral", "title": "资产负债率", "text": f"{debt:.2f}%：{debt_text}"})
    if not financial and cash_ratio is not None:
        cash_text = "经营现金流相对营收为正" if cash_ratio >= 0 else "经营现金流相对营收为负，需核对季节性与应收变化"
        flags.append({"type": "up" if cash_ratio >= 0 else "warn", "title": "经营现金流/营收", "text": f"{cash_ratio:.2f}%：{cash_text}"})
    if financial:
        flags.append({"type": "neutral", "title": "行业口径提示", "text": "金融行业不宜用传统毛利率、资产负债率与一般制造业直接比较。"})
    return flags


def fundamental_payload(raw_code: Any) -> Dict[str, Any]:
    """查询东方财富公开行情、公司概况和主要财务指标，并在服务端短时缓存。"""
    code = clean_stock_code(raw_code)
    now = time.time()
    with FUNDAMENTAL_LOCK:
        hit = FUNDAMENTAL_CACHE.get(code)
        if hit and now - hit[0] < FUNDAMENTAL_CACHE_TTL:
            cached = dict(hit[1])
            cached["cached"] = True
            return cached

    market, secid, survey_code = code_market(code)
    headers = {"User-Agent": "Mozilla/5.0 (A-share Quant Dashboard)", "Referer": "https://quote.eastmoney.com/"}
    warnings: List[str] = []
    company: Dict[str, Any] = {}
    finance_rows: List[Dict[str, Any]] = []
    quote: Dict[str, Any] = {}

    try:
        r = requests.get("https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax", params={"code": survey_code}, headers=headers, timeout=12)
        r.raise_for_status()
        company = r.json() if r.text.strip() else {}
    except Exception as e:
        warnings.append("公司概况暂时获取失败")

    secucode = f"{code}.{market}"
    try:
        params = {"reportName": "RPT_F10_FINANCE_MAINFINADATA", "columns": "ALL", "filter": f'(SECUCODE="{secucode}")', "pageNumber": 1, "pageSize": 5, "sortTypes": "-1", "sortColumns": "REPORT_DATE", "source": "HSF10", "client": "PC"}
        r = requests.get("https://datacenter-web.eastmoney.com/api/data/v1/get", params=params, headers=headers, timeout=12)
        r.raise_for_status()
        finance_rows = ((r.json().get("result") or {}).get("data") or [])
    except Exception:
        warnings.append("财务指标暂时获取失败")

    try:
        fields = "f2,f3,f8,f9,f10,f20,f21,f23,f24,f25,f43,f57,f58,f116,f117,f162,f167,f168,f170,f173"
        r = requests.get("https://push2.eastmoney.com/api/qt/stock/get", params={"secid": secid, "fields": fields}, headers=headers, timeout=12)
        r.raise_for_status()
        quote = r.json().get("data") or {}
    except Exception:
        warnings.append("即时估值与行情暂时获取失败")

    jbzl = company.get("jbzl") or {}
    fxxg = company.get("fxxg") or {}
    current = finance_rows[0] if finance_rows else {}
    name = quote.get("f58") or jbzl.get("agjc") or current.get("SECURITY_NAME_ABBR") or code
    # stock/get 和 clist/get 的部分字段缩放口径不同；优先使用未缩放的常规字段。
    raw_price = quote.get("f2")
    price = nullable_float(raw_price, 2)
    if price is None and quote.get("f43") is not None:
        price = nullable_float((nullable_float(quote.get("f43"), 4) or 0) / 100, 2)
    raw_pct = quote.get("f3")
    pct = nullable_float(raw_pct, 2)
    if pct is None and quote.get("f170") is not None:
        pct = nullable_float((nullable_float(quote.get("f170"), 4) or 0) / 100, 2)
    quote_info = {
        "price": price,
        "pct": pct,
        "pe_dynamic": nullable_float(quote.get("f9"), 2) if quote.get("f9") is not None else (nullable_float((nullable_float(quote.get("f162"), 4) or 0) / 100, 2) if quote.get("f162") is not None else None),
        "pb": nullable_float(quote.get("f23"), 2) if quote.get("f23") is not None else nullable_float(quote.get("f173"), 2),
        "turnover": nullable_float(quote.get("f8"), 2),
        "volume_ratio": nullable_float(quote.get("f10"), 2),
        "total_market_cap": nullable_float(quote.get("f20") if quote.get("f20") is not None else quote.get("f116"), 2),
        "float_market_cap": nullable_float(quote.get("f21") if quote.get("f21") is not None else quote.get("f117"), 2),
        "return_60d": nullable_float(quote.get("f24"), 2),
        "return_ytd": nullable_float(quote.get("f25"), 2),
    }
    history = []
    for row in finance_rows:
        history.append({
            "period": row.get("REPORT_DATE_NAME") or str(row.get("REPORT_DATE") or "")[:10],
            "type": row.get("REPORT_TYPE") or "-",
            "notice_date": str(row.get("NOTICE_DATE") or "")[:10],
            "revenue": nullable_float(row.get("TOTALOPERATEREVE"), 2),
            "revenue_yoy": nullable_float(row.get("TOTALOPERATEREVETZ"), 2),
            "net_profit": nullable_float(row.get("PARENTNETPROFIT"), 2),
            "profit_yoy": nullable_float(row.get("PARENTNETPROFITTZ"), 2),
            "roe": nullable_float(row.get("ROEJQ"), 2),
            "gross_margin": nullable_float(row.get("XSMLL"), 2),
            "debt_ratio": nullable_float(row.get("ZCFZL"), 2),
            "operating_cash_to_revenue": nullable_float(row.get("JYXJLYYSR"), 2),
        })
    if not (company or finance_rows or quote):
        raise RuntimeError("公开数据源暂不可用，请稍后重试。")
    industry = jbzl.get("sshy") or current.get("ORG_TYPE") or "-"
    payload = {
        "ok": True,
        "code": code,
        "name": name,
        "market": market,
        "cached": False,
        "fetched_at": now_cn().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "东方财富公开行情、F10公司概况及财务指标接口",
        "warnings": warnings,
        "company": {
            "full_name": jbzl.get("gsmc") or name,
            "industry": industry,
            "market_type": jbzl.get("zqlb") or ("上交所A股" if market == "SH" else "深交所A股"),
            "exchange": jbzl.get("ssjys") or ("上海证券交易所" if market == "SH" else "深圳证券交易所"),
            "listing_date": fxxg.get("ssrq") or "-",
            "province": jbzl.get("qy") or "-",
            "chairman": jbzl.get("dsz") or jbzl.get("zjl") or "-",
            "legal_representative": jbzl.get("frdb") or "-",
            "employees": jbzl.get("gyrs") or "-",
            "business_summary": re.sub(r"\s+", " ", str(jbzl.get("gsjj") or "")).strip()[:420],
        },
        "quote": quote_info,
        "latest_finance": history[0] if history else {},
        "finance_history": history,
        "assessment": latest_fundamental_assessment(current, industry) if current else [],
        "notes": ["估值和行情字段为查询时点公开快照，可能受盘中波动、停牌或数据源延迟影响。", "财报采用最新公开披露报告期；季报、年报口径不可简单横向替代。", "本页面展示事实数据与结构化提示，不构成买卖建议。"],
    }
    with FUNDAMENTAL_LOCK:
        FUNDAMENTAL_CACHE[code] = (now, payload)
        # 防止长时间运行后缓存无限增长。
        if len(FUNDAMENTAL_CACHE) > 120:
            expired = sorted(FUNDAMENTAL_CACHE.items(), key=lambda item: item[1][0])[:30]
            for old_code, _ in expired:
                FUNDAMENTAL_CACHE.pop(old_code, None)
    return payload




def _clean_codes(values: Any) -> List[str]:
    out: List[str] = []
    for value in values if isinstance(values, list) else []:
        try:
            code = clean_stock_code(value)
        except ValueError:
            continue
        if code not in out:
            out.append(code)
    return out[:MAX_WATCHLIST]

def _normal_auto_sync(value: Any) -> Dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    try:
        top_n = int(raw.get("top_n", AUTO_SYNC_DEFAULT_LIMIT))
    except Exception:
        top_n = AUTO_SYNC_DEFAULT_LIMIT
    return {"enabled": bool(raw.get("enabled", False)), "top_n": max(1, min(top_n, min(MAX_WATCHLIST, 40))), "min_score": AUTO_SYNC_MIN_SCORE, "last_synced_at": str(raw.get("last_synced_at") or ""), "last_signal_file": str(raw.get("last_signal_file") or "")}

def _normal_monitor_baselines(value: Any) -> Dict[str, Dict[str, Any]]:
    raw = value if isinstance(value, dict) else {}
    out: Dict[str, Dict[str, Any]] = {}
    for raw_code, item in raw.items():
        try:
            code = clean_stock_code(raw_code)
        except ValueError:
            continue
        item = item if isinstance(item, dict) else {}
        price = nullable_float(item.get("price"), 4)
        if price is not None and price <= 0:
            price = None
        out[code] = {
            "price": price,
            "added_at": str(item.get("added_at") or "")[:32],
            "price_source": str(item.get("price_source") or "")[:80],
            "source": str(item.get("source") or "")[:80],
            "name": str(item.get("name") or "")[:80],
        }
    return out

def _capture_monitor_baseline(code: str, source: str, name: str = "") -> Dict[str, Any]:
    baseline = {"price": None, "added_at": now_cn().strftime("%Y-%m-%d %H:%M:%S"), "price_source": "行情暂不可用", "source": source, "name": name or code}
    try:
        quote_info = fetch_live_quote(code)
    except Exception:
        quote_info = {}
    price = nullable_float((quote_info or {}).get("price"), 4)
    if price is not None and price > 0:
        baseline["price"] = price
        baseline["price_source"] = str((quote_info or {}).get("source") or "公开行情快照")[:80]
        baseline["name"] = str((quote_info or {}).get("name") or name or code)[:80]
    return baseline

def _baseline_from_signal(row: Dict[str, Any]) -> Dict[str, Any]:
    code = str(row.get("code") or "")
    price = nullable_float(row.get("close"), 4)
    return {
        "price": price if price is not None and price > 0 else None,
        "added_at": now_cn().strftime("%Y-%m-%d %H:%M:%S"),
        "price_source": "收盘扫描收盘价" if price is not None and price > 0 else "收盘扫描未提供收盘价",
        "source": "自动同步收盘推荐",
        "name": str(row.get("name") or code)[:80],
    }

def _read_watch_state_unlocked() -> Dict[str, Any]:
    try:
        raw = json.loads(WATCHLIST_FILE.read_text(encoding="utf-8")) if WATCHLIST_FILE.exists() else {}
    except Exception:
        raw = {}
    raw = raw if isinstance(raw, dict) else {}
    # 兼容旧版 {"codes": [...]}：原有勾选项全部视为手动清单，绝不会被自动同步覆盖。
    manual = _clean_codes(raw.get("manual_codes") if isinstance(raw.get("manual_codes"), list) else raw.get("codes", []))
    auto = [code for code in _clean_codes(raw.get("auto_codes", [])) if code not in manual]
    excluded = [code for code in _clean_codes(raw.get("excluded_auto_codes", [])) if code in auto and code not in manual]
    active = set(manual) | set(auto)
    baselines = {code: item for code, item in _normal_monitor_baselines(raw.get("monitor_baselines")).items() if code in active}
    return {"manual_codes": manual, "auto_codes": auto, "excluded_auto_codes": excluded, "auto_sync": _normal_auto_sync(raw.get("auto_sync")), "monitor_baselines": baselines}

def _effective_watch_codes(state: Dict[str, Any]) -> List[str]:
    manual = list(state.get("manual_codes") or [])
    excluded = set(state.get("excluded_auto_codes") or [])
    auto = [code for code in (state.get("auto_codes") or []) if code not in excluded and code not in manual]
    return (manual + auto)[:MAX_WATCHLIST]

def _write_watch_state_unlocked(state: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"updated_at": now_cn().strftime("%Y-%m-%d %H:%M:%S"), "manual_codes": _clean_codes(state.get("manual_codes", [])), "auto_codes": _clean_codes(state.get("auto_codes", [])), "excluded_auto_codes": _clean_codes(state.get("excluded_auto_codes", [])), "auto_sync": _normal_auto_sync(state.get("auto_sync"))}
    # 保留 codes 方便旧版本/人工查看，同时新版本以 manual_codes + auto_codes 管理来源。
    payload["codes"] = _effective_watch_codes(payload)
    raw_baselines = _normal_monitor_baselines(state.get("monitor_baselines"))
    payload["monitor_baselines"] = {code: raw_baselines[code] for code in payload["codes"] if code in raw_baselines}
    temp = WATCHLIST_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(WATCHLIST_FILE)
    return payload

def _invalidate_opening_cache() -> None:
    with OPENING_LOCK:
        OPENING_CACHE.update({"at": 0.0, "source": "", "payload": {}})

def load_watch_state() -> Dict[str, Any]:
    with WATCHLIST_LOCK:
        return _read_watch_state_unlocked()

def load_watch_codes() -> List[str]:
    return _effective_watch_codes(load_watch_state())

def update_watch_codes(mode: str, codes: List[Any]) -> Dict[str, Any]:
    incoming = _clean_codes(codes)
    baseline_needed: List[str] = []
    source = "手动添加监控" if mode == "add" else "手动替换监控"
    with WATCHLIST_LOCK:
        state = _read_watch_state_unlocked()
        before = set(_effective_watch_codes(state))
        manual = list(state["manual_codes"]); auto = set(state["auto_codes"]); excluded = set(state["excluded_auto_codes"])
        if mode == "replace":
            state["manual_codes"] = incoming
            # "清空/替换" 应即时只保留用户选择；下一交易日自动同步会生成新候选。
            state["excluded_auto_codes"] = [code for code in state["auto_codes"] if code not in incoming]
            baseline_needed = [code for code in incoming if code not in before]
        elif mode == "add":
            state["manual_codes"] = (manual + [code for code in incoming if code not in manual])[:MAX_WATCHLIST]
            state["excluded_auto_codes"] = [code for code in excluded if code not in incoming]
            baseline_needed = [code for code in incoming if code not in before]
        elif mode == "remove":
            state["manual_codes"] = [code for code in manual if code not in incoming]
            state["excluded_auto_codes"] = list(excluded | (auto & set(incoming)))
        else:
            raise ValueError("不支持的清单更新方式")
        _write_watch_state_unlocked(state)
    # 仅对本次新加入的代码记录当时公开行情，不会为旧清单伪造历史加入价。
    captured = {code: _capture_monitor_baseline(code, source) for code in baseline_needed}
    if captured:
        with WATCHLIST_LOCK:
            state = _read_watch_state_unlocked()
            active = set(_effective_watch_codes(state))
            baselines = _normal_monitor_baselines(state.get("monitor_baselines"))
            for code, baseline in captured.items():
                if code in active and code not in baselines:
                    baselines[code] = baseline
            state["monitor_baselines"] = baselines
            _write_watch_state_unlocked(state)
    _invalidate_opening_cache()
    return watchlist_payload()

def set_auto_sync(value: Any) -> Dict[str, Any]:
    raw = value if isinstance(value, dict) else {"enabled": value}
    with WATCHLIST_LOCK:
        state = _read_watch_state_unlocked(); current = _normal_auto_sync(state.get("auto_sync"))
        current["enabled"] = bool(raw.get("enabled", current["enabled"]))
        if "top_n" in raw:
            try: current["top_n"] = max(1, min(int(raw["top_n"]), min(MAX_WATCHLIST, 40)))
            except Exception: pass
        state["auto_sync"] = current
        _write_watch_state_unlocked(state)
    if current["enabled"]:
        sync_auto_watchlist()
    _invalidate_opening_cache()
    return watchlist_payload()

def signal_data_quality(signal: Optional[Path], require_today: bool = True) -> Dict[str, Any]:
    """验证自动监控使用的数据是否完整、新鲜且日期一致；不合格时宁可保留旧清单。"""
    if not signal:
        return {"ok": False, "reason": "尚无收盘扫描报告"}
    ymd = date_from_signal(signal)
    meta = meta_for_date(ymd)
    universe = max(0, int(safe_float(meta.get("universe_count"))))
    scanned = max(0, int(safe_float(meta.get("kline_scanned_count"))))
    failed = max(0, int(safe_float(meta.get("failed_count", len(meta.get("errors", []))))))
    failure_rate = failed / max(1, universe)
    trade_date = str(meta.get("latest_trade_date") or "").replace("-", "")
    reasons: List[str] = []
    if not meta:
        reasons.append("缺少扫描元数据")
    if not trade_date or trade_date != ymd:
        reasons.append("指数交易日与信号文件日期不一致")
    current = now_cn()
    if require_today and current.weekday() >= 5:
        reasons.append("今天是周末，禁止自动更新次日监控池")
    if require_today and trade_date != current.strftime("%Y%m%d"):
        reasons.append("最新行情不是今天的交易日数据")
    if universe <= 0 or scanned <= 0:
        reasons.append("没有有效的K线深筛样本")
    if failure_rate > AUTO_SYNC_MAX_FAILURE_RATE:
        reasons.append(f"K线失败率 {failure_rate:.1%} 超过阈值 {AUTO_SYNC_MAX_FAILURE_RATE:.0%}")
    row_dates = {str(r.get("date") or "").replace("-", "") for r in read_csv_dict(signal) if r.get("date")}
    if row_dates and row_dates != {ymd}:
        reasons.append("候选股票行情日期不一致")
    return {"ok": not reasons, "reason": "；".join(reasons) if reasons else "数据质量检查通过",
            "signal_file": signal.name, "trade_date": trade_date, "universe_count": universe,
            "kline_scanned_count": scanned, "failed_count": failed, "failure_rate": round(failure_rate, 4)}

def sync_auto_watchlist() -> Dict[str, Any]:
    """把最新收盘扫描中的高分、非情绪候选写入自动层；不会覆盖手动清单。"""
    signal = latest_signal_file()
    quality = signal_data_quality(signal, require_today=True)
    if not quality["ok"]:
        return {"synced": False, "reason": quality["reason"], "quality": quality}
    assert signal is not None
    ranked: List[Dict[str, Any]] = []
    for row in consolidate_signal_rows(read_csv_dict(signal)):
        group = str(row.get("strategy_group") or strategy_group_name(str(row.get("strategy", ""))))
        risk = str(row.get("risk_tags") or "")
        low, high = parse_price_range(row.get("buy_zone"))
        stop = nullable_float(row.get("stop_loss"), 3)
        # 自动池只保留可执行性更高的非情绪候选；边缘分、过热、高波动、无买区/止损均留给人工观察。
        if ("涨停情绪类" in group or safe_float(row.get("score")) < AUTO_SYNC_MIN_SCORE
                or str(row.get("execution_stage") or "") == "防守观察"
                or any(tag in risk for tag in ("波动偏大", "高波动", "换手过热", "RSI过热", "高风险"))
                or low is None or high is None or stop is None or stop <= 0):
            continue
        ranked.append(row)
    with WATCHLIST_LOCK:
        state = _read_watch_state_unlocked(); cfg = _normal_auto_sync(state.get("auto_sync"))
        if not cfg["enabled"]:
            return {"synced": False, "reason": "自动同步未开启"}
        # 先限制同类集中度，再按总分补足，避免监控池被单一战法/板块风格占满。
        selected_rows: List[Dict[str, Any]] = []; group_counts: Dict[str, int] = {}
        for row in ranked:
            primary = str(row.get("strategy_group") or "综合类").split("+")[0]
            if group_counts.get(primary, 0) >= 3:
                continue
            selected_rows.append(row); group_counts[primary] = group_counts.get(primary, 0) + 1
            if len(selected_rows) >= cfg["top_n"]:
                break
        if len(selected_rows) < cfg["top_n"]:
            chosen = {str(row.get("code")) for row in selected_rows}
            for row in ranked:
                if str(row.get("code")) not in chosen:
                    selected_rows.append(row); chosen.add(str(row.get("code")))
                if len(selected_rows) >= cfg["top_n"]:
                    break
        selected = [str(row["code"]) for row in selected_rows]
        previous_signal = str(cfg.get("last_signal_file") or "")
        state["auto_codes"] = selected
        # 新的收盘信号到来后，重新给用户展示新的自动候选；手动股票始终保留。
        state["excluded_auto_codes"] = []
        if previous_signal != signal.name:
            baselines = _normal_monitor_baselines(state.get("monitor_baselines"))
            manual = set(state.get("manual_codes") or [])
            for row in selected_rows:
                code = str(row["code"])
                if code not in manual:
                    baselines[code] = _baseline_from_signal(row)
            state["monitor_baselines"] = baselines
        cfg["last_synced_at"] = now_cn().strftime("%Y-%m-%d %H:%M:%S"); cfg["last_signal_file"] = signal.name; state["auto_sync"] = cfg
        _write_watch_state_unlocked(state)
    _invalidate_opening_cache()
    return {"synced": True, "count": len(selected), "signal_file": signal.name, "quality": quality}

def watchlist_payload() -> Dict[str, Any]:
    state = load_watch_state(); codes = _effective_watch_codes(state); manual = set(state.get("manual_codes") or []); auto = set(state.get("auto_codes") or [])
    baselines = _normal_monitor_baselines(state.get("monitor_baselines"))
    rows = consolidate_signal_rows(read_csv_dict(latest_signal_file())) if latest_signal_file() else []
    mapped: Dict[str, Dict[str, Any]] = {str(row["code"]): row for row in rows}
    items = []
    for code in codes:
        source = "手动添加" if code in manual else ("自动同步的收盘推荐" if code in auto else "自定义代码")
        items.append({"code": code, "name": (mapped.get(code) or {}).get("name") or (baselines.get(code) or {}).get("name") or code, "source": source, "strategy": (mapped.get(code) or {}).get("strategy") or "自定义保守趋势确认", "baseline": baselines.get(code)})
    return {"codes": codes, "count": len(codes), "max": MAX_WATCHLIST, "manual_count": len(manual), "auto_count": len([code for code in codes if code in auto and code not in manual]), "auto_sync": state.get("auto_sync") or _normal_auto_sync({}), "baseline_by_code": {code: baselines[code] for code in codes if code in baselines}, "items": items}

def opening_session() -> Dict[str, Any]:
    """返回中国交易时段状态；交易所休市日由实时行情可用性作二次保护。"""
    n = now_cn()
    clock = n.strftime("%H:%M")
    if n.weekday() >= 5:
        return {"active": False, "mode": "closed", "label": "非交易日", "time": n.strftime("%Y-%m-%d %H:%M:%S"), "next_action": "下一个交易日开盘后自动核验"}
    if clock < "09:30":
        return {"active": False, "mode": "preopen", "label": "待开盘", "time": n.strftime("%Y-%m-%d %H:%M:%S"), "next_action": "09:35 开始第一次核验"}
    if "09:30" <= clock < "09:35":
        return {"active": False, "mode": "auction", "label": "集合竞价观察", "time": n.strftime("%Y-%m-%d %H:%M:%S"), "next_action": "09:35 后再生成开盘确认结论"}
    if "09:35" <= clock < "11:30":
        return {"active": True, "mode": "morning", "label": "早盘核验窗口", "time": n.strftime("%Y-%m-%d %H:%M:%S"), "next_action": "按价格、承接和风险条件核验"}
    if "11:30" <= clock < "13:00":
        return {"active": False, "mode": "noon", "label": "早盘窗口已结束", "time": n.strftime("%Y-%m-%d %H:%M:%S"), "next_action": "只做早盘结果复盘，不再生成新的建仓结论"}
    if "13:00" <= clock < "15:00":
        return {"active": False, "mode": "afternoon", "label": "下午交易中", "time": n.strftime("%Y-%m-%d %H:%M:%S"), "next_action": "开盘策略已过时效，等待下一交易日"}
    return {"active": False, "mode": "after", "label": "已收盘", "time": n.strftime("%Y-%m-%d %H:%M:%S"), "next_action": "等待下一交易日09:35后的核验"}


def parse_price_range(value: Any) -> Tuple[Optional[float], Optional[float]]:
    nums = re.findall(r"\d+(?:\.\d+)?", str(value or ""))
    if len(nums) < 2:
        return None, None
    low, high = float(nums[0]), float(nums[1])
    return (low, high) if low <= high else (high, low)


def _quote_time(value: Any) -> Tuple[Optional[str], Optional[str], bool]:
    """标准化行情时间，并判断是否为中国当前自然日，防止休市日误用旧快照。"""
    trade_dt: Optional[dt.datetime] = None
    try:
        if isinstance(value, (int, float)) or str(value or "").isdigit() and len(str(value)) <= 10:
            stamp = int(float(value))
            if stamp > 0:
                trade_dt = dt.datetime.fromtimestamp(stamp, dt.timezone.utc).astimezone(dt.timezone(dt.timedelta(hours=8))).replace(tzinfo=None)
        else:
            digits = re.sub(r"\D", "", str(value or ""))
            if len(digits) >= 14:
                trade_dt = dt.datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
            elif len(digits) == 8:
                trade_dt = dt.datetime.strptime(digits, "%Y%m%d")
    except Exception:
        trade_dt = None
    trade_time = trade_dt.strftime("%Y-%m-%d %H:%M:%S") if trade_dt else None
    trade_date = trade_dt.date().isoformat() if trade_dt else None
    stale = trade_date != now_cn().date().isoformat()
    return trade_time, trade_date, stale


def _eastmoney_live_quote(code: str) -> Dict[str, Any]:
    _, secid, _ = code_market(code)
    headers = {"User-Agent": "Mozilla/5.0 (A-share Quant Dashboard)", "Referer": "https://quote.eastmoney.com/"}
    fields = "f2,f3,f6,f8,f10,f15,f16,f17,f18,f57,f58,f124"
    r = requests.get("https://push2.eastmoney.com/api/qt/stock/get", params={"secid": secid, "fields": fields}, headers=headers, timeout=8)
    r.raise_for_status()
    raw = r.json().get("data") or {}
    price = nullable_float(raw.get("f2"), 2); open_price = nullable_float(raw.get("f17"), 2); previous_close = nullable_float(raw.get("f18"), 2)
    trade_time, trade_date, stale = _quote_time(raw.get("f124"))
    gap_pct = round((open_price / previous_close - 1) * 100, 2) if open_price and previous_close else None
    return {"ok": bool(price and price > 0 and not stale), "stale": stale, "source": "东方财富", "code": code, "name": raw.get("f58") or code,
            "price": price, "pct": nullable_float(raw.get("f3"), 2), "open": open_price, "high": nullable_float(raw.get("f15"), 2),
            "low": nullable_float(raw.get("f16"), 2), "previous_close": previous_close, "gap_pct": gap_pct,
            "turnover": nullable_float(raw.get("f8"), 2), "volume_ratio": nullable_float(raw.get("f10"), 2),
            "amount": nullable_float(raw.get("f6"), 2), "trade_time": trade_time, "trade_date": trade_date,
            "fetched_at": now_cn().strftime("%Y-%m-%d %H:%M:%S")}


def _tencent_live_quote(code: str) -> Dict[str, Any]:
    market, _, _ = code_market(code); symbol = market.lower() + code
    r = requests.get(f"https://qt.gtimg.cn/q={symbol}", headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"}, timeout=8)
    r.raise_for_status()
    text = r.content.decode("gbk", errors="ignore")
    match = re.search(r'="(.*)"', text)
    fields = (match.group(1) if match else "").split("~")
    if len(fields) < 35:
        raise ValueError("腾讯行情字段不完整")
    price = nullable_float(fields[3], 2); previous_close = nullable_float(fields[4], 2); open_price = nullable_float(fields[5], 2)
    trade_time, trade_date, stale = _quote_time(fields[30] if len(fields) > 30 else None)
    gap_pct = round((open_price / previous_close - 1) * 100, 2) if open_price and previous_close else None
    return {"ok": bool(price and price > 0 and not stale), "stale": stale, "source": "腾讯行情", "code": code, "name": fields[1] or code,
            "price": price, "pct": nullable_float(fields[32] if len(fields) > 32 else None, 2), "open": open_price,
            "high": nullable_float(fields[33] if len(fields) > 33 else None, 2), "low": nullable_float(fields[34] if len(fields) > 34 else None, 2),
            "previous_close": previous_close, "gap_pct": gap_pct, "turnover": nullable_float(fields[38] if len(fields) > 38 else None, 2),
            "volume_ratio": nullable_float(fields[49] if len(fields) > 49 else None, 2), "amount": nullable_float(fields[37] if len(fields) > 37 else None, 2),
            "trade_time": trade_time, "trade_date": trade_date, "fetched_at": now_cn().strftime("%Y-%m-%d %H:%M:%S")}


def fetch_live_quote(code: str) -> Dict[str, Any]:
    """实时行情双源容灾；只有行情日期为今天才允许进入建仓判断。"""
    code = clean_stock_code(code); errors: List[str] = []
    for fetcher, attempts in ((_eastmoney_live_quote, 2), (_tencent_live_quote, 1)):
        for attempt in range(attempts):
            try:
                return fetcher(code)
            except Exception as exc:
                errors.append(f"{fetcher.__name__}:{type(exc).__name__}")
                if attempt + 1 < attempts:
                    time.sleep(0.25)
    raise RuntimeError("实时行情双数据源均不可用：" + ", ".join(errors))


def _eastmoney_daily_kline(code: str) -> List[Dict[str, Any]]:
    _, secid, _ = code_market(code)
    params={"secid":secid,"fields1":"f1,f2,f3,f4,f5,f6","fields2":"f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61","klt":101,"fqt":1,"beg":(now_cn().date()-dt.timedelta(days=260)).strftime("%Y%m%d"),"end":(now_cn().date()+dt.timedelta(days=2)).strftime("%Y%m%d")}
    r=requests.get("https://push2his.eastmoney.com/api/qt/stock/kline/get",params=params,headers={"User-Agent":"Mozilla/5.0","Referer":"https://quote.eastmoney.com/"},timeout=10); r.raise_for_status()
    out=[]
    for raw in ((r.json().get("data") or {}).get("klines") or []):
        f=str(raw).split(",")
        try: out.append({"date":f[0],"open":float(f[1]),"close":float(f[2]),"high":float(f[3]),"low":float(f[4]),"volume":float(f[5])})
        except (ValueError,IndexError): pass
    return out


def _tencent_daily_kline(code: str, days: int) -> List[Dict[str, Any]]:
    market, _, _ = code_market(code); symbol = market.lower() + code
    r=requests.get("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",params={"param":f"{symbol},day,,,{max(days,100)},qfq"},headers={"User-Agent":"Mozilla/5.0","Referer":"https://gu.qq.com/"},timeout=10); r.raise_for_status()
    node=((r.json().get("data") or {}).get(symbol) or {}); raw_rows=node.get("qfqday") or node.get("day") or []
    out=[]
    for f in raw_rows:
        try: out.append({"date":str(f[0]),"open":float(f[1]),"close":float(f[2]),"high":float(f[3]),"low":float(f[4]),"volume":float(f[5])})
        except (ValueError,TypeError,IndexError): pass
    return out


def fetch_daily_kline(code: str, days: int = 100) -> List[Dict[str, Any]]:
    """日线采用东方财富主源、腾讯备用源，提升自定义股票评估可用性。"""
    code=clean_stock_code(code); out=[]; errors=[]
    for fetcher in (_eastmoney_daily_kline, lambda c: _tencent_daily_kline(c, days)):
        try:
            out=fetcher(code)
            if out: break
        except Exception as exc: errors.append(f"{type(exc).__name__}")
    if not out: raise RuntimeError("日线双数据源均不可用：" + ", ".join(errors))
    today=now_cn().date().isoformat()
    return [row for row in out if row["date"] < today][-days:]



def _chart_daily_bars(code: str, limit: int = 4200) -> Tuple[List[Dict[str, Any]], str, str]:
    """获取含当日动态日K的前复权数据；东方财富主源，腾讯备用。"""
    _, secid, _ = code_market(code); errors: List[str] = []
    try:
        params = {"secid": secid, "fields1": "f1,f2,f3,f4,f5,f6", "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                  "klt": 101, "fqt": 1, "beg": "19900101", "end": "20500101", "lmt": limit}
        r = requests.get("https://push2his.eastmoney.com/api/qt/stock/kline/get", params=params,
                         headers={"User-Agent":"Mozilla/5.0", "Referer":"https://quote.eastmoney.com/"}, timeout=10); r.raise_for_status()
        data = r.json().get("data") or {}; out = []
        for raw in data.get("klines") or []:
            f = str(raw).split(",")
            try: out.append({"date":f[0], "open":float(f[1]), "close":float(f[2]), "high":float(f[3]), "low":float(f[4]), "volume":float(f[5])})
            except (ValueError, IndexError): pass
        if out: return out[-limit:], str(data.get("name") or code), "东方财富"
    except Exception as exc: errors.append(f"eastmoney:{type(exc).__name__}")
    try:
        market, _, _ = code_market(code); symbol = market.lower() + code
        r = requests.get("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get", params={"param":f"{symbol},day,,,{min(limit, 1300)},qfq"},
                         headers={"User-Agent":"Mozilla/5.0", "Referer":"https://gu.qq.com/"}, timeout=10); r.raise_for_status()
        node = ((r.json().get("data") or {}).get(symbol) or {}); rows = node.get("qfqday") or node.get("day") or []; out=[]
        for f in rows:
            try: out.append({"date":str(f[0]), "open":float(f[1]), "close":float(f[2]), "high":float(f[3]), "low":float(f[4]), "volume":float(f[5])})
            except (ValueError, TypeError, IndexError): pass
        quote = ((node.get("qt") or {}).get(symbol) or [])
        if out: return out[-limit:], str(quote[1] if len(quote) > 1 else code), "腾讯行情"
    except Exception as exc: errors.append(f"tencent:{type(exc).__name__}")
    raise RuntimeError("K线双数据源均不可用：" + ", ".join(errors))


def _tencent_period_bars(code: str, period: str, limit: int) -> Tuple[List[Dict[str, Any]], str, str]:
    """腾讯周/月前复权K线，用于东方财富不可用时补足长周期均线。"""
    if period not in {"week", "month"}:
        raise ValueError("腾讯长周期只支持 week、month")
    market, _, _ = code_market(code); symbol = market.lower() + code
    request_limit = min(max(int(limit), 120), 640)
    r = requests.get(
        "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
        params={"param": f"{symbol},{period},,,{request_limit},qfq"},
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"},
        timeout=10,
    )
    r.raise_for_status(); node = ((r.json().get("data") or {}).get(symbol) or {})
    raw_rows = node.get("qfq" + period) or node.get(period) or []; out=[]
    for f in raw_rows:
        try: out.append({"date":str(f[0]), "open":float(f[1]), "close":float(f[2]), "high":float(f[3]), "low":float(f[4]), "volume":float(f[5])})
        except (ValueError, TypeError, IndexError): pass
    if not out: raise RuntimeError("腾讯长周期K线暂不可用")
    quote = ((node.get("qt") or {}).get(symbol) or [])
    return out[-request_limit:], str(quote[1] if len(quote) > 1 else code), "腾讯行情"


def _aggregate_kline(rows: List[Dict[str, Any]], period: str) -> List[Dict[str, Any]]:
    if period == "day": return [dict(x) for x in rows]
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        try: date = dt.date.fromisoformat(str(row.get("date")))
        except ValueError: continue
        key = f"{date.isocalendar().year}-W{date.isocalendar().week:02d}" if period == "week" else f"{date.year}-{date.month:02d}"
        grouped.setdefault(key, []).append(row)
    out=[]
    for items in grouped.values():
        out.append({"date":items[-1]["date"], "open":items[0]["open"], "close":items[-1]["close"],
                    "high":max(x["high"] for x in items), "low":min(x["low"] for x in items), "volume":sum(x["volume"] for x in items)})
    return out


def _with_moving_averages(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    closes = [safe_float(x.get("close")) for x in rows]; out=[]
    for i, raw in enumerate(rows):
        row = dict(raw)
        for n in (5, 10, 20, 60):
            row[f"ma{n}"] = round(sum(closes[i-n+1:i+1]) / n, 3) if i + 1 >= n else None
        for field in ("open", "close", "high", "low", "volume"):
            row[field] = round(safe_float(row.get(field)), 3 if field != "volume" else 0)
        out.append(row)
    return out


def kline_refresh_policy(current: Optional[dt.datetime] = None) -> Dict[str, Any]:
    """K线刷新策略：交易时段短缓存，其他时间降低无意义的外部请求。"""
    current = current or now_cn(); clock = current.strftime("%H:%M:%S")
    active = current.weekday() < 5 and (("09:15:00" <= clock <= "11:35:00") or ("12:55:00" <= clock <= "15:05:00"))
    return {
        "market_active": active,
        "market_state": "交易时段" if active else "非交易时段",
        "cache_ttl_seconds": max(5, KLINE_ACTIVE_CACHE_TTL if active else KLINE_CACHE_TTL),
        "auto_refresh_seconds": 30 if active else 300,
    }


def kline_payload(raw_code: Any, raw_period: Any = "day", raw_limit: Any = 120, force_refresh: bool = False) -> Dict[str, Any]:
    code = clean_stock_code(raw_code); period = str(raw_period or "day").lower()
    aliases = {"d":"day", "daily":"day", "w":"week", "weekly":"week", "m":"month", "monthly":"month"}; period = aliases.get(period, period)
    if period not in {"day", "week", "month"}: raise ValueError("K线周期只支持 day、week、month")
    try: limit = int(raw_limit)
    except (TypeError, ValueError): raise ValueError("K线数量参数无效")
    limit = max(40, min(limit, 640)); key=f"{code}:{period}:{limit}"; now=time.time(); current=now_cn(); policy=kline_refresh_policy(current)
    with KLINE_CACHE_LOCK:
        cached=KLINE_CACHE.get(key)
        if not force_refresh and cached and now-cached[0] < policy["cache_ttl_seconds"]:
            result=dict(cached[1]); result.update(policy); result["cached"]=True; result["cache_age_seconds"]=max(0,int(now-cached[0])); result["served_at"]=current.strftime("%Y-%m-%d %H:%M:%S")
            return result
    source_limits = {"day": limit + 60, "week": (limit + 60) * 6, "month": (limit + 60) * 24}
    daily, name, source = _chart_daily_bars(code, source_limits[period])
    if source == "腾讯行情" and period in {"week", "month"}:
        period_rows, name, source = _tencent_period_bars(code, period, limit + 60)
        aggregation_note = "周线和月线使用数据源提供的前复权长周期K线。"
    else:
        period_rows = _aggregate_kline(daily, period)
        aggregation_note = "周线和月线由前复权日线聚合。"
    # 多取60根用于均线预热，最终只返回页面需要的数量。
    calculated = _with_moving_averages(period_rows)
    rows = calculated[-limit:]
    if not rows: raise RuntimeError("未获得可展示的K线数据")
    fetched_at=current.strftime("%Y-%m-%d %H:%M:%S"); latest_bar_date=str(rows[-1].get("date") or "-")
    note = "日线盘中最后一根可能随当日行情变化；" + aggregation_note
    if latest_bar_date != current.date().isoformat():
        note += f" 当前最新K线截止 {latest_bar_date}，不是自然日 {current.date().isoformat()} 的行情。"
    payload={"ok":True,"code":code,"name":name,"period":period,"source":source,"adjust":"前复权","updated_at":fetched_at,"fetched_at":fetched_at,"served_at":fetched_at,
             "latest_bar_date":latest_bar_date,"cached":False,"cache_age_seconds":0,**policy,"rows":rows,
             "ma_periods":[5,10,20,60],"note":note}
    with KLINE_CACHE_LOCK:
        KLINE_CACHE[key]=(now,dict(payload))
        if len(KLINE_CACHE)>200:
            oldest=sorted(KLINE_CACHE.items(),key=lambda x:x[1][0])[:50]
            for old_key,_ in oldest: KLINE_CACHE.pop(old_key,None)
    return payload


def custom_watch_plan(code: str) -> Dict[str, Any]:
    code=clean_stock_code(code)
    base={"code":code,"name":code,"strategy_group":"自定义保守趋势确认","strategy":"MA20/MA60趋势 + 开盘承接","is_custom":True}
    try: bars=fetch_daily_kline(code,100)
    except Exception as exc:
        return {**base,"score":0,"buy_zone":"-","stop_loss":None,"action":"等待日线数据恢复后再评估","risk_tags":"日线数据不可用","setup_ok":False,"setup_reasons":[f"× 无法读取完整日线：{type(exc).__name__}"]}
    if len(bars)<65:
        return {**base,"score":0,"buy_zone":"-","stop_loss":None,"action":"样本不足，暂不建立计划","risk_tags":"日线样本不足","setup_ok":False,"setup_reasons":["× 至少需要60个完整交易日日线"]}
    c=[x["close"] for x in bars]; v=[x["volume"] for x in bars]; last=bars[-1]; ma20=sum(c[-20:])/20; ma60=sum(c[-60:])/60; ma20old=sum(c[-25:-5])/20; vol20=sum(v[-20:])/20; high20=max(x["high"] for x in bars[-20:]); low5=min(x["low"] for x in bars[-5:])
    try: name=fetch_live_quote(code).get("name") or code
    except Exception: name=code
    gates=[("收盘位于MA20上方",last["close"]>ma20,f"收盘 {last['close']:.2f}，MA20 {ma20:.2f}"),("中期均线多头",ma20>ma60,f"MA20 {ma20:.2f}，MA60 {ma60:.2f}"),("MA20最近5日向上",ma20>ma20old,f"5日前MA20 {ma20old:.2f}"),("未远离MA20",last["close"]<=ma20*1.10,f"偏离 {(last['close']/ma20-1)*100:.1f}%"),("量能不弱于常态",last["volume"]>=vol20*.70,f"量比 {last['volume']/vol20:.2f} 倍" if vol20 else "均量不可用"),("未处于20日极端追高位",last["close"]<=high20*.985 or last["close"]<=ma20*1.06,f"20日高点 {high20:.2f}")]
    ok=all(x[1] for x in gates); score=round(45+sum(8.5 for x in gates if x[1]),1); low=max(ma20*.995,last["close"]*.982); high=min(last["close"]*1.012,ma20*1.055); high=high if high>low else low*1.012
    return {**base,"name":name,"score":score,"buy_zone":f"{low:.2f}~{high:.2f}","stop_loss":round(min(ma20*.972,low5*.988),3),"action":"仅在计划区间且开盘承接正常时小仓分批；任一门槛失效则等待。","risk_tags":"自定义代码；需通过趋势门槛" if ok else "自定义代码；未通过保守趋势门槛","setup_ok":ok,"setup_reasons":[f"{'✓' if yes else '×'} {n}：{t}" for n,yes,t in gates]}

def opening_decision(row: Dict[str, Any], quote: Dict[str, Any]) -> Dict[str, Any]:
    """保守多条件开盘核验：输出条件与理由，不自动下单。"""
    code=clean_stock_code(row.get("code")); score=safe_float(row.get("score")); low,high=parse_price_range(row.get("buy_zone")); stop=nullable_float(row.get("stop_loss"),3); strategy=str(row.get("strategy") or "-"); group=str(row.get("strategy_group") or strategy_group_name(strategy)); level,_=level_for(row)
    custom=bool(row.get("is_custom")); setup_ok=bool(row.get("setup_ok",True)); rps60=nullable_float(row.get("rps60"),1); risk=str(row.get("risk_tags") or ""); risk_pass=not any(x in risk for x in ("波动偏大","高波动","换手过热","RSI过热","高风险")); score_limit=78 if custom else 72; score_pass=score>=score_limit; rps_pass=custom or rps60 is None or rps60>=55; emotional=any(x in strategy for x in ("首板","涨停","回马枪"))
    result={"code":code,"name":row.get("name") or code,"score":score,"level":level,"strategy":strategy,"strategy_group":group,"buy_zone":row.get("buy_zone") or "-","zone_low":low,"zone_high":high,"stop_loss":stop,"action":row.get("action") or "-","risk_tags":risk or "-","quote":quote,"source":"自定义代码" if custom else "昨日推荐","status":"行情未就绪","rank":9,"reason":"实时行情暂不可用，不能据此给出执行判断。","checks":[]}
    if quote.get("stale"):
        result["reason"] = f"行情日期 {quote.get('trade_date') or '未知'} 不是今天，可能为休市或数据源延迟，不能生成建仓结论。"
        return result
    if not quote.get("ok"): return result
    price,pct,gap=quote.get("price"),quote.get("pct"),quote.get("gap_pct"); in_zone=low is not None and high is not None and price is not None and low*.992<=price<=high*1.008; too_high=gap is not None and gap>4.5; limit_guard=17.0 if code.startswith(("30","68")) else 8.5; near_limit=pct is not None and pct>=limit_guard; broken=stop is not None and price is not None and price<=stop; weak=gap is not None and gap<=-3.5 and price is not None and quote.get("open") and price<quote["open"]; above=price is not None and quote.get("open") and price>=quote["open"]*.997
    checks=[{"name":"计划买入区间","pass":in_zone,"text":f"现价 {price:.2f}；计划区间 {low:.2f}~{high:.2f}" if low is not None and high is not None else "没有有效买入区间"},{"name":"开盘溢价","pass":not too_high,"text":f"开盘较昨收 {gap:+.2f}%（阈值≤+4.5%）" if gap is not None else "开盘价不可用"},{"name":"止损保护","pass":not broken and stop is not None,"text":f"止损 {stop:.3f}" if stop is not None else "缺少明确止损"},{"name":"开盘承接","pass":bool(above),"text":"现价未明显跌破开盘价" if above else "现价弱于开盘价"},{"name":"昨日信号强度","pass":score_pass,"text":f"规则评分 {score:.1f}（阈值≥{score_limit}）"},{"name":"相对强度/风险","pass":rps_pass and risk_pass,"text":(f"RPS60 {rps60:.0f}" if rps60 is not None else "无RPS60")+("；无高波动/过热标签" if risk_pass else f"；风险：{risk}")}]
    checks += [{"name":"自定义趋势门槛","pass":str(x).startswith("✓"),"text":str(x).lstrip("✓× ")} for x in (row.get("setup_reasons") or [])]
    result["checks"]=checks
    if broken: result.update(status="不建议参与",rank=3,reason="现价已触及或跌破预设止损，原计划失效；不做补仓或摊低成本。")
    elif near_limit or too_high: result.update(status="不建议追高",rank=2,reason=("盘中接近涨停" if near_limit else f"高开 {gap:+.2f}%")+"，已偏离计划价格；等待回踩，不因排名靠前追价。")
    elif weak: result.update(status="不建议参与",rank=3,reason="明显低开且开盘后继续走弱，等待新的日线信号。")
    elif emotional: result.update(status="等待人工确认",rank=1,reason="属于涨停/情绪类信号，必须人工复核竞价承接、板块强度和流动性。")
    elif custom and not setup_ok: result.update(status="不建议参与",rank=3,reason="自定义股票未通过MA20/MA60、量能和乖离等保守趋势门槛，不纳入可建仓名单。")
    elif not(score_pass and rps_pass and risk_pass): result.update(status="等待确认",rank=1,reason="未通过评分、相对强度或风险门槛；宁可错过，不降低标准。")
    elif in_zone and above and (pct is None or pct>-2.5) and stop is not None: result.update(status="可计划内执行",rank=0,reason="价格处于计划区间、未过度高开、承接未走弱，并通过评分/强度/风险与止损多重确认；仅适合小仓分批。")
    else: result.update(status="等待确认",rank=1,reason=("当前不在计划买入区间" if not in_zone else "开盘承接尚未确认")+"；不追涨、不抄底，等待条件同步满足。")
    return result

def _opening_snapshot_datetime(name: str) -> Optional[dt.datetime]:
    match = re.fullmatch(r"opening_(\d{8})_(\d{4})\.json", str(name or ""))
    if not match:
        return None
    try:
        return dt.datetime.strptime(match.group(1) + match.group(2), "%Y%m%d%H%M")
    except ValueError:
        return None

def _opening_snapshot_path(raw_file: Any) -> Optional[Path]:
    name = Path(str(raw_file or "")).name
    if name != str(raw_file or "") or _opening_snapshot_datetime(name) is None:
        return None
    path = REPORT_DIR / "opening_checks" / name
    try:
        if not path.is_file() or path.resolve().parent != (REPORT_DIR / "opening_checks").resolve():
            return None
        if path.stat().st_size > OPENING_SNAPSHOT_MAX_BYTES:
            return None
    except OSError:
        return None
    return path

def prune_opening_checks(out_dir: Optional[Path] = None, reference: Optional[dt.datetime] = None) -> int:
    """只保留当前自然日及前两日的定时核验快照。"""
    out_dir = out_dir or (REPORT_DIR / "opening_checks")
    if not out_dir.exists():
        return 0
    cutoff = (reference or now_cn()).date() - dt.timedelta(days=OPENING_HISTORY_RETENTION_DAYS - 1)
    removed = 0
    for path in out_dir.glob("opening_*.json"):
        stamp = _opening_snapshot_datetime(path.name)
        if stamp is not None and stamp.date() < cutoff:
            try:
                path.unlink(); removed += 1
            except OSError:
                pass
    return removed

def _read_opening_snapshot(raw_file: Any) -> Optional[Tuple[Path, Dict[str, Any]]]:
    path = _opening_snapshot_path(raw_file)
    if path is None:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    return (path, payload) if isinstance(payload, dict) else None

def _opening_change_pct(price: Any, baseline: Any) -> Optional[float]:
    current = nullable_float(price, 4); start = nullable_float(baseline, 4)
    if current is None or start is None or current <= 0 or start <= 0:
        return None
    return round((current / start - 1) * 100, 2)

def opening_history_payload(limit: int = 12) -> List[Dict[str, Any]]:
    """读取最近三日的自动开盘核验快照，只返回页面复盘需要的汇总字段。"""
    out_dir = REPORT_DIR / "opening_checks"
    prune_opening_checks(out_dir)
    items: List[Dict[str, Any]] = []
    if not out_dir.exists():
        return items
    safe_limit = max(1, min(int(limit), 50))
    paths = sorted((p for p in out_dir.glob("opening_*.json") if _opening_snapshot_datetime(p.name) is not None), reverse=True)
    for path in paths[:safe_limit]:
        read = _read_opening_snapshot(path.name)
        if read is None:
            continue
        _, payload = read
        summary = payload.get("summary") or {}
        items.append({
            "file": path.name,
            "checked_at": str(payload.get("checked_at") or ""),
            "source_date": str(payload.get("source_date") or ""),
            "total": int(safe_float(summary.get("total"))),
            "eligible": int(safe_float(summary.get("eligible"))),
            "wait": int(safe_float(summary.get("wait"))),
            "avoid": int(safe_float(summary.get("avoid"))),
            "unready": int(safe_float(summary.get("unready"))),
        })
    return items

def opening_detail_payload(raw_file: Any) -> Dict[str, Any]:
    # 详情接口也执行清理，避免仅凭旧文件名绕过最近三日留存规则。
    prune_opening_checks()
    read = _read_opening_snapshot(raw_file)
    if read is None:
        return {"ok": False, "message": "核验详情不存在、文件格式不正确或已超过最近三日保留期。", "rows": []}
    path, payload = read
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    rows = [dict(row) for row in rows[:MAX_OPENING_DETAIL_ROWS] if isinstance(row, dict)]
    codes: List[str] = []
    for row in rows:
        try:
            code = clean_stock_code(row.get("code")); row["code"] = code
        except ValueError:
            continue
        if code not in codes:
            codes.append(code)
    def fetch_one(code: str) -> Tuple[str, Dict[str, Any]]:
        try:
            return code, fetch_live_quote(code)
        except Exception as exc:
            return code, {"ok": False, "code": code, "source": "当前行情获取失败", "error": type(exc).__name__, "fetched_at": now_cn().strftime("%Y-%m-%d %H:%M:%S")}
    latest: Dict[str, Dict[str, Any]] = {}
    if codes:
        with futures.ThreadPoolExecutor(max_workers=min(10, len(codes))) as ex:
            for code, quote_info in ex.map(fetch_one, codes):
                latest[code] = quote_info if isinstance(quote_info, dict) else {}
    detail_rows: List[Dict[str, Any]] = []
    for row in rows:
        code = str(row.get("code") or "")
        if not code:
            continue
        baseline = row.get("monitor_baseline") if isinstance(row.get("monitor_baseline"), dict) else {}
        quote_snapshot = row.get("quote") if isinstance(row.get("quote"), dict) else {}
        current = latest.get(code) or {}
        baseline_price = nullable_float(baseline.get("price"), 4)
        snapshot_price = nullable_float(quote_snapshot.get("price"), 4)
        current_price = nullable_float(current.get("price"), 4)
        detail = dict(row)
        detail.update({
            "monitor_baseline": baseline,
            "baseline_price": baseline_price,
            "snapshot_price": snapshot_price,
            "snapshot_change_pct": _opening_change_pct(snapshot_price, baseline_price),
            "current_quote": current,
            "current_price": current_price,
            "current_change_pct": _opening_change_pct(current_price, baseline_price),
        })
        detail_rows.append(detail)
    return {"ok": True, "file": path.name, "checked_at": str(payload.get("checked_at") or ""), "source_date": str(payload.get("source_date") or ""), "summary": payload.get("summary") if isinstance(payload.get("summary"), dict) else {}, "message": "历史核验价为保存快照；当前行情在打开详情时重新获取，二者均相对加入监控时的基准价计算涨跌。", "rows": detail_rows}


def opening_check_payload(force: bool = False) -> Dict[str, Any]:
    session=opening_session(); signal=latest_signal_file(); watch=watchlist_payload(); mode="自选监控" if watch["count"] else "默认高分候选"
    if not signal: return {"ok":False,"session":session,"message":"尚无收盘扫描信号，不能生成开盘核验。","rows":[],"summary":{},"schedule":OPENING_SCHEDULE,"run_state":public_run_state(OPENING_RUN_STATE, OPENING_LOCK),"watchlist":watch,"monitor_mode":mode,"history":opening_history_payload()}
    ymd=date_from_signal(signal); source_date=f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"; rows=consolidate_signal_rows(read_csv_dict(signal)); mapping={str(row["code"]):row for row in rows}
    try: signal_age_days=(now_cn().date()-dt.datetime.strptime(ymd,"%Y%m%d").date()).days
    except ValueError: signal_age_days=999
    common={"source_date":source_date,"signal_age_days":signal_age_days,"schedule":OPENING_SCHEDULE,"run_state":public_run_state(OPENING_RUN_STATE, OPENING_LOCK),"limit":OPENING_CHECK_LIMIT,"watchlist":watch,"monitor_mode":mode,"history":opening_history_payload()}
    # 非盘中时段先返回，避免为自定义清单无意义地请求日线和实时行情。
    if not session["active"]: return {"ok":True,"session":session,"message":"当前不在盘中核验时段；页面将在交易时段内读取实时行情。","rows":[],"summary":{},**common}
    if ymd>=now_cn().strftime("%Y%m%d"): return {"ok":False,"session":session,"message":"当前收盘信号与今日同日，需等待下一交易日开盘后再做建仓核验。","rows":[],"summary":{},**common}
    if signal_age_days > 14: return {"ok":False,"session":session,"message":f"最近收盘信号距今已 {signal_age_days} 天，超过安全时效，禁止生成建仓结论；请先完成新的全市场扫描。","rows":[],"summary":{},**common}
    ranked=[]
    if watch["count"]:
        for code in watch["codes"]:
            item=dict(mapping[code]) if code in mapping else custom_watch_plan(code); item["is_custom"]=code not in mapping; ranked.append(item)
    else: ranked=sorted(rows,key=lambda r:(-safe_float(r.get("score")),str(r.get("code") or "")))[:OPENING_CHECK_LIMIT]
    cache_key=f"{ymd}:{','.join(watch['codes'])}"; now=time.time()
    with OPENING_LOCK: cache=OPENING_CACHE.copy()
    if not force and cache.get("payload") and cache.get("source")==cache_key and now-safe_float(cache.get("at"))<OPENING_CACHE_TTL:
        result=dict(cache["payload"]); result["cached"]=True; result["history"]=opening_history_payload(); return result
    acquired=OPENING_REFRESH_LOCK.acquire(timeout=30 if force else 0)
    if not acquired:
        with OPENING_LOCK: busy_cache=OPENING_CACHE.copy()
        if busy_cache.get("payload") and busy_cache.get("source")==cache_key:
            result=dict(busy_cache["payload"]); result["cached"]=True; result["refreshing"]=True; result["history"]=opening_history_payload(); return result
        return {"ok":True,"cached":False,"refreshing":True,"session":session,"message":"实时行情正在刷新，请稍后再次查看；为避免重复请求数据源，本次未并发发起第二轮核验。","rows":[],"summary":{},**common}
    try:
        if not force:
            with OPENING_LOCK: cache=OPENING_CACHE.copy()
            if cache.get("payload") and cache.get("source")==cache_key and time.time()-safe_float(cache.get("at"))<OPENING_CACHE_TTL:
                result=dict(cache["payload"]); result["cached"]=True; result["history"]=opening_history_payload(); return result
        def one(row):
            code=clean_stock_code(row.get("code"))
            try: return code,fetch_live_quote(code)
            except Exception as exc: return code,{"ok":False,"stale":False,"code":code,"name":row.get("name") or code,"source":"双源失败","error":f"{type(exc).__name__}","fetched_at":now_cn().strftime("%Y-%m-%d %H:%M:%S")}
        quotes={}
        with futures.ThreadPoolExecutor(max_workers=min(10,max(1,len(ranked)))) as ex:
            for code,quote in ex.map(one,ranked): quotes[code]=quote
        out=[opening_decision(row,quotes.get(clean_stock_code(row.get("code")),{})) for row in ranked]; out.sort(key=lambda r:(r["rank"],-safe_float(r.get("score")),r["code"]))
        # 记录本次核验所对应的加入监控基准，防止后续清单变动后历史对比失真。
        baseline_by_code = watch.get("baseline_by_code") or {}
        for item in out:
            baseline = baseline_by_code.get(str(item.get("code") or ""))
            if isinstance(baseline, dict):
                item["monitor_baseline"] = dict(baseline)
        summary={"total":len(out),"eligible":sum(r["status"]=="可计划内执行" for r in out),"wait":sum(r["status"] in ("等待确认","等待人工确认") for r in out),"avoid":sum(r["status"] in ("不建议参与","不建议追高") for r in out),"unready":sum(r["status"]=="行情未就绪" for r in out)}
        payload={"ok":True,"cached":False,"session":session,"message":"开盘确认采用保守多条件确认模型，基于前一交易日信号与公开行情快照；不会自动下单。","checked_at":now_cn().strftime("%Y-%m-%d %H:%M:%S"),"rows":out,"summary":summary,**common}
        with OPENING_LOCK: OPENING_CACHE.update({"at":time.time(),"source":cache_key,"payload":payload})
        return payload
    finally:
        OPENING_REFRESH_LOCK.release()

def save_opening_check(payload: Dict[str, Any]) -> Optional[Path]:
    if not payload.get("rows"):
        return None
    out_dir = REPORT_DIR / "opening_checks"; out_dir.mkdir(parents=True, exist_ok=True)
    prune_opening_checks(out_dir)
    p = out_dir / f"opening_{now_cn().strftime('%Y%m%d_%H%M')}.json"
    temp = p.with_suffix(".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(p)
    return p


def run_opening_check_background() -> bool:
    with OPENING_LOCK:
        if OPENING_RUN_STATE.get("running"):
            return False
        OPENING_RUN_STATE.update({"running": True, "last_started": now_cn().strftime("%Y-%m-%d %H:%M:%S"), "last_finished": None, "last_error": None})
    def target() -> None:
        try:
            payload = opening_check_payload(force=True)
            saved = save_opening_check(payload)
            with OPENING_LOCK:
                OPENING_RUN_STATE["last_file"] = str(saved) if saved else None
        except Exception as e:
            with OPENING_LOCK:
                OPENING_RUN_STATE["last_error"] = f"{type(e).__name__}: {e}"
        finally:
            with OPENING_LOCK:
                OPENING_RUN_STATE["running"] = False; OPENING_RUN_STATE["last_finished"] = now_cn().strftime("%Y-%m-%d %H:%M:%S")
    threading.Thread(target=target, name="opening-check", daemon=True).start()
    return True


def opening_scheduler_loop(schedule: str) -> None:
    times: List[Tuple[int, int]] = []
    for item in schedule.split(","):
        item = item.strip()
        if re.match(r"^\d{1,2}:\d{2}$", item):
            h, m = item.split(":"); times.append((int(h), int(m)))
    last = ""
    while True:
        try:
            n = now_cn()
            if n.weekday() < 5:
                for h, m in times:
                    if n.hour == h and n.minute == m:
                        key = f"{n.date()} {h:02d}:{m:02d}"
                        if key != last:
                            last = key; run_opening_check_background()
            time.sleep(20)
        except Exception:
            time.sleep(60)

def all_reports() -> List[Dict[str, Any]]:
    out=[]
    for p in sorted(REPORT_DIR.glob("signals_*.csv"), reverse=True):
        ymd=date_from_signal(p); rows=read_csv_dict(p); meta=meta_for_date(ymd)
        out.append({"date": f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}" if len(ymd)==8 else ymd, "signal_file": p.name, "report_file": f"report_{ymd}.md" if (REPORT_DIR/f"report_{ymd}.md").exists() else None, "count": len(rows), "universe_count": meta.get("universe_count"), "kline_scanned_count": meta.get("kline_scanned_count"), "top_score": max([safe_float(r.get("score")) for r in rows], default=0)})
    return out

def feedback_overview() -> Dict[str, Any]:
    """读取每个信号日最新的一份日线复盘，避免同一信号日被重复统计。"""
    latest_by_source: Dict[str, Tuple[str, Path]] = {}
    for p in REPORT_DIR.glob("feedback_*_asof_*.csv"):
        m = re.match(r"feedback_(\d{8})_asof_(\d{8})\.csv$", p.name)
        if not m:
            continue
        source, asof = m.groups()
        if source not in latest_by_source or asof > latest_by_source[source][0]:
            latest_by_source[source] = (asof, p)
    if not latest_by_source:
        return {"has_data": False, "message": "尚未形成可复盘样本。系统会在下一交易日自动以信号日收盘为基准追踪上一批候选。"}
    all_rows: List[Dict[str, Any]] = []
    for source, (asof, p) in latest_by_source.items():
        for r in read_csv_dict(p):
            r["source_date"] = source
            r["as_of_date"] = asof
            all_rows.append(r)
    def valid_num(v: Any) -> Optional[float]:
        try:
            if v in (None, "", "-", "nan"):
                return None
            return float(v)
        except Exception:
            return None
    observed = [valid_num(r.get("ret_latest_pct")) for r in all_rows]
    observed = [v for v in observed if v is not None]
    positive = sum(v > 0 for v in observed)
    summary = {
        "sample_count": len(all_rows),
        "observed_count": len(observed),
        "positive_count": positive,
        "positive_rate_pct": round(positive / len(observed) * 100, 1) if observed else None,
        "avg_observation_return_pct": round(sum(observed) / len(observed), 2) if observed else None,
        "target_hit_count": sum(str(r.get("target_hit", "")) == "是" for r in all_rows),
        "stop_hit_count": sum(str(r.get("stop_hit", "")) == "是" for r in all_rows),
        "signal_days": len(latest_by_source),
        "sample_adequacy": "可初步观察" if len(observed) >= 30 else "样本不足，不判断有效性",
    }
    # 多战法共振会在各自战法下留下归因记录，仅用于观察，不可视为独立因果样本。
    tactic_buckets: Dict[str, List[float]] = {}
    tactic_meta: Dict[str, Dict[str, int]] = {}
    for r in all_rows:
        ret = valid_num(r.get("ret_latest_pct"))
        for tactic in {x.strip() for x in str(r.get("strategy", "")).split("+") if x.strip()}:
            tactic_buckets.setdefault(tactic, [])
            tactic_meta.setdefault(tactic, {"sample_count": 0, "target_hit_count": 0, "stop_hit_count": 0})
            tactic_meta[tactic]["sample_count"] += 1
            tactic_meta[tactic]["target_hit_count"] += int(str(r.get("target_hit", "")) == "是")
            tactic_meta[tactic]["stop_hit_count"] += int(str(r.get("stop_hit", "")) == "是")
            if ret is not None:
                tactic_buckets[tactic].append(ret)
    tactic_stats=[]
    for tactic, meta in tactic_meta.items():
        vals=tactic_buckets.get(tactic, [])
        tactic_stats.append({"strategy": tactic, **meta, "observed_count": len(vals), "positive_rate_pct": round(sum(v > 0 for v in vals) / len(vals) * 100, 1) if vals else None, "avg_observation_return_pct": round(sum(vals) / len(vals), 2) if vals else None})
    tactic_stats.sort(key=lambda x: (x["sample_count"], x["avg_observation_return_pct"] if x["avg_observation_return_pct"] is not None else -999), reverse=True)
    all_rows.sort(key=lambda r: (str(r.get("source_date", "")), str(r.get("code", ""))), reverse=True)
    newest_source = max(latest_by_source)
    return {
        "has_data": True,
        "source_date": f"{newest_source[:4]}-{newest_source[4:6]}-{newest_source[6:]}",
        "as_of_date": f"{latest_by_source[newest_source][0][:4]}-{latest_by_source[newest_source][0][4:6]}-{latest_by_source[newest_source][0][6:]}",
        "summary": summary,
        "rows": all_rows[:20],
        "tactic_stats": tactic_stats[:18],
        "definition": "以信号日收盘为基准的日线观察，不等同于真实成交收益；同日触及止损与目标时，无法用日线确定先后。",
    }

def recommendation_text(rows: List[Dict[str, Any]], market: str) -> str:
    if not rows: return "今日没有通过规则的强信号。弱市或无信号时，最好的交易是等待。"
    non_risk=[r for r in rows if (r.get("strategy_group") or "") != "涨停情绪类"]
    top=(non_risk or rows)[0]
    lvl,_=level_for(top)
    caution = "；当前大盘偏弱，建议降低仓位" if "弱" in market or "防守" in market else ""
    return f"今日首选：{top.get('code')} {top.get('name')}｜{top.get('strategy_group')}｜{top.get('strategy')}｜规则评分 {top.get('score')}（{lvl}）。执行：{top.get('action')}；区间 {top.get('buy_zone')}；止损 {top.get('stop_loss')}{caution}。"

def load_latest_payload() -> Dict[str, Any]:
    signal=latest_signal_file()
    if not signal:
        return {"has_data": False, "message": "暂无报告，请点击“立即运行全市场扫描”。", "run_state": public_run_state(RUN_STATE, RUN_LOCK), "reports": all_reports(), "feedback": feedback_overview(), "group_order": GROUP_ORDER, "strategy_book": STRATEGY_BOOK, "schedule": DEFAULT_SCHEDULE, "watchlist": watchlist_payload()}
    ymd=date_from_signal(signal); rows=read_csv_dict(signal); rows.sort(key=lambda r: safe_float(r.get("score")), reverse=True)
    # 旧版 CSV 不含执行字段，先加载当日元数据再做兼容补齐。
    meta=meta_for_date(ymd); report=report_for_date(ymd); report_text=report.read_text(encoding="utf-8") if report else ""
    for r in rows:
        if not r.get("strategy_group"): r["strategy_group"] = strategy_group_name(str(r.get("strategy", "")))
        if not r.get("execution_stage"):
            r["execution_stage"] = "防守观察" if "弱势" in (meta.get("market_label") or "") else "次日条件确认"
        if not r.get("risk_budget_hint"):
            r["risk_budget_hint"] = "请按自身风险承受能力设置仓位，避免同题材集中"
        lvl,cls=level_for(r); r["level"]=lvl; r["level_class"]=cls
    market=meta.get("market_label") or ""
    if not market:
        mm=re.search(r"市场环境：\*\*(.*?)\*\*", report_text); market=mm.group(1) if mm else ""
    # 一只股票可能同时触发多个战法分类（例如“突破类+趋势动量类”）。
    # 统计与分类龙头按每一个命中的分类归属，避免多标签结果在分类页中“消失”。
    groups=[g for g in GROUP_ORDER if g!="全部"]
    group_counts={g:0 for g in groups}
    group_top={}
    for r in rows:
        labels=str(r.get("strategy_group") or "综合类")
        matched=[g for g in groups if g in labels]
        if not matched: matched=["综合类"]
        for g in matched:
            group_counts[g]=group_counts.get(g,0)+1
            if g not in group_top: group_top[g]=r
    quality = signal_data_quality(signal, require_today=False)
    scope = {
        "is_full": str(meta.get("scan_mode", "")).startswith("全市场") or DEFAULT_FULL,
        "text": "全市场基础过滤后扫描：沪深主板/创业板/科创板全部先进入基础池，再排除ST、退市、新股、低流动性/低市值/极端换手，最后逐只抓K线做战法深筛。",
        "spot_count": meta.get("spot_count"), "universe_count": meta.get("universe_count"), "kline_scanned_count": meta.get("kline_scanned_count"), "failed_count": meta.get("failed_count", len(meta.get("errors", []))), "data_quality": quality, "execution_policy": meta.get("execution_policy", "候选信号需等待下一交易日价格条件确认，不构成直接交易指令。"),
    }
    return {"has_data": True, "date": f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}" if len(ymd)==8 else ymd, "rows": rows, "top": rows[:12], "recommendation": recommendation_text(rows, market), "market": market, "group_counts": group_counts, "group_top": group_top, "group_order": GROUP_ORDER, "strategy_book": STRATEGY_BOOK, "scope": scope, "meta": meta, "signal_file": signal.name, "report_file": report.name if report else None, "report_excerpt": report_text[:12000], "run_state": public_run_state(RUN_STATE, RUN_LOCK), "reports": all_reports(), "feedback": feedback_overview(), "schedule": DEFAULT_SCHEDULE, "watchlist": watchlist_payload()}

def run_scan_background(top:int=DEFAULT_TOP,max_stocks:int=DEFAULT_MAX_STOCKS,full:bool=DEFAULT_FULL,workers:int=DEFAULT_WORKERS)->bool:
    with RUN_LOCK:
        if RUN_STATE.get("running"): return False
        RUN_STATE.update({"running":True,"last_started":now_cn().strftime("%Y-%m-%d %H:%M:%S"),"last_finished":None,"last_returncode":None,"last_error":None})
        def target():
            log_path=LOG_DIR/f"scan_{now_cn().strftime('%Y%m%d_%H%M%S')}.log"
            with RUN_LOCK: RUN_STATE["last_log"]=str(log_path)
            cmd=[sys.executable,str(ENGINE),"--top",str(top),"--workers",str(workers)]
            if full: cmd.append("--full")
            else: cmd += ["--max-stocks",str(max_stocks)]
            try:
                with log_path.open("w",encoding="utf-8") as log:
                    log.write("CMD: "+" ".join(cmd)+"\n\n"); log.flush()
                    p=subprocess.run(cmd,cwd=str(BASE_DIR),stdout=log,stderr=subprocess.STDOUT,text=True,timeout=60*120)
                with RUN_LOCK:
                    RUN_STATE["last_returncode"]=p.returncode
                    if p.returncode!=0:
                        RUN_STATE["last_error"]=f"扫描退出码 {p.returncode}，请查看 {log_path}"
                if p.returncode==0:
                    sync_result = sync_auto_watchlist()
                    with RUN_LOCK: RUN_STATE["last_auto_sync"] = sync_result
            except Exception as e:
                with RUN_LOCK:
                    RUN_STATE["last_returncode"]=-1; RUN_STATE["last_error"]=f"{type(e).__name__}: {e}"
                with log_path.open("a",encoding="utf-8") as log: log.write("\nERROR:\n"+traceback.format_exc())
            finally:
                with RUN_LOCK:
                    RUN_STATE["running"]=False; RUN_STATE["last_finished"]=now_cn().strftime("%Y-%m-%d %H:%M:%S")
        threading.Thread(target=target,name="quant-scan",daemon=True).start(); return True

def scheduler_loop(schedule:str):
    times=[]
    for item in schedule.split(','):
        item=item.strip()
        if re.match(r"^\d{1,2}:\d{2}$", item):
            h,m=item.split(':'); times.append((int(h),int(m)))
    last=""
    while True:
        try:
            n=now_cn()
            if n.weekday()<5:
                for h,m in times:
                    if n.hour==h and n.minute==m:
                        key=f"{n.date()} {h:02d}:{m:02d}"
                        if key!=last: last=key; run_scan_background()
            time.sleep(20)
        except Exception: time.sleep(60)


def _trade_date(value: Any, field: str, required: bool = True) -> str:
    text = str(value or "").strip()
    if not text and not required: return ""
    try: parsed = dt.date.fromisoformat(text)
    except ValueError: raise ValueError(f"{field}必须是 YYYY-MM-DD 格式")
    if parsed < dt.date(2000, 1, 1) or parsed > now_cn().date() + dt.timedelta(days=1): raise ValueError(f"{field}超出允许范围")
    return parsed.isoformat()


def _trade_number(value: Any, field: str, low: float, high: float) -> float:
    try: number = float(value)
    except (TypeError, ValueError): raise ValueError(f"{field}必须是数字")
    if number != number or number < low or number > high: raise ValueError(f"{field}必须在 {low:g} 到 {high:g} 之间")
    return round(number, 4)


def _trade_text(value: Any, field: str, limit: int, required: bool = False) -> str:
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", str(value or "")).strip()
    if required and not text: raise ValueError(f"{field}不能为空")
    if len(text) > limit: raise ValueError(f"{field}不能超过 {limit} 个字符")
    return text


def _trade_metrics(item: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(item); entry = safe_float(row.get("entry_price")); shares = int(safe_float(row.get("shares")))
    initial_stop = safe_float(row.get("initial_stop_loss")); current_stop = safe_float(row.get("current_stop_loss"), initial_stop)
    initial_risk = max(0.0, entry - initial_stop) * shares; current_risk = max(0.0, entry - current_stop) * shares
    row.update({"cost": round(entry * shares, 2), "initial_risk": round(initial_risk, 2), "current_risk": round(current_risk, 2), "protected_profit": round(max(0.0, current_stop - entry) * shares, 2)})
    if row.get("status") == "closed" and row.get("exit_price") not in (None, ""):
        pnl = (safe_float(row.get("exit_price")) - entry) * shares
        row.update({"pnl": round(pnl, 2), "return_pct": round(pnl / (entry * shares) * 100, 2) if entry > 0 and shares > 0 else None, "r_multiple": round(pnl / initial_risk, 2) if initial_risk > 0 else None})
    else: row.update({"pnl": None, "return_pct": None, "r_multiple": None})
    return row


def _empty_trade_state(warning: str = "") -> Dict[str, Any]: return {"updated_at": "", "items": [], "warning": warning}


def _load_trade_state(strict: bool = False) -> Dict[str, Any]:
    if not TRADE_JOURNAL_FILE.exists(): return _empty_trade_state()
    try:
        if TRADE_JOURNAL_FILE.stat().st_size > MAX_TRADE_FILE_BYTES: raise ValueError("交易复盘数据文件超过 2MB 安全上限")
        raw = json.loads(TRADE_JOURNAL_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or not isinstance(raw.get("items", []), list): raise ValueError("交易复盘数据格式无效")
        return {"updated_at": str(raw.get("updated_at") or ""), "items": [dict(x) for x in raw.get("items", []) if isinstance(x, dict)][:MAX_TRADE_RECORDS], "warning": ""}
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        if strict: raise ValueError(f"交易复盘数据暂不可写：{exc}")
        return _empty_trade_state("交易复盘数据读取失败，已进入只读安全降级，请检查服务日志或恢复备份。")


def _save_trade_state(state: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": now_cn().strftime("%Y-%m-%d %H:%M:%S"), "items": state.get("items", [])[:MAX_TRADE_RECORDS]}
    encoded = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    if len(encoded) > MAX_TRADE_FILE_BYTES: raise ValueError("交易复盘记录过多，保存后将超过 2MB 安全上限")
    temp = TRADE_JOURNAL_FILE.with_name(TRADE_JOURNAL_FILE.name + ".tmp")
    try:
        with temp.open("wb") as fh: fh.write(encoded); fh.flush(); os.fsync(fh.fileno())
        temp.replace(TRADE_JOURNAL_FILE)
    finally:
        if temp.exists():
            try: temp.unlink()
            except OSError: pass


def trade_journal_payload() -> Dict[str, Any]:
    with TRADE_JOURNAL_LOCK: state = _load_trade_state()
    rows = [_trade_metrics(x) for x in state.get("items", [])]
    open_rows = sorted([x for x in rows if x.get("status") == "open"], key=lambda x: str(x.get("entry_date") or ""), reverse=True)
    closed_rows = sorted([x for x in rows if x.get("status") == "closed"], key=lambda x: str(x.get("exit_date") or ""), reverse=True)
    return {"ok": not bool(state.get("warning")), "warning": state.get("warning", ""), "updated_at": state.get("updated_at", ""), "open": open_rows, "closed": closed_rows,
            "summary": {"open_count": len(open_rows), "closed_count": len(closed_rows), "open_cost": round(sum(safe_float(x.get("cost")) for x in open_rows), 2), "current_risk": round(sum(safe_float(x.get("current_risk")) for x in open_rows), 2), "realized_pnl": round(sum(safe_float(x.get("pnl")) for x in closed_rows), 2)}}


def trade_journal_action(body: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(body.get("mode") or "").strip().lower()
    if mode not in {"add", "update", "close", "delete"}: raise ValueError("不支持的交易复盘操作")
    with TRADE_JOURNAL_LOCK:
        state = _load_trade_state(strict=True); items = state["items"]; now_text = now_cn().strftime("%Y-%m-%d %H:%M:%S")
        if mode == "add":
            if len(items) >= MAX_TRADE_RECORDS: raise ValueError(f"交易复盘最多保存 {MAX_TRADE_RECORDS} 条记录")
            code = clean_stock_code(body.get("code")); entry_date = _trade_date(body.get("entry_date"), "建仓日期"); signal_date = _trade_date(body.get("signal_date"), "信号日期", required=False)
            if signal_date and signal_date > entry_date: raise ValueError("信号日期不能晚于建仓日期")
            entry = _trade_number(body.get("entry_price"), "建仓价", 0.01, 100000); stop = _trade_number(body.get("initial_stop_loss"), "初始止损", 0.01, 100000)
            if stop >= entry: raise ValueError("初始止损必须低于建仓价；移动止盈请在新增后更新当前保护价")
            shares_raw = _trade_number(body.get("shares"), "股数", 1, 100000000)
            if int(shares_raw) != shares_raw: raise ValueError("股数必须是整数")
            items.append({"id": secrets.token_urlsafe(12), "code": code, "name": _trade_text(body.get("name"), "股票名称", 40), "signal_date": signal_date, "entry_date": entry_date, "entry_price": entry, "shares": int(shares_raw), "initial_stop_loss": stop, "current_stop_loss": stop, "status": "open", "exit_date": "", "exit_price": None, "note": _trade_text(body.get("note"), "备注", 300), "created_at": now_text, "updated_at": now_text})
        else:
            record_id = _trade_text(body.get("id"), "记录ID", 80, required=True); item = next((x for x in items if hmac.compare_digest(str(x.get("id") or ""), record_id)), None)
            if not item: raise ValueError("交易记录不存在或已被删除")
            if mode == "delete": items.remove(item)
            elif mode == "update":
                if item.get("status") != "open": raise ValueError("已结束交易不能修改持仓风控")
                if "current_stop_loss" in body: item["current_stop_loss"] = _trade_number(body.get("current_stop_loss"), "当前保护价", 0.01, 100000)
                if "note" in body: item["note"] = _trade_text(body.get("note"), "备注", 300)
                item["updated_at"] = now_text
            else:
                if item.get("status") != "open": raise ValueError("该交易已经结束")
                exit_date = _trade_date(body.get("exit_date"), "退出日期")
                if exit_date < str(item.get("entry_date") or ""): raise ValueError("退出日期不能早于建仓日期")
                item.update({"status": "closed", "exit_date": exit_date, "exit_price": _trade_number(body.get("exit_price"), "退出价", 0.01, 100000), "note": _trade_text(body.get("note", item.get("note")), "备注", 300), "updated_at": now_text})
        _save_trade_state(state)
    return trade_journal_payload()



def trade_journal_page_html() -> str:
    extra = r'''.journal-nav{display:flex;gap:10px;flex-wrap:wrap}.journal-grid{display:grid;grid-template-columns:380px 1fr;gap:16px}.journal-form{display:grid;grid-template-columns:1fr 1fr;gap:11px}.journal-form label{display:grid;gap:5px;color:var(--muted);font-size:13px}.journal-form .wide{grid-column:1/-1}.journal-form input,.journal-form textarea{width:100%;box-sizing:border-box;background:#081221;border:1px solid var(--line);color:var(--text);border-radius:10px;padding:11px}.journal-form textarea{min-height:80px;resize:vertical}.journal-summary{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px}.journal-stat{padding:15px;border:1px solid var(--line);background:rgba(9,20,36,.75);border-radius:14px}.journal-stat b{font-size:22px;display:block;margin-top:6px}.journal-table{overflow:auto}.journal-table table{min-width:1050px}.journal-empty{padding:30px;text-align:center;color:var(--muted)}.journal-warn{padding:12px;border:1px solid #805f25;background:#2b210e;color:#ffd889;border-radius:10px;margin-bottom:12px}.pnl-up{color:#69e6aa}.pnl-down{color:#ff8d99}.journal-note{max-width:250px;white-space:normal}.mini-actions{display:flex;gap:6px;flex-wrap:wrap}.mini-actions button{padding:6px 9px}.token-gate{max-width:560px;margin:80px auto}.journal-tabs{display:flex;gap:8px;margin:12px 0}.journal-tabs button.active{border-color:var(--cyan);color:var(--cyan)}@media(max-width:900px){.journal-grid{grid-template-columns:1fr}.journal-summary{grid-template-columns:1fr 1fr}}@media(max-width:560px){.journal-form{grid-template-columns:1fr}.journal-form .wide{grid-column:auto}.journal-summary{grid-template-columns:1fr}}'''
    js = r'''try{localStorage.removeItem('quant_token')}catch(e){}
const esc=v=>String(v??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
function token(){try{return sessionStorage.getItem('quant_token')||''}catch(e){return ''}}function keepToken(v){try{sessionStorage.setItem('quant_token',v)}catch(e){}}function money(v){return Number(v||0).toLocaleString('zh-CN',{minimumFractionDigits:2,maximumFractionDigits:2})}let journal=null,tab='open';
async function api(method,body){const h={'X-Quant-Token':token()};if(body)h['Content-Type']='application/json';const r=await fetch('/api/trades',{method,headers:h,body:body?JSON.stringify(body):undefined});let d={};try{d=await r.json()}catch(e){}if(r.status===401){showGate(d.message||'请输入管理令牌');throw new Error('unauthorized')}if(!r.ok)throw new Error(d.message||'请求失败');return d}
function showGate(msg='交易复盘台需要管理令牌'){document.getElementById('journalApp').innerHTML=`<section class="card token-gate"><span class="eyebrow">受保护的交易记录</span><h1>解锁交易复盘台</h1><p class="muted">${esc(msg)}。令牌只保存在当前浏览器标签页，关闭后失效。</p><div class="watch-add"><input id="journalToken" class="fundamental-input" type="password" placeholder="输入管理令牌"><button class="btn primary" id="unlock">解锁</button><a class="btn" href="/">返回首页</a></div></section>`;document.getElementById('unlock').onclick=()=>{const v=document.getElementById('journalToken').value.trim();if(v){keepToken(v);loadJournal()}}}
async function loadJournal(){try{journal=await api('GET');renderJournal()}catch(e){if(e.message!=='unauthorized')showGate(e.message)}}
function formHtml(){const today=new Date().toLocaleDateString('en-CA'),q=new URLSearchParams(location.search),val=k=>esc(q.get(k)||'');return `<section class="card"><h2>新增执行记录</h2><p class="small muted">记录实际或模拟建仓，不连接券商、不会自动下单。</p><form id="tradeForm" class="journal-form"><label>股票代码<input name="code" maxlength="6" placeholder="000001" value="${val('code')}" required></label><label>股票名称<input name="name" maxlength="40" placeholder="可选" value="${val('name')}"></label><label>信号日期<input name="signal_date" type="date" value="${val('signal_date')}"></label><label>建仓日期<input name="entry_date" type="date" value="${today}" required></label><label>建仓价<input name="entry_price" type="number" min="0.01" step="0.001" value="${val('entry_price')}" required></label><label>股数<input name="shares" type="number" min="1" step="1" value="${val('shares')}" required></label><label>初始止损<input name="initial_stop_loss" type="number" min="0.01" step="0.001" value="${val('stop_loss')}" required></label><label class="wide">复盘备注<textarea name="note" maxlength="300" placeholder="记录建仓依据、市场环境、执行偏差"></textarea></label><div class="wide"><button class="btn primary" type="submit">保存交易记录</button></div></form></section>`}
function rowHtml(x){const pnl=x.pnl==null?'-':`<span class="${x.pnl>=0?'pnl-up':'pnl-down'}">${x.pnl>=0?'+':''}${money(x.pnl)}</span>`;const result=x.status==='closed'?`${pnl}<br><span class="small">${x.return_pct==null?'-':x.return_pct+'%'} / ${x.r_multiple==null?'-':x.r_multiple+'R'}</span>`:`风险 ${money(x.current_risk)}${x.protected_profit>0?`<br><span class="pnl-up small">保护利润 ${money(x.protected_profit)}</span>`:''}`;const actions=x.status==='open'?`<button class="filter-btn" onclick="editRisk('${esc(x.id)}',${Number(x.current_stop_loss||0)})">改保护价</button><button class="filter-btn" onclick="closeTrade('${esc(x.id)}')">记录退出</button>`:'';return `<tr><td><b>${esc(x.code)}</b><br><span class="small muted">${esc(x.name||'-')}</span></td><td>${esc(x.entry_date)}<br><span class="small muted">信号 ${esc(x.signal_date||'-')}</span></td><td>${money(x.entry_price)} × ${Number(x.shares||0).toLocaleString()}</td><td>${money(x.initial_stop_loss)}<br><span class="small muted">当前 ${money(x.current_stop_loss)}</span></td><td>${result}</td><td class="journal-note">${esc(x.note||'-')}</td><td><div class="mini-actions">${actions}<button class="filter-btn" onclick="deleteTrade('${esc(x.id)}')">删除</button></div></td></tr>`}
function renderJournal(){const s=journal.summary||{},rows=tab==='open'?(journal.open||[]):(journal.closed||[]);document.getElementById('journalApp').innerHTML=`<section class="opening-hero"><div><span class="eyebrow">推荐 → 建仓 → 风控 → 退出 → 复盘</span><h1>交易复盘台</h1><p>把实际或模拟执行记录下来，用真实结果校验战法与执行纪律。</p></div><div class="journal-nav"><a class="btn" href="/">量化首页</a><a class="btn" href="/opening">早盘确认</a><button class="btn" id="lockBtn">清除令牌</button></div></section>${journal.warning?`<div class="journal-warn">${esc(journal.warning)}</div>`:''}<div class="journal-summary"><div class="journal-stat">当前持仓<b>${s.open_count||0}</b></div><div class="journal-stat">持仓成本<b>¥${money(s.open_cost)}</b></div><div class="journal-stat">当前止损风险<b>¥${money(s.current_risk)}</b></div><div class="journal-stat">已实现盈亏<b class="${Number(s.realized_pnl)>=0?'pnl-up':'pnl-down'}">¥${money(s.realized_pnl)}</b></div></div><div class="journal-grid">${formHtml()}<section class="card"><div class="category-head"><div><h2>${tab==='open'?'当前持仓':'已结束交易'}</h2><p>${tab==='open'?'及时上移保护价，但不要随意放宽初始风险。':'用收益率与R倍数复盘，不用单次盈亏评价战法。'}</p></div><span class="pill">更新 ${esc(journal.updated_at||'-')}</span></div><div class="journal-tabs"><button class="filter-btn ${tab==='open'?'active':''}" onclick="tab='open';renderJournal()">当前持仓 ${s.open_count||0}</button><button class="filter-btn ${tab==='closed'?'active':''}" onclick="tab='closed';renderJournal()">已结束 ${s.closed_count||0}</button></div><div class="journal-table">${rows.length?`<table><thead><tr><th>股票</th><th>日期</th><th>成本</th><th>止损/保护</th><th>风险/结果</th><th>备注</th><th>操作</th></tr></thead><tbody>${rows.map(rowHtml).join('')}</tbody></table>`:'<div class="journal-empty">暂无记录</div>'}</div></section></div><div class="footer">仅用于手动执行记录与复盘，不代表券商真实持仓，不会自动交易，也不构成投资建议。</div>`;document.getElementById('lockBtn').onclick=()=>{sessionStorage.removeItem('quant_token');showGate('管理令牌已清除')};document.getElementById('tradeForm').onsubmit=addTrade}
async function mutate(body){try{journal=await api('POST',body);renderJournal()}catch(e){if(e.message!=='unauthorized')alert(e.message)}}function addTrade(e){e.preventDefault();const o=Object.fromEntries(new FormData(e.target).entries());o.mode='add';mutate(o)}function editRisk(id,current){const v=prompt('输入新的当前保护价（可高于建仓价，表示移动止盈保护）',current);if(v!==null)mutate({mode:'update',id,current_stop_loss:v})}function closeTrade(id){const exit_date=prompt('退出日期 YYYY-MM-DD',new Date().toLocaleDateString('en-CA'));if(!exit_date)return;const exit_price=prompt('退出价格');if(exit_price)mutate({mode:'close',id,exit_date,exit_price})}function deleteTrade(id){if(confirm('确定删除这条交易复盘记录？此操作不可撤销。'))mutate({mode:'delete',id})}loadJournal();'''
    return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>交易复盘台｜A股量化驾驶舱</title><style>{STYLE}{extra}</style></head><body><div class="bg-orb one"></div><div class="bg-orb two"></div><main class="wrap" id="journalApp"></main><script>{js}</script></body></html>'''


def page_html(payload: Dict[str,Any]) -> str:
    data=script_json(payload)
    return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>A股量化推荐驾驶舱</title><style>{STYLE}</style></head><body><div class="bg-orb one"></div><div class="bg-orb two"></div><main class="wrap" id="app"></main><div class="modal" id="modal"><div class="modal-card"><button class="x" onclick="hideReport()">×</button><h2>Markdown报告</h2><pre id="reportText"></pre></div></div><div class="modal kline-modal" id="klineModal"><div class="modal-card kline-card"><button class="x kline-close" onclick="closeKline()">×</button><div class="kline-head"><div><span class="eyebrow">前复权行情 · 仅供技术复盘</span><h2 id="klineTitle">K线图</h2><div id="klineMeta" class="small muted"></div></div><div class="kline-actions"><button class="filter-btn active" data-period="day" onclick="setKlinePeriod('day')">日线</button><button class="filter-btn" data-period="week" onclick="setKlinePeriod('week')">周线</button><button class="filter-btn" data-period="month" onclick="setKlinePeriod('month')">月线</button><button class="filter-btn" id="klineFullscreenBtn" onclick="toggleKlineFullscreen()">全屏</button><button class="filter-btn" id="klineRefreshBtn" onclick="refreshKlineNow()">刷新行情</button><label class="kline-auto"><input id="klineAuto" type="checkbox" checked onchange="setKlineAutoRefresh(this.checked)">自动刷新</label><button class="filter-btn" onclick="klineToFundamental()">查看基本面</button></div></div><div class="kline-tools"><div id="klineMa" class="kline-ma"></div><div class="kline-range"><span class="small muted">历史范围</span><button class="filter-btn" data-limit="120" onclick="setKlineLimit(120)">120根</button><button class="filter-btn active" data-limit="240" onclick="setKlineLimit(240)">240根</button><button class="filter-btn" data-limit="480" onclick="setKlineLimit(480)">480根</button><button class="filter-btn" data-limit="640" onclick="setKlineLimit(640)">640根</button><span class="kline-divider"></span><button class="filter-btn" onclick="panKline(-1)">← 较早</button><button class="filter-btn" onclick="zoomKline(.7)">＋ 放大</button><button class="filter-btn" onclick="zoomKline(1.4)">－ 缩小</button><button class="filter-btn" onclick="panKline(1)">较新 →</button><button class="filter-btn" onclick="resetKlineView()">回到最新</button></div></div><div class="kline-status"><div id="klineInfo" class="kline-info">点击代码后加载K线数据</div><div class="kline-status-right"><div id="klineViewport" class="small muted"></div><div id="klineRefreshStatus" class="kline-refresh-status">等待刷新计划</div></div></div><div class="kline-canvas-wrap"><canvas id="klineCanvas" aria-label="股票K线图"></canvas><div id="klineLoading" class="kline-loading">正在加载K线…</div></div><div class="small muted kline-note" id="klineNote">红色为上涨，绿色为下跌；均线随所选日/周/月周期计算。鼠标滚轮可缩放，按住图表左右拖拽可浏览历史，方向键也可平移。</div></div></div><script>window.__DATA__={data};{SCRIPT}</script></body></html>'''


def opening_detail_page_html(payload: Dict[str, Any]) -> str:
    def esc(value: Any) -> str:
        return html.escape(str(value if value not in (None, "") else "-"))
    def price(value: Any) -> str:
        number = nullable_float(value, 4)
        return "-" if number is None or number <= 0 else f"{number:.2f}"
    def change(value: Any) -> str:
        number = nullable_float(value, 2)
        if number is None:
            return '<span class="muted">基准价未记录</span>'
        klass = "up" if number > 0 else ("down" if number < 0 else "flat")
        return f'<span class="{klass}">{number:+.2f}%</span>'
    if not payload.get("ok"):
        return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>核验详情｜A股量化驾驶舱</title><style>{STYLE}.detail-wrap{{max-width:900px;margin:46px auto;padding:0 18px}}.detail-empty{{padding:28px;border:1px solid #35516e;border-radius:16px;background:#0b1d31;color:#d8eaff}}</style></head><body><main class="detail-wrap"><section class="detail-empty"><h1>开盘核验详情</h1><p>{esc(payload.get("message"))}</p><p><a class="btn" href="/opening">返回开盘建仓确认</a></p></section></main></body></html>'''
    rows = payload.get("rows") or []
    cards: List[str] = []
    for row in rows:
        baseline = row.get("monitor_baseline") if isinstance(row.get("monitor_baseline"), dict) else {}
        snapshot = row.get("quote") if isinstance(row.get("quote"), dict) else {}
        current = row.get("current_quote") if isinstance(row.get("current_quote"), dict) else {}
        checks = row.get("checks") if isinstance(row.get("checks"), list) else []
        checks_html = "".join(f'<li class="{"pass" if item.get("pass") else "fail"}"><b>{"✓" if item.get("pass") else "×"}</b> {esc(item.get("name"))}：{esc(item.get("text"))}</li>' for item in checks if isinstance(item, dict)) or '<li class="muted">未保存条件明细</li>'
        current_time = current.get("trade_time") or current.get("fetched_at") or "-"
        current_source = current.get("source") or "当前行情未就绪"
        stale_note = "（可能延迟）" if current.get("stale") else ""
        cards.append(f'''<article class="detail-card">
          <div class="detail-head"><div><button type="button" class="code" onclick="openDetailKline(&quot;{esc(row.get("code"))}&quot;)">{esc(row.get("code"))}</button> <b>{esc(row.get("name"))}</b><p>{esc(row.get("status"))}｜评分 {safe_float(row.get("score")):.1f}｜{esc(row.get("strategy_group"))} · {esc(row.get("strategy"))}</p></div><span class="status">{esc(row.get("status"))}</span></div>
          <div class="price-grid">
            <section><small>加入监控基准价</small><strong>{price(row.get("baseline_price"))}</strong><em>{esc(baseline.get("added_at") or "未记录加入时间")}</em><p>{esc(baseline.get("source") or "旧快照未保存")}｜{esc(baseline.get("price_source") or "-")}</p></section>
            <section><small>本次核验价（历史快照）</small><strong>{price(row.get("snapshot_price"))}</strong><em>相对基准 {change(row.get("snapshot_change_pct"))}</em><p>{esc(snapshot.get("trade_time") or payload.get("checked_at"))}｜{esc(snapshot.get("source"))}</p></section>
            <section><small>当前行情（打开详情时获取）</small><strong>{price(row.get("current_price"))}</strong><em>相对基准 {change(row.get("current_change_pct"))}</em><p>{esc(current_time)}｜{esc(current_source)} {esc(stale_note)}</p></section>
          </div>
          <div class="detail-body"><section><h3>本次建仓结论</h3><p>{esc(row.get("reason"))}</p><p class="muted">原计划：{esc(row.get("action"))}｜计划买入区：{esc(row.get("buy_zone"))}｜止损：{price(row.get("stop_loss"))}</p></section><section><h3>条件校验</h3><ul>{checks_html}</ul></section></div>
        </article>''')
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>核验详情｜A股量化驾驶舱</title><style>{STYLE}
    .detail-wrap{{max-width:1380px;margin:0 auto;padding:28px 18px 55px}}.detail-hero{{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin:8px 0 18px}}.detail-hero h1{{margin:0 0 8px;font-size:30px}}.detail-hero p{{margin:0;color:var(--muted);line-height:1.7}}.detail-note{{border:1px solid #2d5a7d;background:#092039;color:#cce9ff;padding:13px 15px;border-radius:13px;margin-bottom:16px;line-height:1.65}}.detail-card{{border:1px solid #294966;background:#0b1b2e;border-radius:18px;padding:17px;margin:14px 0;box-shadow:0 12px 28px #00000022}}.detail-head{{display:flex;justify-content:space-between;gap:12px;border-bottom:1px solid #213f5d;padding-bottom:12px;margin-bottom:13px}}.detail-head p{{margin:7px 0 0;color:#9eb7cf;font-size:13px}}.code{{border:1px solid #4aa1ce;background:#0e3150;color:#a9ecff;padding:4px 8px;border-radius:8px;font-weight:900;cursor:pointer}}.status{{border:1px solid #365b7e;color:#d7efff;border-radius:999px;padding:5px 10px;height:max-content;font-size:13px;font-weight:850}}.price-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:11px}}.price-grid section{{background:#08182a;border:1px solid #244765;border-radius:13px;padding:12px}}.price-grid small{{display:block;color:#9cb5cc;font-weight:800}}.price-grid strong{{display:block;font-size:25px;color:#f5fbff;margin:7px 0 4px}}.price-grid em{{font-style:normal;font-size:13px}}.price-grid p{{font-size:12px;color:#89a6c0;line-height:1.55;margin:7px 0 0}}.up{{color:#ff7979;font-weight:900}}.down{{color:#4ed6a1;font-weight:900}}.flat{{color:#d3deea;font-weight:900}}.detail-body{{display:grid;grid-template-columns:1.25fr 1fr;gap:14px;margin-top:14px}}.detail-body section{{border-left:3px solid #3b92c7;background:#09182a;border-radius:0 11px 11px 0;padding:10px 12px}}.detail-body h3{{font-size:14px;margin:0 0 9px}}.detail-body p{{font-size:13px;line-height:1.65;margin:0 0 8px}}.detail-body ul{{list-style:none;margin:0;padding:0;display:grid;gap:6px;font-size:12px;line-height:1.55}}.pass b{{color:#48d49e}}.fail b{{color:#ff7777}}@media(max-width:820px){{.detail-hero,.detail-head{{flex-direction:column}}.price-grid,.detail-body{{grid-template-columns:1fr}}}}
    </style></head><body><main class="detail-wrap"><section class="detail-hero"><div><span class="eyebrow">历史快照 · 当前行情对比</span><h1>开盘建仓核验详情</h1><p>核验时间：{esc(payload.get("checked_at"))}｜收盘信号日：{esc(payload.get("source_date"))}｜共 {safe_float(summary.get("total")):.0f} 只</p></div><div><a class="btn" href="/opening">返回开盘确认</a> <a class="btn" href="/">量化驾驶舱</a></div></section><div class="detail-note">{esc(payload.get("message"))} 若“加入监控基准价”显示未记录，说明该股票来自旧版监控清单或当时行情未能取得，系统不会用今天价格倒推伪造历史基准。</div>{''.join(cards) or '<section class="detail-note">该次快照未保存可展示的股票明细。</section>'}</main><script>function openDetailKline(code){{const clean=String(code||'').replace(/\\D/g,'').slice(0,6);if(!/^(?:00|30|60|68)\\d{{4}}$/.test(clean))return;window.open('/?kline='+encodeURIComponent(clean),'_blank','noopener')}}</script></body></html>'''

def opening_page_html(payload: Dict[str, Any]) -> str:
    def esc(v: Any) -> str:
        return html.escape(str(v if v not in (None, "") else "-"))

    def money(v: Any) -> str:
        return "-" if v is None else f"{safe_float(v):.2f}"

    session = payload.get("session") or {}
    summary = payload.get("summary") or {}
    rows = payload.get("rows") or []
    watch = payload.get("watchlist") or {}
    history = payload.get("history") or []
    opening_rows = [{
        "code": str(r.get("code") or ""),
        "name": str(r.get("name") or ""),
        "status": str(r.get("status") or ""),
        "score": safe_float(r.get("score")),
        "strategy_group": str(r.get("strategy_group") or ""),
        "strategy": str(r.get("strategy") or ""),
        "price": nullable_float((r.get("quote") or {}).get("price"), 3),
        "zone_low": nullable_float(r.get("zone_low"), 3),
        "buy_zone": str(r.get("buy_zone") or "-"),
        "stop_loss": nullable_float(r.get("stop_loss"), 3),
        "reason": str(r.get("reason") or ""),
    } for r in rows]

    watch_script = (
        "<script>window.__OPEN_SOURCE_DATE__=" + script_json(payload.get("source_date") or "") + ";const openWatch=" + script_json(watch)
        + ";const openRows=" + script_json(opening_rows) + r""";
try{localStorage.removeItem('quant_token')}catch(e){}
const openEsc=v=>String(v??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
function getAdminToken(){try{return sessionStorage.getItem('quant_token')||''}catch(e){return ''}}
function setAdminToken(v){try{sessionStorage.setItem('quant_token',v)}catch(e){}}
function clearAdminToken(){try{sessionStorage.removeItem('quant_token')}catch(e){}alert('本标签页的管理令牌已清除')}
function openOpeningKline(code){const clean=String(code||'').replace(/\D/g,'').slice(0,6);if(!/^(?:00|30|60|68)\d{4}$/.test(clean)){alert('仅支持沪深A股六位代码');return}window.open('/?kline='+encodeURIComponent(clean),'_blank','noopener')}
function openWatchHeaders(){const h={'Content-Type':'application/json'};const token=getAdminToken();if(token)h['X-Quant-Token']=token;return h}
async function openWatchSave(mode,codes){let r=await fetch('/api/watchlist',{method:'POST',headers:openWatchHeaders(),body:JSON.stringify({mode:mode,codes:codes})});if(r.status===401){const t=prompt('请输入管理令牌');if(!t)return;setAdminToken(t);return openWatchSave(mode,codes)}const d=await r.json();if(!r.ok||!d.ok){alert(d.message||'更新失败');return}location.reload()}
function addWatch(){const value=document.getElementById('watchCode').value;const codes=value.split(/[,，\s]+/).filter(Boolean);if(!codes.length){alert('请输入沪深A股六位代码');return}openWatchSave('add',codes)}
function removeWatch(code){openWatchSave('remove',[code])}
function clearWatch(){if(confirm('清空后将隐藏当前自动候选，下一交易日会按开关重新同步，是否继续？'))openWatchSave('replace',[])}
function journalFromOpening(code){const r=openRows.find(x=>x.code===code);if(!r)return;const plan=(buildPortfolioPlans().plans||{})[code]||calculatePlan(r);const p=new URLSearchParams({code:r.code,name:r.name||'',signal_date:String(window.__OPEN_SOURCE_DATE__||''),entry_price:String(r.price||r.zone_low||''),shares:String(plan.shares||''),stop_loss:String(r.stop_loss||'')});location.href='/positions?'+p.toString()}
async function setOpenAutoSync(enabled){let r=await fetch('/api/watchlist',{method:'POST',headers:openWatchHeaders(),body:JSON.stringify({mode:'auto_sync',auto_sync:{enabled:enabled}})});if(r.status===401){const t=prompt('请输入管理令牌');if(!t){document.getElementById('openAutoSync').checked=!enabled;return}setAdminToken(t);return setOpenAutoSync(enabled)}const d=await r.json();if(!r.ok||!d.ok){alert(d.message||'设置失败');document.getElementById('openAutoSync').checked=!enabled;return}location.reload()}
function riskNumber(id,fallback){const n=Number(document.getElementById(id)?.value);return Number.isFinite(n)&&n>0?n:fallback}
function riskSettings(){return {capital:riskNumber('riskCapital',100000),riskPct:riskNumber('riskPct',0.5),maxPosPct:riskNumber('maxPosPct',20),portfolioPct:riskNumber('portfolioPct',60),portfolioRiskPct:riskNumber('portfolioRiskPct',2)}}
function saveRiskSettings(){const s=riskSettings();try{sessionStorage.setItem('opening_risk_settings',JSON.stringify(s))}catch(e){}renderPositionPlans()}
function loadRiskSettings(){try{const s=JSON.parse(sessionStorage.getItem('opening_risk_settings')||'{}');for(const k of ['capital','riskPct','maxPosPct','portfolioPct','portfolioRiskPct'])if(s[k]&&document.getElementById(k==='capital'?'riskCapital':k))document.getElementById(k==='capital'?'riskCapital':k).value=s[k]}catch(e){}}
function calculatePlan(row){const s=riskSettings();const price=Number(row.price||row.zone_low||0);const stop=Number(row.stop_loss||0);if(!(price>0&&stop>0&&price>stop))return {shares:0,amount:0,loss:0,price:price,note:'价格或止损无效'};const perShare=price-stop;const riskShares=Math.floor((s.capital*s.riskPct/100)/perShare/100)*100;const positionShares=Math.floor((s.capital*s.maxPosPct/100)/price/100)*100;const shares=Math.max(0,Math.min(riskShares,positionShares));return {shares:shares,amount:shares*price,loss:shares*perShare,price:price,note:shares?'按风险预算与仓位上限取较小值':'资金或风险预算不足一手'}}
function buildPortfolioPlans(){const s=riskSettings();let remainingAmount=s.capital*s.portfolioPct/100,remainingRisk=s.capital*s.portfolioRiskPct/100,totalAmount=0,totalLoss=0,eligible=0,allocated=0;const plans={};for(const row of openRows){let p=calculatePlan(row);if(row.status==='可计划内执行'){eligible++;if(p.shares>0){const perShare=p.price-Number(row.stop_loss||0);const byAmount=Math.floor(remainingAmount/p.price/100)*100;const byRisk=perShare>0?Math.floor(remainingRisk/perShare/100)*100:0;const shares=Math.max(0,Math.min(p.shares,byAmount,byRisk));p={...p,shares:shares,amount:shares*p.price,loss:shares*perShare,note:shares?p.note:'达到组合总仓位或总风险上限'};remainingAmount=Math.max(0,remainingAmount-p.amount);remainingRisk=Math.max(0,remainingRisk-p.loss);totalAmount+=p.amount;totalLoss+=p.loss;if(shares)allocated++}}plans[row.code]=p}return {plans,totalAmount,totalLoss,eligible,allocated,settings:s}}
function renderPositionPlans(){const portfolio=buildPortfolioPlans();for(const row of openRows){const el=document.getElementById('position-'+row.code);if(!el)continue;const p=portfolio.plans[row.code]||calculatePlan(row);const prefix=row.status==='可计划内执行'?'':'仅作预案 · ';el.innerHTML=p.shares?`<b>${prefix}${p.shares} 股</b><br><span class="small muted">约 ¥${p.amount.toFixed(0)}｜止损风险约 ¥${p.loss.toFixed(0)}</span>`:`<span class="small muted">${prefix}${openEsc(p.note)}</span>`}const summary=document.getElementById('portfolioSummary');if(summary){const s=portfolio.settings;const warning=portfolio.allocated<portfolio.eligible?'；部分候选因组合上限未分配仓位':'';summary.innerHTML=`组合预案：<b>${portfolio.allocated}/${portfolio.eligible}</b> 只可执行候选获得仓位｜预计占用 <b>¥${portfolio.totalAmount.toFixed(0)}</b> / ¥${(s.capital*s.portfolioPct/100).toFixed(0)}｜止损风险 <b>¥${portfolio.totalLoss.toFixed(0)}</b> / ¥${(s.capital*s.portfolioRiskPct/100).toFixed(0)}${warning}`}}
function csvSafe(v){let s=String(v??'').replace(/\r?\n/g,' ');if(/^[=+\-@]/.test(s))s="'"+s;return '"'+s.replace(/"/g,'""')+'"'}
function exportOpeningCsv(){if(!openRows.length){alert('当前没有可导出的盘中核验结果');return}const head=['代码','名称','状态','规则评分','战法分类','战法','现价','买入区间','止损','组合仓位股数','预计占用资金','预计止损风险','执行理由'];const portfolio=buildPortfolioPlans();const data=openRows.map(r=>{const p=portfolio.plans[r.code]||calculatePlan(r);return [r.code,r.name,r.status,r.score,r.strategy_group,r.strategy,r.price??'',r.buy_zone,r.stop_loss??'',p.shares,p.amount.toFixed(2),p.loss.toFixed(2),r.reason]});const csv='\ufeff'+[head,...data].map(x=>x.map(csvSafe).join(',')).join('\r\n');const blob=new Blob([csv],{type:'text/csv;charset=utf-8'});const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='opening_check_'+new Date().toISOString().slice(0,10)+'.csv';document.body.appendChild(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(a.href),1000)}
(function(){const hero=document.querySelector('.opening-hero');if(!hero)return;const chips=(openWatch.items||[]).map(x=>`<span class="watch-chip"><button type="button" class="opening-code-link chip-code-link" data-opening-code="${openEsc(x.code)}" title="在新标签页打开K线图">${openEsc(x.code)}</button> ${openEsc(x.name)}<small>${openEsc(x.source)}</small><button class="watch-remove" title="移除监控" aria-label="移除 ${openEsc(x.code)}" onclick="removeWatch('${openEsc(x.code)}')">×</button></span>`).join('')||'<span class="muted">尚未选择；此时默认核验昨日高分候选。</span>';hero.insertAdjacentHTML('afterend',`<section class="card opening-watch" style="margin-bottom:16px"><div class="category-head"><div><h2>我的早盘建仓监控</h2><p>首页可勾选昨日推荐加入监控；也可在这里添加任意沪深 A 股六位代码。自选清单不为空时，本页仅核验清单内股票。</p></div><span class="pill">${openWatch.count||0}/${openWatch.max||100} 只</span></div><div class="watch-chips">${chips}</div><div class="watch-add"><input id="watchCode" class="fundamental-input" maxlength="80" placeholder="输入代码，如 000938；多个代码用逗号分隔"><button class="btn primary" onclick="addWatch()">加入监控</button><button class="btn" onclick="clearWatch()">清空手动监控</button><button class="btn" onclick="clearAdminToken()">清除本页令牌</button><label class="monitor-auto"><input id="openAutoSync" type="checkbox" ${(openWatch.auto_sync||{}).enabled?'checked':''} onchange="setOpenAutoSync(this.checked)"> 自动同步收盘高分股</label></div><div class="small muted" style="margin-top:8px">开启后：每次收盘扫描成功，会自动同步前 ${(openWatch.auto_sync||{}).top_n||12} 只高分候选（纯涨停情绪类除外）；手动监控股票不会被覆盖。管理令牌仅保存在当前标签页；公网管理操作请优先使用 HTTPS。</div></section>`);document.querySelectorAll('[data-opening-code]').forEach(btn=>btn.addEventListener('click',()=>openOpeningKline(btn.dataset.openingCode)));document.getElementById('watchCode').addEventListener('keydown',e=>{if(e.key==='Enter')addWatch()});loadRiskSettings();renderPositionPlans()})();
</script>"""
    )

    status_class = {"可计划内执行": "ok", "等待确认": "wait", "等待人工确认": "wait", "不建议参与": "no", "不建议追高": "no"}
    row_html = []
    for r in rows:
        q = r.get("quote") or {}
        checks = r.get("checks") or []
        check_html = "<br>".join(f'<span class="{"yes" if c.get("pass") else "no"}"><i>{"✓" if c.get("pass") else "×"}</i>{esc(c.get("name"))}</span>：{esc(c.get("text"))}' for c in checks)
        cls = status_class.get(r.get("status"), "na")
        gap = q.get("gap_pct")
        gap_text = "-" if gap is None else f"{safe_float(gap):+.2f}%"
        pct_text = "-" if q.get("pct") is None else f"{safe_float(q.get('pct')):+.2f}%"
        code = str(r.get("code") or "")
        row_html.append(f'<tr><td><button type="button" class="opening-code-link" title="在新标签页打开日线、周线、月线和均线" onclick="openOpeningKline(&quot;{esc(code)}&quot;)">{esc(code)}</button><br><span class="muted small">{esc(r.get("name"))} · {esc(r.get("source"))}</span></td><td><span class="opening-badge {cls}">{esc(r.get("status"))}</span><br><span class="muted small">规则评分 {safe_float(r.get("score")):.1f}｜{esc(r.get("level"))}</span></td><td>{esc(r.get("strategy_group"))}<br><span class="small muted">{esc(r.get("strategy"))}</span></td><td>{money(q.get("price"))}<br><span class="small muted">涨跌 {pct_text}｜{esc(q.get("source"))}<br>{esc(q.get("trade_time"))}</span></td><td>开 {money(q.get("open"))}<br><span class="small muted">高开 {gap_text}</span></td><td>{esc(r.get("buy_zone"))}<br><span class="small muted">止损 {money(r.get("stop_loss"))}</span></td><td id="position-{esc(code)}" class="position-plan"><span class="small muted">计算中</span></td><td class="opening-checks">{check_html or "-"}</td><td class="opening-reason">{esc(r.get("reason"))}<br><span class="small muted">原计划：{esc(r.get("action"))}</span><br><button class="filter-btn" onclick="journalFromOpening(&quot;{esc(code)}&quot;)">带入交易复盘</button></td></tr>')
    table = "".join(row_html) or '<div class="opening-empty">当前未进入盘中核验时段，因此不读取实时行情、不输出建仓名单。<br>请在交易日 09:35 之后打开或刷新本页。</div>'

    history_rows = "".join(
        f'<tr><td>{esc(x.get("checked_at"))}</td><td>{esc(x.get("source_date"))}</td><td>{safe_float(x.get("total")):.0f}</td><td class="ok-text">{safe_float(x.get("eligible")):.0f}</td><td>{safe_float(x.get("wait")):.0f}</td><td>{safe_float(x.get("avoid")):.0f}</td><td>{safe_float(x.get("unready")):.0f}</td><td><a class="filter-btn" href="/opening/detail?file={quote(str(x.get("file") or ""))}">查看详情</a></td></tr>'
        for x in history
    ) or '<tr><td colspan="8" class="muted">最近三天尚无自动核验快照；交易日盘中定时任务执行后会在这里留下轨迹。</td></tr>'


    active = bool(session.get("active"))
    auto_refresh = "setTimeout(()=>location.reload(),120000);" if active else ""
    monitor_desc = (f"当前按自选监控清单核验 {safe_float(watch.get('count')):.0f} 只股票，包含昨日推荐与手动添加代码。" if payload.get("monitor_mode") == "自选监控" else f"当前未设置自选清单，默认核验昨日评分靠前的 {safe_float(payload.get('limit')):.0f} 只候选。")
    return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>开盘建仓确认｜A股量化驾驶舱</title><style>{STYLE}</style></head><body><div class="bg-orb one"></div><div class="bg-orb two"></div><main class="wrap"><section class="opening-hero"><div><span class="eyebrow">前一交易日信号 · 盘中价格确认</span><h1>开盘建仓确认</h1><p>只核验昨天收盘后的候选是否仍满足计划价格与风险条件；系统不会自动下单。</p></div><div class="opening-action"><a class="btn" href="/">返回量化驾驶舱</a><a class="btn" href="/positions">交易复盘台</a><button class="btn primary" onclick="location.reload()">立即刷新</button></div></section><section class="card" style="margin-bottom:16px"><div class="category-head"><div><h2>{esc(session.get("label"))}</h2><p>核验时点：{esc(payload.get("checked_at") or session.get("time"))}｜信号数据日：{esc(payload.get("source_date"))}</p></div><div class="opening-status"><b>自动核验</b><br>交易日 {esc(payload.get("schedule"))}<br><span class="muted">{esc(session.get("next_action"))}</span></div></div><div class="opening-grid"><div class="opening-kpi"><div class="t">核验候选</div><div class="n">{safe_float(summary.get("total")):.0f}</div></div><div class="opening-kpi"><div class="t">可计划内执行</div><div class="n" style="color:#74f0b9">{safe_float(summary.get("eligible")):.0f}</div></div><div class="opening-kpi"><div class="t">等待确认</div><div class="n" style="color:#ffe28b">{safe_float(summary.get("wait")):.0f}</div></div><div class="opening-kpi"><div class="t">不建议参与/追高</div><div class="n" style="color:#ff9ba8">{safe_float(summary.get("avoid")):.0f}</div></div></div><div class="opening-note"><b>保守多条件确认模型：</b>必须同时通过计划价格区间、开盘溢价、止损、开盘承接、昨日评分、RPS60/风险标签等门槛。情绪/涨停类信号只显示“等待人工确认”；手动添加的股票还需通过 MA20/MA60、多头斜率、量能和乖离门槛。任一关键条件失败就等待或放弃，不为凑数量降低标准。{esc(payload.get("message"))}</div></section><section class="card risk-planner"><div class="category-head"><div><h2>早盘风险预算与仓位预案</h2><p>按账户风险预算和单股仓位上限计算整手数量，取两种限制中的较小值。参数只保存在当前浏览器标签页。</p></div><button class="btn" onclick="exportOpeningCsv()">导出当前核验CSV</button></div><div class="risk-controls"><label>账户资金（元）<input id="riskCapital" type="number" min="1000" step="1000" value="100000" oninput="saveRiskSettings()"></label><label>单笔最大风险（%）<input id="riskPct" type="number" min="0.1" max="10" step="0.1" value="0.5" oninput="saveRiskSettings()"></label><label>单股仓位上限（%）<input id="maxPosPct" type="number" min="1" max="100" step="1" value="20" oninput="saveRiskSettings()"></label><label>组合总仓位上限（%）<input id="portfolioPct" type="number" min="1" max="100" step="1" value="60" oninput="saveRiskSettings()"></label><label>组合总止损风险（%）<input id="portfolioRiskPct" type="number" min="0.1" max="20" step="0.1" value="2" oninput="saveRiskSettings()"></label></div><div id="portfolioSummary" class="portfolio-summary">组合预案等待盘中核验结果</div><div class="small muted">系统按页面排序依次分配仓位，同时受单股风险、单股仓位、组合总仓位和组合总止损风险约束。只有“可计划内执行”的股票占用组合预算；其余股票仅作比较预案。</div></section><section class="card"><div class="category-head"><div><h2>盘中核验明细</h2><p>{esc(monitor_desc)} 状态为“可计划内执行”也不代表保证收益，实际执行仍应采用预设止损与风险预算。</p></div><span class="pill">公开行情快照</span></div><div class="opening-table">{('<table><thead><tr><th>股票</th><th>建仓状态</th><th>战法</th><th>现价</th><th>开盘表现</th><th>昨日计划</th><th>仓位预案</th><th>条件校验</th><th>执行说明</th></tr></thead><tbody>'+table+'</tbody></table>') if rows else table}</div></section><section class="card opening-history"><div class="category-head"><div><h2>最近三日自动核验轨迹 <span class="pill">近3日 {len(history)} 次</span></h2><p>每次定时核验都会保存逐股详情；仅保留最近三个自然日，可查看加入监控基准、本次快照和当前行情的涨跌对比。</p></div><span class="pill">最近3日 {len(history)} 次</span></div><div class="opening-table"><table><thead><tr><th>核验时间</th><th>信号日</th><th>总数</th><th>可执行</th><th>等待</th><th>回避</th><th>行情未就绪</th><th>详情</th></tr></thead><tbody>{history_rows}</tbody></table></div></section><div class="footer">免责声明：此页面仅把前一交易日量化候选与公开盘中行情进行规则核验，不构成投资建议，也不提供自动下单服务。请自行判断并严格执行风险控制。</div></main>{watch_script}<script>{auto_refresh}</script></body></html>'''
STYLE = r'''
:root{--bg:#050914;--panel:#0d1628;--panel2:#111e34;--text:#eaf2ff;--muted:#91a7c6;--line:#223957;--cyan:#58dcff;--blue:#7fa6ff;--green:#42d392;--yellow:#ffd166;--red:#ff6b6b;--purple:#b794ff}*{box-sizing:border-box}body{margin:0;min-height:100vh;background:linear-gradient(140deg,#050914,#091426 45%,#111827);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",Arial,sans-serif}.bg-orb{position:fixed;border-radius:999px;filter:blur(40px);opacity:.45;pointer-events:none}.bg-orb.one{width:430px;height:430px;background:#1262ff;left:-120px;top:-100px}.bg-orb.two{width:360px;height:360px;background:#06b6d4;right:2%;top:120px}.wrap{max-width:1500px;margin:0 auto;padding:26px;position:relative}.hero{display:grid;grid-template-columns:1fr auto;gap:18px;align-items:start;margin-bottom:18px}.eyebrow{display:inline-flex;gap:8px;align-items:center;border:1px solid #245179;background:#0b223b;padding:7px 10px;border-radius:999px;color:#b9eaff;font-weight:800;font-size:12px}.title h1{font-size:38px;letter-spacing:-1px;margin:12px 0 6px}.title p{color:var(--muted);margin:0;line-height:1.7}.actions{display:flex;gap:10px;justify-content:flex-end;flex-wrap:wrap}.btn{border:1px solid #2c4a71;background:#11233d;color:#eff7ff;padding:11px 15px;border-radius:14px;cursor:pointer;text-decoration:none;font-weight:900;box-shadow:0 10px 24px #0004}.btn:hover{transform:translateY(-1px);background:#173458}.btn.primary{border:0;background:linear-gradient(90deg,#1677ff,#06b6d4)}.grid{display:grid;grid-template-columns:minmax(0,1.42fr) 410px;gap:16px}.card{background:linear-gradient(180deg,rgba(17,31,54,.90),rgba(8,16,29,.92));border:1px solid var(--line);border-radius:24px;padding:18px;box-shadow:0 22px 55px #0007;backdrop-filter:blur(14px)}.card h2,.card h3{margin:0 0 12px}.kpis{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin:16px 0}.kpi{background:linear-gradient(180deg,#142844,#0e1b30);border:1px solid #274260;border-radius:18px;padding:14px;min-height:86px}.kpi .label{font-size:12px;color:var(--muted)}.kpi .value{font-size:25px;font-weight:1000;margin-top:8px}.recommend{font-size:18px;line-height:1.75;padding:17px;border:1px solid #26729e;background:linear-gradient(135deg,#1570ef35,#06b6d425);border-radius:18px}.scope{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}.scope div{border:1px solid #25415f;background:#091a2d;border-radius:14px;padding:12px;color:#cfe1f7}.category-panel{margin:0 0 18px}.category-head{display:flex;justify-content:space-between;gap:12px;align-items:end;margin:0 2px 10px}.category-head h2{margin:0;font-size:19px}.category-head p{margin:3px 0 0;color:var(--muted);font-size:13px;line-height:1.55}.category-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.category-card{position:relative;text-align:left;border:1px solid #294b70;background:linear-gradient(145deg,#102743,#0b1729);color:var(--text);border-radius:17px;padding:13px;cursor:pointer;transition:.16s}.category-card:hover,.category-card.active{transform:translateY(-2px);border-color:#59d7ff;background:linear-gradient(145deg,#123b63,#102238);box-shadow:0 12px 24px #0005}.category-card .cat-name{font-weight:950;font-size:15px}.category-card .cat-count{position:absolute;right:12px;top:12px;color:#80efd0;font-size:22px;font-weight:950}.category-card .cat-items{color:#9eb6d2;font-size:12px;line-height:1.55;margin:8px 34px 0 0;min-height:37px}.category-card .cat-note{color:#ffc96d;font-size:11px;margin-top:6px}.strategy-panel{margin:0 0 18px}.strategy-groups{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.strategy-group{border:1px solid #294b70;background:#09182b;border-radius:16px;padding:12px}.strategy-group-title{font-size:13px;font-weight:950;color:#8edfff;margin-bottom:9px}.strategy-chips{display:flex;flex-wrap:wrap;gap:7px}.strategy-chip{border:1px solid #305276;background:#0d223b;color:#dcecff;border-radius:999px;padding:7px 10px;cursor:pointer;font-size:12px;font-weight:800}.strategy-chip:hover,.strategy-chip.active{background:#1677ff;border-color:#6ce4ff;color:#fff}.strategy-chip em{font-style:normal;opacity:.74;margin-left:4px}.feedback-note{margin:0 0 11px;color:#a9c5e4;font-size:12px;line-height:1.65}.feedback-table{overflow:auto;border:1px solid #284968;border-radius:14px;max-height:300px}.feedback-table table{min-width:860px}.feedback-table th,.feedback-table td{padding:9px 10px}.feedback-up{color:#53e0a0;font-weight:900}.feedback-down{color:#ff9b9b;font-weight:900}.exec-stage{display:inline-block;border:1px solid #4675a0;background:#153052;border-radius:999px;padding:4px 7px;font-size:11px;color:#d7ecff;font-weight:900}.tactic-table{margin-top:12px}.tabs{display:flex;gap:8px;flex-wrap:wrap;margin:15px 0}.tab{padding:8px 12px;border:1px solid #2b4770;background:#0d1d33;border-radius:999px;color:#cfe2ff;cursor:pointer;font-weight:800}.tab.active{background:linear-gradient(90deg,#1d5d91,#1677ff);color:white;border-color:#58dcff}.toolbar{display:flex;gap:10px;align-items:center;justify-content:space-between;flex-wrap:wrap;margin:12px 0}.search{background:#09182b;border:1px solid #2b4770;color:white;border-radius:13px;padding:11px;min-width:280px}.switches{display:flex;gap:8px;flex-wrap:wrap}.pill{display:inline-flex;gap:6px;align-items:center;border:1px solid #2a466a;background:#0a1b30;border-radius:999px;padding:7px 10px;color:#c8dbf5;font-size:12px;font-weight:800}.table-wrap{overflow:auto;border-radius:16px;border:1px solid var(--line);max-height:670px}table{width:100%;border-collapse:collapse;min-width:1700px;background:#081426}th,td{padding:11px 10px;border-bottom:1px solid #1d314e;white-space:nowrap;text-align:left}th{position:sticky;top:0;background:#132641;color:#bfd1ed;font-size:12px;z-index:1}tr:hover{background:#10223a}.reason{max-width:380px;overflow:hidden;text-overflow:ellipsis}.badge{display:inline-block;padding:4px 8px;border-radius:999px;font-size:12px;font-weight:900}.strong{background:#2ad78226;color:var(--green);border:1px solid #42d39266}.good{background:#7aa2ff22;color:#b8c9ff;border:1px solid #7aa2ff66}.watch{background:#ffd16622;color:var(--yellow);border:1px solid #ffd16666}.risk{background:#ff6b6b22;color:#ffaaaa;border:1px solid #ff6b6b66}.side{display:flex;flex-direction:column;gap:16px}.leader{display:grid;gap:10px}.leader-card{border:1px solid #284463;border-radius:16px;background:#0a1a2d;padding:13px}.leader-card b{font-size:15px}.muted{color:var(--muted)}.small{font-size:12px;line-height:1.75}.book{display:grid;gap:10px}.book-item{border:1px solid #263f60;background:linear-gradient(180deg,#0b1d33,#081729);border-radius:16px;padding:12px}.book-item .items{color:#dcecff;font-weight:900}.bar{height:8px;background:#0a1728;border-radius:999px;overflow:hidden;margin-top:8px}.bar i{display:block;height:100%;background:linear-gradient(90deg,#42d392,#58dcff);border-radius:999px}.hist{display:grid;gap:8px}.hist-row{display:flex;justify-content:space-between;gap:8px;border:1px solid #243c5a;background:#0a1a2e;border-radius:12px;padding:9px 10px}.footer{margin:18px 0;color:#7890af;font-size:12px;line-height:1.7}.modal{position:fixed;inset:0;background:#000c;display:none;align-items:center;justify-content:center;padding:24px;z-index:1000}.modal.show{display:flex}.modal-card{width:min(1100px,96vw);max-height:86vh;overflow:auto;background:#0c1729;border:1px solid #2b4770;border-radius:20px;padding:20px;box-shadow:0 30px 80px #000}.x{float:right;background:#1b2b45;border:1px solid #38577f;color:white;border-radius:10px;padding:6px 10px;cursor:pointer}pre{white-space:pre-wrap;color:#d9e8ff;line-height:1.55}@media (max-width:1100px){.hero,.grid{grid-template-columns:1fr}.kpis{grid-template-columns:repeat(2,1fr)}.category-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.scope{grid-template-columns:1fr}.actions{justify-content:flex-start}}@media (max-width:650px){.category-grid,.strategy-groups{grid-template-columns:1fr}.category-head{align-items:start;flex-direction:column}}.overview-panel{margin:0 0 18px;border-color:#2b6585;background:linear-gradient(145deg,rgba(19,48,79,.96),rgba(8,18,33,.94))}.overview-head{display:flex;justify-content:space-between;gap:14px;align-items:start}.market-badge{display:inline-flex;align-items:center;gap:8px;border:1px solid #4382a8;background:#0b2842;border-radius:999px;padding:7px 11px;font-size:12px;font-weight:950;color:#bceeff}.market-badge.weak{border-color:#8f4b55;background:#3b1720;color:#ffc1c1}.market-badge.strong{border-color:#2f8f6a;background:#0f382c;color:#a6f4d1}.scan-chip{border:1px solid #315577;background:#091a2d;border-radius:13px;padding:9px 12px;color:#bed4ed;font-size:12px;line-height:1.55}.overview-grid{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(320px,.8fr);gap:12px;margin-top:12px}.overview-box{border:1px solid #2b4c6e;background:#091a2d;border-radius:15px;padding:12px}.overview-box h3{font-size:14px;margin:0 0 9px}.dist-list{display:grid;gap:7px}.dist-row{display:grid;grid-template-columns:92px 1fr 34px;gap:8px;align-items:center;font-size:12px}.dist-bar{height:8px;border-radius:999px;background:#13243a;overflow:hidden}.dist-bar i{display:block;height:100%;border-radius:999px;background:linear-gradient(90deg,#42d392,#58dcff)}.quick-nav{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.quick-nav a{color:#bfeaff;text-decoration:none;border:1px solid #34577d;background:#0b2139;border-radius:999px;padding:6px 9px;font-size:12px;font-weight:850}.strategy-panel.collapsed .strategy-groups{display:none}.strategy-panel.collapsed{padding-bottom:12px}.table-wrap{max-height:720px}.table-wrap thead th{position:sticky;top:0;z-index:4;background:#10243b}.table-wrap th:nth-child(1),.table-wrap td:nth-child(1){position:sticky;left:0;z-index:3;background:#0c1b2e}.table-wrap th:nth-child(2),.table-wrap td:nth-child(2){position:sticky;left:42px;z-index:3;background:#0c1b2e}.table-wrap th:nth-child(3),.table-wrap td:nth-child(3){position:sticky;left:116px;z-index:3;background:#0c1b2e}.table-wrap thead th:nth-child(-n+3){z-index:6;background:#10243b}.filter-btn{border:1px solid #34577d;background:#0d223b;color:#dcecff;border-radius:999px;padding:7px 10px;cursor:pointer;font-size:12px;font-weight:850}.filter-btn.active{background:#1677ff;border-color:#6ce4ff;color:#fff}.usage-steps{margin:0;padding-left:20px;color:#cfe0f3;font-size:13px;line-height:1.85}.usage-steps b{color:#8fe6ff}@media (max-width:900px){.overview-grid{grid-template-columns:1fr}.overview-head{flex-direction:column}.table-wrap th:nth-child(1),.table-wrap td:nth-child(1),.table-wrap th:nth-child(2),.table-wrap td:nth-child(2),.table-wrap th:nth-child(3),.table-wrap td:nth-child(3){position:static}}.fundamental-panel{margin:0 0 18px}.fundamental-search{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.fundamental-input{min-width:220px;flex:1;border:1px solid #34577d;background:#09192c;color:#eef6ff;border-radius:13px;padding:11px 13px;font-size:15px;outline:none}.fundamental-input:focus{border-color:#58dcff;box-shadow:0 0 0 3px #58dcff22}.sample-chips{display:flex;gap:7px;flex-wrap:wrap}.sample-chip,.code-link{border:1px solid #34577d;background:#0d2540;color:#9ee9ff;border-radius:999px;padding:6px 9px;font-weight:800;cursor:pointer}.code-link{padding:3px 6px;border-radius:7px;font-size:12px}.fundamental-result{margin-top:14px}.fundamental-loading,.fundamental-error{padding:14px;border-radius:14px;border:1px solid #285276;background:#091a2e}.fundamental-error{color:#ffb1b1;border-color:#853e4c}.fundamental-title{display:flex;justify-content:space-between;gap:12px;align-items:start}.fundamental-title h3{margin:0}.fundamental-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:12px 0}.metric{border:1px solid #294564;border-radius:14px;background:#0a1a2d;padding:11px}.metric .label{color:var(--muted);font-size:12px}.metric .value{font-size:19px;font-weight:950;margin-top:5px}.fundamental-two{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.facts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.fact{border:1px solid #243f60;background:#0a192b;border-radius:11px;padding:8px 10px;font-size:12px}.fact b{color:#9edfff;display:block;margin-bottom:3px}.assessment{display:grid;gap:8px}.assessment-item{border-left:3px solid #6da8d8;background:#0a1a2d;padding:9px 11px;border-radius:0 10px 10px 0;font-size:12px}.assessment-item.up{border-color:#42d392}.assessment-item.down{border-color:#ff6b6b}.assessment-item.warn{border-color:#ffd166}.business-summary{margin:12px 0 0;padding:11px;border:1px solid #243f60;background:#09182a;border-radius:12px;color:#cce0f6;font-size:13px;line-height:1.7}.fundamental-note{color:var(--muted);font-size:12px;line-height:1.65;margin-top:10px}@media (max-width:900px){.fundamental-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.fundamental-two{grid-template-columns:1fr}}@media (max-width:650px){.fundamental-grid,.facts{grid-template-columns:1fr}}

.risk-planner{margin-bottom:16px}.risk-controls{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin:14px 0}.risk-controls label{display:grid;gap:7px;color:var(--muted);font-size:12px;font-weight:800}.risk-controls input{width:100%;box-sizing:border-box;border:1px solid #294c70;background:#08182a;color:#f3f8ff;border-radius:10px;padding:11px 12px;font:inherit}.risk-controls input:focus{outline:2px solid rgba(76,195,255,.25);border-color:#4cc3ff}.portfolio-summary{margin:4px 0 10px;padding:11px 12px;border:1px solid #294c70;border-radius:10px;background:#0a1c30;color:#cde9ff;font-size:13px;line-height:1.6}.portfolio-summary b{color:#74f0b9}.position-plan{min-width:190px;line-height:1.55}.position-plan b{color:#dff7ff}.opening-history{margin-top:16px}.ok-text{color:#74f0b9;font-weight:900}@media(max-width:700px){.risk-controls{grid-template-columns:1fr}.risk-planner .category-head{align-items:flex-start}.position-plan{min-width:160px}}
'''

STYLE += r'''
.opening-hero{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin:0 0 18px}.opening-hero h1{margin:9px 0 6px;font-size:32px}.opening-hero p{margin:0;color:var(--muted);line-height:1.65}.opening-status{border:1px solid #2d6d98;background:#0a2035;border-radius:16px;padding:12px 14px;min-width:205px;line-height:1.65}.opening-status b{color:#79e2ff}.opening-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:0 0 16px}.opening-kpi{border:1px solid #294b70;background:#0b1b30;border-radius:18px;padding:14px}.opening-kpi .n{font-size:26px;font-weight:950;margin-top:5px}.opening-kpi .t{font-size:12px;color:var(--muted)}.opening-note{border:1px solid #365a74;background:#102139;border-radius:15px;padding:13px 15px;color:#d4e7fa;line-height:1.75;margin:0 0 16px}.opening-table{overflow:auto;border:1px solid #274665;border-radius:16px;max-height:730px}.opening-table table{min-width:1380px}.opening-table th{position:sticky;top:0;z-index:2;background:#112a46}.opening-table td{vertical-align:top}.opening-badge{font-size:12px;font-weight:900;border-radius:999px;padding:6px 9px;white-space:nowrap;display:inline-block}.opening-badge.ok{background:#123d31;color:#74f0b9}.opening-badge.wait{background:#3c3211;color:#ffe28b}.opening-badge.no{background:#412033;color:#ffb1bc}.opening-badge.na{background:#22354a;color:#b9d8ef}.opening-reason{max-width:340px;line-height:1.55}.opening-checks{font-size:12px;line-height:1.65;min-width:210px}.opening-checks i{font-style:normal;font-weight:900;margin-right:4px}.opening-checks .yes{color:#62e4a7}.opening-checks .no{color:#ff8d9c}.opening-action{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.opening-action .btn{display:inline-block}.opening-empty{text-align:center;padding:36px 18px;color:var(--muted)}@media(max-width:900px){.opening-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.opening-hero{display:block}.opening-status{margin-top:13px}}@media(max-width:550px){.opening-grid{grid-template-columns:1fr}.opening-hero h1{font-size:26px}}
'''

STYLE += r'''
.quick-nav{position:sticky;top:8px;z-index:30;margin:0 0 16px;padding:9px 12px;border:1px solid #26465f;border-radius:14px;background:rgba(7,22,37,.94);backdrop-filter:blur(14px);display:flex;align-items:center;justify-content:space-between;gap:10px;box-shadow:0 10px 28px rgba(0,0,0,.22)}.quick-links,.quick-state{display:flex;align-items:center;gap:7px;flex-wrap:wrap}.quick-links a,.quick-state span{padding:5px 8px;border-radius:999px;font-size:12px;color:#b9d3e5;text-decoration:none;border:1px solid transparent}.quick-links a:hover{color:#fff;border-color:#346988;background:#12344c}.quick-state span{background:#0b2439;border-color:#284963}.quick-state .on{color:#8ff0bd;border-color:#317a5c}.quick-state .off{color:#ffd28a;border-color:#725a2d}.compact-table .detail-col{display:none}@media(max-width:760px){.quick-nav{position:static;align-items:flex-start;flex-direction:column}.quick-state{display:none}}
.monitor-toolbar{margin:11px 0 0;padding:10px 12px;border:1px solid #315b79;border-radius:13px;background:#0a1d31;display:flex;align-items:center;gap:9px;flex-wrap:wrap}.monitor-toolbar b{color:#8fe6ff}.monitor-toolbar .small{margin-right:auto}.monitor-auto{display:flex;gap:6px;align-items:center;padding:5px 8px;border:1px solid #39718b;border-radius:999px;color:#cfeafa;font-size:12px;cursor:pointer;background:#0b3851}.monitor-auto input{accent-color:#45c58a}.watch-check{accent-color:#31c3ff;width:16px;height:16px;cursor:pointer}.monitor-save-state{font-size:12px;color:#7ee7b6}.monitor-save-state[data-kind="saving"]{color:#ffe28b}.monitor-save-state[data-kind="error"]{color:#ff9baa}.monitor-add{background:#126b50!important;border-color:#55dda9!important;color:#dffff2!important}.monitor-choice{display:flex;align-items:center;gap:5px;white-space:nowrap;font-size:11px;color:#8fb3d3;cursor:pointer}.monitor-choice:has(input:checked){color:#8ff0c4;font-weight:900}.watch-chips{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0}.watch-chip{display:inline-flex;align-items:center;gap:7px;padding:7px 9px;border:1px solid #355b7a;background:#0a1d31;border-radius:999px;color:#d9edff;font-size:12px}.watch-chip small{color:#7fb2d8}.watch-chip button{border:0;background:transparent;color:#ff9baa;font-size:17px;line-height:1;cursor:pointer}.opening-code-link{border:1px solid #34577d;background:#0d2540;color:#9ee9ff;border-radius:7px;padding:3px 6px;font-weight:800;cursor:pointer;font-size:12px;line-height:1.25}.opening-code-link:hover,.opening-code-link:focus-visible{border-color:#58dcff;background:#123555;color:#dfffff;outline:none}.watch-chip .opening-code-link{border:0;background:transparent;padding:0;color:#d9edff;font-size:12px}.watch-chip .opening-code-link:hover,.watch-chip .opening-code-link:focus-visible{color:#9ee9ff;text-decoration:underline}.watch-chip .watch-remove{margin-left:1px}.watch-add{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.watch-add .fundamental-input{max-width:460px}@media(max-width:650px){.watch-add .fundamental-input{min-width:100%;max-width:none}.monitor-toolbar .small{width:100%;margin-right:0}}
'''

STYLE += r'''
.kline-modal{padding:12px;overscroll-behavior:contain}.kline-card{position:relative;width:min(1500px,calc(100vw - 24px));height:min(920px,calc(100vh - 24px));max-width:none;max-height:none;padding:18px;overflow:hidden;display:flex;flex-direction:column;border-radius:18px}.kline-modal.fullscreen{padding:0}.kline-modal.fullscreen .kline-card{width:100vw;height:100vh;border-radius:0;border:0}.kline-head{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;padding-right:44px;flex:0 0 auto}.kline-head h2{margin:7px 0 5px}.kline-actions,.kline-ma,.kline-range{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.kline-auto{display:flex;gap:6px;align-items:center;border:1px solid #315b79;background:#0b2439;border-radius:999px;padding:7px 10px;color:#cfeafa;font-size:12px;cursor:pointer}.kline-auto input{accent-color:#45c58a}.kline-actions .filter-btn:disabled{opacity:.55;cursor:wait}.kline-tools{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;margin:12px 0 6px;flex:0 0 auto}.kline-ma{margin:0}.kline-range{justify-content:flex-end}.kline-divider{width:1px;height:24px;background:#31516c}.ma-toggle{display:flex;gap:6px;align-items:center;border:1px solid #33536f;background:#0a1b2e;border-radius:999px;padding:6px 9px;font-size:12px;cursor:pointer}.ma-toggle input{accent-color:var(--ma-color)}.kline-status{display:flex;justify-content:space-between;gap:12px;align-items:center;flex:0 0 auto}.kline-status-right{text-align:right;display:grid;gap:3px}.kline-refresh-status{font-size:12px;color:#76d7ff}.kline-info{min-height:24px;color:#cfe8ff;font-size:13px;margin:4px 0}.kline-canvas-wrap{position:relative;border:1px solid #284967;border-radius:14px;background:#071321;overflow:hidden;flex:1 1 auto;min-height:320px}.kline-canvas-wrap canvas{display:block;width:100%;height:100%;min-height:320px;cursor:crosshair;touch-action:none}.kline-canvas-wrap canvas.dragging{cursor:grabbing}.kline-loading{position:absolute;inset:0;display:grid;place-items:center;background:rgba(5,13,24,.82);color:#9edfff;font-weight:850}.kline-loading.hide{display:none}.kline-note{margin-top:7px;flex:0 0 auto}.kline-close{position:absolute;right:16px;top:16px;z-index:3;font-size:18px}.kline-modal.fullscreen #klineFullscreenBtn{background:#1677ff;border-color:#6ce4ff}body.kline-open{overflow:hidden}@media(max-width:760px){.kline-modal{padding:0}.kline-card{width:100vw;height:100vh;padding:12px;border-radius:0;border:0}.kline-head{display:block;padding-right:40px}.kline-actions{margin-top:10px}.kline-tools{display:block}.kline-range{justify-content:flex-start;margin-top:8px}.kline-status{display:block}.kline-status-right{text-align:left;margin-bottom:5px}.kline-info{white-space:nowrap;overflow:auto}.kline-canvas-wrap{min-height:300px}.kline-canvas-wrap canvas{min-height:300px}.kline-range .filter-btn{padding:7px 9px}.kline-divider{display:none}}
'''

SCRIPT = r'''
try{localStorage.removeItem('quant_token')}catch(e){}
function getAdminToken(){try{return sessionStorage.getItem('quant_token')||''}catch(e){return ''}}function setAdminToken(v){try{sessionStorage.setItem('quant_token',v)}catch(e){}}function clearAdminToken(){try{sessionStorage.removeItem('quant_token')}catch(e){}alert('本标签页的管理令牌已清除')}
const esc=x=>String(x??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
let active='全部', activeStrategy='全部', q='', priorityOnly=false, showAllRows=false, strategyExpanded=false, compactTable=true;
let monitorCodes=new Set((((window.__DATA__.watchlist)||{}).codes||[])),monitorDraft=new Set(),monitorAutoSync=!!((((window.__DATA__.watchlist)||{}).auto_sync||{}).enabled); let monitorDecorating=false,monitorObserver=null,monitorSaveTimer=null,monitorSaveVersion=0,monitorSaving=false,monitorPending=false;
const num=x=>{if(x===undefined||x===null||x==='')return '-'; return x};
function rowVisible(r){const g=r.strategy_group||'综合类', st=r.strategy||''; const s=(r.code+' '+r.name+' '+st+' '+g+' '+(r.reason||'')).toLowerCase(); return (active==='全部'||g.includes(active)||active.includes(g)) && (activeStrategy==='全部'||st.includes(activeStrategy)) && (!q||s.includes(q.toLowerCase()));}
function kpi(label,value,sub=''){return `<div class="kpi"><div class="label">${label}</div><div class="value">${num(value)}</div><div class="small muted">${sub}</div></div>`}
function feedbackHtml(f){if(!f||!f.has_data)return `<h2>历史信号反馈</h2><p class="feedback-note">${esc((f&&f.message)||'暂未形成可复盘样本。')}</p><div class="scope"><div><b>量化闭环尚在积累</b><br><span class="muted">下一交易日扫描时，系统会自动追踪上一批候选；样本积累后才展示观察期正收益比例、平均观察收益和目标/止损触达情况。</span></div></div>`;const s=f.summary||{}, rows=f.rows||[], tactics=f.tactic_stats||[];const adequacy=s.sample_adequacy||((s.observed_count||0)>=30?'可初步观察':'样本不足，不判断有效性');const trs=rows.map(r=>{const ret=parseFloat(r.ret_latest_pct);const cls=isFinite(ret)?(ret>0?'feedback-up':ret<0?'feedback-down':''):'',txt=isFinite(ret)?`${ret>0?'+':''}${ret.toFixed(2)}%`:esc(r.ret_latest_pct||'-');return `<tr><td>${esc(r.source_date||'-')}</td><td><b>${esc(r.code)}</b> ${esc(r.name)}</td><td>${esc(r.strategy)}</td><td class="${cls}">${txt}</td><td>${esc(r.max_high_pct||'-')}%</td><td>${esc(r.max_drawdown_pct||'-')}%</td><td>${esc(r.status||'-')}</td></tr>`}).join('');const ttrs=tactics.map(t=>`<tr><td><b>${esc(t.strategy)}</b></td><td>${esc(t.sample_count)}</td><td>${t.positive_rate_pct==null?'-':esc(t.positive_rate_pct)+'%'}</td><td>${t.avg_observation_return_pct==null?'-':esc(t.avg_observation_return_pct)+'%'}</td><td>${esc(t.target_hit_count)}</td><td>${esc(t.stop_hit_count)}</td></tr>`).join('');return `<h2>历史信号反馈</h2><p class="feedback-note">最新复盘信号日：${esc(f.source_date)}；截至：${esc(f.as_of_date)}。${esc(f.definition||'')}</p><div class="kpis">${kpi('复盘样本',s.sample_count||0,adequacy)}${kpi('观察期正收益比例',s.positive_rate_pct==null?'-':s.positive_rate_pct+'%','收盘观察')}${kpi('平均观察收益',s.avg_observation_return_pct==null?'-':s.avg_observation_return_pct+'%','非实盘收益')}${kpi('曾达目标',s.target_hit_count||0,'日线触达')}${kpi('曾触止损',s.stop_hit_count||0,'日线触达')}${kpi('信号日期数',s.signal_days||0,'持续积累')}</div><div class="feedback-table"><table><thead><tr><th>信号日</th><th>股票</th><th>战法</th><th>观察收益</th><th>区间最大涨幅</th><th>区间最大回撤</th><th>状态</th></tr></thead><tbody>${trs||'<tr><td colspan="7" class="muted">暂无可展示反馈</td></tr>'}</tbody></table></div><div class="feedback-table tactic-table"><table><thead><tr><th>战法归因（共振会重复归因）</th><th>样本</th><th>观察期正收益比例</th><th>平均观察收益</th><th>达目标</th><th>触止损</th></tr></thead><tbody>${ttrs||'<tr><td colspan="6" class="muted">样本不足，暂不评价战法优劣</td></tr>'}</tbody></table></div>`}
function money(v){const n=parseFloat(v);if(!isFinite(n))return '-';const a=Math.abs(n);if(a>=1e8)return (n/1e8).toFixed(2)+'亿';if(a>=1e4)return (n/1e4).toFixed(2)+'万';return n.toFixed(2)}
function pct(v){const n=parseFloat(v);return isFinite(n)?`${n>0?'+':''}${n.toFixed(2)}%`:'-'}
function dailyOverview(d,rows,meta,scope,core,avgRps){const market=d.market||'未识别';const tone=(market.includes('弱')||market.includes('防守'))?'weak':(market.includes('强')?'strong':'');const stages={};rows.forEach(r=>{const x=r.execution_stage||'待确认';stages[x]=(stages[x]||0)+1});const stageRows=Object.entries(stages).sort((a,b)=>b[1]-a[1]);const maxStage=Math.max(1,...stageRows.map(x=>x[1]));const stageHtml=stageRows.map(([name,count])=>`<div class="dist-row"><span>${esc(name)}</span><div class="dist-bar"><i style="width:${Math.round(count/maxStage*100)}%"></i></div><b>${count}</b></div>`).join('');const top=rows[0]||{};const last=(d.run_state||{}).last_finished||'尚未完成';const quality=scope.data_quality||{};const completeness=scope.universe_count?Math.max(0,100-(Number(quality.failure_rate||0)*100)).toFixed(1)+'%':'-';const qualityText=quality.ok?'通过':'需复核';return `<section class="card overview-panel" id="overview"><div class="overview-head"><div><span class="market-badge ${tone}">市场状态 · ${esc(market)}</span><h2 style="margin-top:11px">今日量化决策摘要</h2><div class="recommend">${esc(d.recommendation)}<div class="small muted" style="margin-top:7px">排序第一仅表示规则评分靠前，仍需遵守“执行层级、价格确认、止损”三个条件。</div></div></div><div class="scan-chip"><b>自动扫描</b><br>每日 ${esc(d.schedule||'15:35,21:00')}<br>最近完成：${esc(last)}<br>数据日：${esc(d.date)}</div></div><div class="kpis">${kpi('全市场行情',scope.spot_count,'进入行情列表')}${kpi('基础过滤池',scope.universe_count,'过滤ST/流动性等')}${kpi('K线深筛',scope.kline_scanned_count,'逐只匹配战法')}${kpi('数据完整度',completeness,qualityText)}${kpi('候选信号',rows.length,'不是全部都要买')}${kpi('核心/优先',core,'优先复核对象')}${kpi('平均RPS',isFinite(avgRps)?avgRps:'-','候选横向强度')}</div><div class="overview-grid"><div class="overview-box"><h3>执行政策与筛选范围</h3><div class="small">${esc(scope.execution_policy||'候选需等待价格确认。')}</div><div class="small muted" style="margin-top:7px">${esc(scope.text||'')}</div><div class="small muted" style="margin-top:7px">数据质量：${esc((scope.data_quality||{}).reason||'未检查')}</div><div class="quick-nav"><a href="#candidates">直接看候选</a><a href="#fundamentals">查询基本面</a><a href="#strategies">查看战法</a><a href="#feedback">查看复盘</a></div></div><div class="overview-box"><h3>执行层级分布</h3><div class="dist-list">${stageHtml||'<span class="muted small">暂无候选</span>'}</div></div></div></section>`}
function fundamentalPanel(rows){const samples=(rows||[]).slice(0,6).map(r=>`<button class="sample-chip" onclick="queryFundamental('${esc(r.code)}')">${esc(r.code)} ${esc(r.name)}</button>`).join('');return `<section class="card fundamental-panel" id="fundamentals"><div class="category-head"><div><h2>个股基本面查询</h2><p>输入沪深A股六位代码，查看公司概况、最新公开财务指标、估值快照与近五期财报。数据按查询时点短时缓存，不替代公告核验。</p></div><span class="pill">公开数据 · 非投资建议</span></div><div class="fundamental-search"><input id="fund-code" class="fundamental-input" maxlength="6" inputmode="numeric" placeholder="输入股票代码，例如 000938 / 600000" onkeydown="if(event.key==='Enter')queryFundamental()"><button class="btn primary" onclick="queryFundamental()">查询基本面</button><div class="sample-chips">${samples}</div></div><div id="fundamental-result" class="fundamental-result"></div></section>`}
function fundamentalHtml(d){const c=d.company||{},q=d.quote||{},f=d.latest_finance||{},history=d.finance_history||[],flags=d.assessment||[];const metric=(l,v,sub='')=>`<div class="metric"><div class="label">${esc(l)}</div><div class="value">${esc(v)}</div><div class="small muted">${esc(sub)}</div></div>`;const facts=[['所属行业',c.industry],['上市日期',c.listing_date],['上市板块',c.market_type],['交易所',c.exchange],['所在地区',c.province],['董事长',c.chairman],['法人代表',c.legal_representative],['员工人数',c.employees]].map(x=>`<div class="fact"><b>${esc(x[0])}</b>${esc(x[1]||'-')}</div>`).join('');const flagHtml=flags.map(x=>`<div class="assessment-item ${esc(x.type||'neutral')}"><b>${esc(x.title)}</b>：${esc(x.text)}</div>`).join('')||'<div class="muted small">暂无足够财务字段形成结构提示。</div>';const hist=history.map(x=>`<tr><td>${esc(x.period)}</td><td>${esc(x.type)}</td><td>${money(x.revenue)}</td><td>${pct(x.revenue_yoy)}</td><td>${money(x.net_profit)}</td><td>${pct(x.profit_yoy)}</td><td>${x.roe==null?'-':esc(x.roe)+'%'}</td><td>${x.gross_margin==null?'-':esc(x.gross_margin)+'%'}</td><td>${x.debt_ratio==null?'-':esc(x.debt_ratio)+'%'}</td></tr>`).join('');const warnings=(d.warnings||[]).map(x=>`<span class="badge risk">${esc(x)}</span>`).join(' ');return `<div class="fundamental-title"><div><h3>${esc(d.code)} ${esc(d.name)}</h3><p class="muted small">数据来源：${esc(d.source)}；查询：${esc(d.fetched_at)}${d.cached?'（缓存）':''}</p></div><div>${warnings}</div></div><div class="fundamental-grid">${metric('最新价',q.price==null?'-':q.price,q.pct==null?'':pct(q.pct))}${metric('动态市盈率',q.pe_dynamic==null?'-':q.pe_dynamic,'亏损或缺失时不具可比性')}${metric('市净率',q.pb==null?'-':q.pb,'按数据源口径')}${metric('总市值',money(q.total_market_cap),'流通市值 '+money(q.float_market_cap))}${metric('最近营收',money(f.revenue),esc(f.period||'最近披露期'))}${metric('归母净利润',money(f.net_profit),'同比 '+pct(f.profit_yoy))}${metric('加权ROE',f.roe==null?'-':f.roe+'%','报告期口径，非年化')}${metric('资产负债率',f.debt_ratio==null?'-':f.debt_ratio+'%','金融行业可比性较弱')}</div><div class="fundamental-two"><div><h3>公司概况</h3><div class="facts">${facts}</div></div><div><h3>财务结构提示</h3><div class="assessment">${flagHtml}</div></div></div>${c.business_summary?`<div class="business-summary"><b>公司简介：</b>${esc(c.business_summary)}</div>`:''}<div class="feedback-table" style="margin-top:14px"><h3>近五期公开财务摘要</h3><table><thead><tr><th>报告期</th><th>类型</th><th>营收</th><th>营收同比</th><th>归母净利</th><th>净利同比</th><th>加权ROE</th><th>毛利率</th><th>资产负债率</th></tr></thead><tbody>${hist||'<tr><td colspan="9" class="muted">暂无财务数据</td></tr>'}</tbody></table></div><div class="fundamental-note">${(d.notes||[]).map(esc).join('　')}</div>`}
async function queryFundamental(raw){const input=document.getElementById('fund-code');const code=(raw||input&&input.value||'').replace(/\D/g,'').slice(0,6);if(input)input.value=code;const el=document.getElementById('fundamental-result');if(!el)return;if(!/^((00|30|60|68)\d{4})$/.test(code)){el.innerHTML='<div class="fundamental-error">请输入沪深A股六位代码，例如 000938、600000 或 688xxx。</div>';return;}el.innerHTML='<div class="fundamental-loading">正在获取公司概况、估值快照和公开财务数据…</div>';try{const r=await fetch('/api/fundamentals?code='+encodeURIComponent(code));const d=await r.json();if(!r.ok||!d.ok)throw new Error(d.message||'查询失败');el.innerHTML=fundamentalHtml(d);el.scrollIntoView({behavior:'smooth',block:'nearest'});}catch(e){el.innerHTML=`<div class="fundamental-error">查询失败：${esc(e.message||'请稍后重试')}</div>`}}

const KLINE_MA_COLORS={5:'#ffd166',10:'#58dcff',20:'#d08cff',60:'#ff9f68'};
let klineState={code:'',name:'',period:'day',data:null,mas:new Set([5,10,20,60]),request:0,limit:240,visible:240,offset:0,auto:true,timer:null,ticker:null,nextRefreshAt:0};
function openKline(code){const clean=String(code||'').replace(/\D/g,'').slice(0,6),row=(window.__DATA__.rows||[]).find(x=>String(x.code)===clean);klineState.code=clean;klineState.name=row?.name||'';klineState.period='day';klineState.data=null;klineState.offset=0;klineState.visible=klineState.limit;document.body.classList.add('kline-open');document.getElementById('klineModal').classList.add('show');document.getElementById('klineAuto').checked=klineState.auto;const refreshBtn=document.getElementById('klineRefreshBtn');refreshBtn.disabled=false;refreshBtn.textContent='刷新行情';document.getElementById('klineRefreshStatus').textContent='正在获取刷新计划…';document.getElementById('klineTitle').textContent=`${klineState.code} ${klineState.name} K线图`;document.querySelectorAll('[data-period]').forEach(b=>b.classList.toggle('active',b.dataset.period==='day'));loadKline()}
function closeKline(){klineState.request++;clearKlineRefreshTimers();const refreshBtn=document.getElementById('klineRefreshBtn');if(refreshBtn){refreshBtn.disabled=false;refreshBtn.textContent='刷新行情'}document.body.classList.remove('kline-open');const modal=document.getElementById('klineModal');modal.classList.remove('show','fullscreen');document.getElementById('klineFullscreenBtn').textContent='全屏'}
function setKlinePeriod(period){if(!['day','week','month'].includes(period)||period===klineState.period)return;klineState.period=period;klineState.offset=0;document.querySelectorAll('[data-period]').forEach(b=>b.classList.toggle('active',b.dataset.period===period));loadKline()}
function clearKlineRefreshTimers(){clearTimeout(klineState.timer);clearInterval(klineState.ticker);klineState.timer=null;klineState.ticker=null;klineState.nextRefreshAt=0}
function updateKlineRefreshStatus(){const el=document.getElementById('klineRefreshStatus');if(!el)return;if(!klineState.auto){el.textContent='自动刷新已关闭';return}if(!klineState.nextRefreshAt){el.textContent='等待刷新计划';return}const remain=Math.max(0,Math.ceil((klineState.nextRefreshAt-Date.now())/1000)),m=String(Math.floor(remain/60)).padStart(2,'0'),sec=String(remain%60).padStart(2,'0');el.textContent=`下次自动刷新 ${m}:${sec}`}
function scheduleKlineRefresh(seconds){clearKlineRefreshTimers();if(!klineState.auto||!document.getElementById('klineModal')?.classList.contains('show')){updateKlineRefreshStatus();return}const wait=Math.max(15,Number(seconds)||300);klineState.nextRefreshAt=Date.now()+wait*1000;updateKlineRefreshStatus();klineState.ticker=setInterval(updateKlineRefreshStatus,1000);klineState.timer=setTimeout(()=>loadKline(true),wait*1000)}
function setKlineAutoRefresh(on){klineState.auto=!!on;if(on)scheduleKlineRefresh(klineState.data?.auto_refresh_seconds||300);else{clearKlineRefreshTimers();updateKlineRefreshStatus()}}
function refreshKlineNow(){if(document.getElementById('klineRefreshBtn')?.disabled)return;loadKline(true,true)}
function toggleKlineFullscreen(){const modal=document.getElementById('klineModal'),on=modal.classList.toggle('fullscreen');document.getElementById('klineFullscreenBtn').textContent=on?'退出全屏':'全屏';setTimeout(drawKline,40)}
function setKlineLimit(limit){limit=Math.max(40,Math.min(640,Number(limit)||240));if(limit===klineState.limit)return;klineState.limit=limit;klineState.visible=limit;klineState.offset=0;document.querySelectorAll('[data-limit]').forEach(b=>b.classList.toggle('active',Number(b.dataset.limit)===limit));loadKline()}
function zoomKline(factor){if(!klineState.data?.rows?.length)return;const max=klineState.data.rows.length,next=Math.max(20,Math.min(max,Math.round(klineState.visible*factor)));klineState.visible=next;klineState.offset=Math.min(klineState.offset,Math.max(0,max-next));drawKline()}
function panKline(direction){if(!klineState.data?.rows?.length)return;const maxOffset=Math.max(0,klineState.data.rows.length-klineState.visible),step=Math.max(1,Math.round(klineState.visible*.45));klineState.offset=Math.max(0,Math.min(maxOffset,klineState.offset+(direction<0?step:-step)));drawKline()}
function resetKlineView(){if(!klineState.data?.rows?.length)return;klineState.offset=0;drawKline()}
function klineToFundamental(){const code=klineState.code;closeKline();location.hash='fundamentals';setTimeout(()=>queryFundamental(code),50)}
function klineMaControls(){const box=document.getElementById('klineMa');box.innerHTML=[5,10,20,60].map(n=>`<label class="ma-toggle" style="--ma-color:${KLINE_MA_COLORS[n]}"><input type="checkbox" ${klineState.mas.has(n)?'checked':''} onchange="toggleKlineMa(${n},this.checked)"><span style="color:${KLINE_MA_COLORS[n]}">MA${n}</span></label>`).join('')}
function toggleKlineMa(n,on){on?klineState.mas.add(n):klineState.mas.delete(n);drawKline()}
async function loadKline(silent=false,force=false){const req=++klineState.request,loading=document.getElementById('klineLoading'),canvas=document.getElementById('klineCanvas'),refreshBtn=document.getElementById('klineRefreshBtn'),oldData=klineState.data,oldVisible=klineState.visible,oldOffset=klineState.offset;clearKlineRefreshTimers();if(!force&&refreshBtn){refreshBtn.disabled=false;refreshBtn.textContent='刷新行情'}if(force&&refreshBtn){refreshBtn.disabled=true;refreshBtn.textContent='刷新中…'}if(!silent){loading.textContent='正在加载K线…';loading.classList.remove('hide');document.getElementById('klineInfo').textContent='';klineState.data=null;if(canvas){const c=canvas.getContext('2d');c.clearRect(0,0,canvas.width,canvas.height)}}klineMaControls();try{const forceParam=force?'&refresh=1':'';const r=await fetch(`/api/kline?code=${encodeURIComponent(klineState.code)}&period=${encodeURIComponent(klineState.period)}&limit=${klineState.limit}${forceParam}&_=${Date.now()}`,{cache:'no-store'});const d=await r.json();if(req!==klineState.request)return;if(!r.ok||!d.ok)throw new Error(d.message||'K线查询失败');klineState.data=d;klineState.offset=silent?Math.min(oldOffset,Math.max(0,d.rows.length-oldVisible)):0;klineState.visible=Math.min(silent?oldVisible:klineState.visible||klineState.limit,d.rows.length);klineState.name=d.name||klineState.name;document.getElementById('klineTitle').textContent=`${d.code} ${d.name||''} · ${{day:'日线',week:'周线',month:'月线'}[d.period]}`;const cacheText=d.cached?`缓存 ${d.cache_age_seconds||0}秒`:'刚获取';document.getElementById('klineMeta').textContent=`${d.source}｜${d.adjust}｜行情截止 ${d.latest_bar_date||'-'}｜查询 ${d.served_at||d.fetched_at||'-'}｜${cacheText}｜${d.market_state||''}`;document.getElementById('klineNote').textContent=(d.note||'红涨绿跌；均线随当前周期计算。')+` 自动刷新：${d.market_active?'交易时段约30秒':'非交易时段约5分钟'}。`;loading.classList.add('hide');drawKline();scheduleKlineRefresh(d.auto_refresh_seconds)}catch(e){if(req!==klineState.request)return;if(!silent){loading.textContent=`加载失败：${e.message||'请稍后重试'}`;scheduleKlineRefresh(60)}else{klineState.data=oldData;document.getElementById('klineMeta').textContent+='｜自动刷新失败，稍后重试';scheduleKlineRefresh(60)}}finally{if(force&&req===klineState.request&&refreshBtn){refreshBtn.disabled=false;refreshBtn.textContent='刷新行情'}}}
function klineVol(v){const n=Number(v||0);return n>=1e8?(n/1e8).toFixed(2)+'亿':n>=1e4?(n/1e4).toFixed(1)+'万':n.toFixed(0)}
function klineRowInfo(r){if(!r)return '';const pct=(r.close/r.open-1)*100;return `${r.date}　开 ${r.open.toFixed(2)}　高 ${r.high.toFixed(2)}　低 ${r.low.toFixed(2)}　收 ${r.close.toFixed(2)}　${pct>=0?'+':''}${pct.toFixed(2)}%　量 ${klineVol(r.volume)}　`+[5,10,20,60].filter(n=>klineState.mas.has(n)&&r['ma'+n]!=null).map(n=>`MA${n} ${Number(r['ma'+n]).toFixed(2)}`).join('　')}
function paintKlineBase(canvas,c,rows,w,h){c.clearRect(0,0,w,h);const left=58,right=18,top=24,priceBottom=Math.round(h*.72),volTop=priceBottom+24,bottom=h-32,plotW=w-left-right,priceH=priceBottom-top,volH=bottom-volTop;let vals=[];rows.forEach(r=>{vals.push(r.low,r.high);[5,10,20,60].forEach(n=>{if(klineState.mas.has(n)&&r['ma'+n]!=null)vals.push(r['ma'+n])})});let min=Math.min(...vals),max=Math.max(...vals),pad=Math.max((max-min)*.06,max*.005,.01);min-=pad;max+=pad;const py=v=>top+(max-v)/(max-min)*priceH,maxVol=Math.max(...rows.map(r=>r.volume||0),1),step=plotW/rows.length,body=Math.max(2,Math.min(9,step*.62));c.font='11px sans-serif';c.strokeStyle='#17334c';c.fillStyle='#7895ae';c.lineWidth=1;for(let i=0;i<=5;i++){const y=top+priceH*i/5;c.beginPath();c.moveTo(left,y);c.lineTo(w-right,y);c.stroke();const v=max-(max-min)*i/5;c.fillText(v.toFixed(2),5,y+4)}for(let i=0;i<rows.length;i++){const r=rows[i],x=left+step*(i+.5),up=r.close>=r.open,color=up?'#ef5350':'#26a69a';c.strokeStyle=color;c.fillStyle=color;c.beginPath();c.moveTo(x,py(r.high));c.lineTo(x,py(r.low));c.stroke();const y1=py(r.open),y2=py(r.close),bh=Math.max(1,Math.abs(y2-y1));up?c.strokeRect(x-body/2,Math.min(y1,y2),body,bh):c.fillRect(x-body/2,Math.min(y1,y2),body,bh);const vh=(r.volume/maxVol)*volH;c.globalAlpha=.7;c.fillRect(x-body/2,bottom-vh,body,vh);c.globalAlpha=1}for(const n of [5,10,20,60]){if(!klineState.mas.has(n))continue;c.strokeStyle=KLINE_MA_COLORS[n];c.lineWidth=1.35;c.beginPath();let started=false;rows.forEach((r,i)=>{const v=r['ma'+n];if(v==null)return;const x=left+step*(i+.5),y=py(v);started?c.lineTo(x,y):c.moveTo(x,y);started=true});if(started)c.stroke()}c.fillStyle='#7895ae';c.strokeStyle='#17334c';c.lineWidth=1;for(let i=0;i<6;i++){const idx=Math.min(rows.length-1,Math.round(i*(rows.length-1)/5)),x=left+step*(idx+.5);c.fillText(rows[idx].date.slice(2),Math.max(left,x-25),h-10)}c.fillText('成交量',5,volTop+12);return {left,right,top,bottom,step,py}}
function drawKline(){const d=klineState.data,canvas=document.getElementById('klineCanvas');if(!d||!canvas||!d.rows?.length)return;const allRows=d.rows,total=allRows.length; klineState.visible=Math.max(20,Math.min(total,klineState.visible||120));klineState.offset=Math.max(0,Math.min(Math.max(0,total-klineState.visible),klineState.offset||0));const end=Math.max(klineState.visible,total-klineState.offset),start=Math.max(0,end-klineState.visible),rows=allRows.slice(start,end),w=Math.max(620,canvas.clientWidth||1000),h=Math.max(320,canvas.clientHeight||560),dpr=Math.min(window.devicePixelRatio||1,2);canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr);const c=canvas.getContext('2d');c.setTransform(dpr,0,0,dpr,0,0);const latest=rows[rows.length-1];let geom=paintKlineBase(canvas,c,rows,w,h);document.getElementById('klineInfo').textContent=klineRowInfo(latest);document.getElementById('klineViewport').textContent=`显示 ${start+1}-${end} / ${total} 根｜${rows[0].date} 至 ${rows[rows.length-1].date}`;canvas.onmousemove=e=>{const rect=canvas.getBoundingClientRect(),mx=(e.clientX-rect.left)*w/rect.width,idx=Math.max(0,Math.min(rows.length-1,Math.floor((mx-geom.left)/geom.step)));geom=paintKlineBase(canvas,c,rows,w,h);document.getElementById('klineInfo').textContent=klineRowInfo(rows[idx]);drawKlineCross(c,w,geom,idx,rows)};canvas.onmouseleave=()=>{geom=paintKlineBase(canvas,c,rows,w,h);document.getElementById('klineInfo').textContent=klineRowInfo(latest)};canvas.onwheel=e=>{e.preventDefault();zoomKline(e.deltaY<0?.8:1.25)};let dragX=null;canvas.onpointerdown=e=>{dragX=e.clientX;canvas.classList.add('dragging');canvas.setPointerCapture?.(e.pointerId)};canvas.onpointerup=e=>{if(dragX==null)return;const dx=e.clientX-dragX;dragX=null;canvas.classList.remove('dragging');if(Math.abs(dx)>35)panKline(dx>0?-1:1)};canvas.onpointercancel=()=>{dragX=null;canvas.classList.remove('dragging')}}
function drawKlineCross(c,w,geom,idx,rows){const r=rows[idx],x=geom.left+geom.step*(idx+.5),y=geom.py(r.close);c.save();c.setLineDash([4,4]);c.strokeStyle='rgba(210,235,255,.48)';c.beginPath();c.moveTo(x,geom.top);c.lineTo(x,geom.bottom);c.moveTo(geom.left,y);c.lineTo(w-geom.right,y);c.stroke();c.restore()}
window.addEventListener('resize',()=>{if(document.getElementById('klineModal')?.classList.contains('show'))drawKline()});document.addEventListener('visibilitychange',()=>{if(!document.hidden&&klineState.auto&&klineState.data&&document.getElementById('klineModal')?.classList.contains('show')&&(!klineState.nextRefreshAt||Date.now()>=klineState.nextRefreshAt))loadKline(true)});document.addEventListener('keydown',e=>{if(!document.getElementById('klineModal')?.classList.contains('show'))return;if(e.key==='Escape')closeKline();else if(e.key==='ArrowLeft')panKline(-1);else if(e.key==='ArrowRight')panKline(1);else if(e.key==='+'||e.key==='=')zoomKline(.8);else if(e.key==='-')zoomKline(1.25);else if(e.key.toLowerCase()==='r')refreshKlineNow();else if(e.key.toLowerCase()==='f')toggleKlineFullscreen()});

function render(){const d=window.__DATA__, el=document.getElementById('app'); if(!d.has_data){el.innerHTML=`<section class="hero"><div class="title"><span class="eyebrow">A股量化驾驶舱</span><h1>暂无扫描结果</h1><p>${esc(d.message||'')}</p></div><div class="actions"><button class="btn primary" onclick="runScan()">立即运行全市场扫描</button></div></section>${statusHtml(d.run_state)}`;return;}
 const rows=d.rows||[], meta=d.meta||{}, scope=d.scope||{}; const fundamentalBlock=fundamentalPanel(rows); const core=rows.filter(r=>['核心推荐','优先关注'].includes(r.level)).length; const avgBase=rows.filter(r=>parseFloat(r.rps_combo)); const avgRps=Math.round(rows.reduce((a,r)=>a+(parseFloat(r.rps_combo)||0),0)/Math.max(1,avgBase.length)); const feedbackBlock=feedbackHtml(d.feedback); const overviewBlock=dailyOverview(d,rows,meta,scope,core,avgRps);
 const tabs=(d.group_order||['全部']).map(g=>`<button class="tab ${g===active?'active':''}" onclick="active='${g}';activeStrategy='全部';showAllRows=false;render()">${g} <b>${g==='全部'?rows.length:(d.group_counts||{})[g]||0}</b></button>`).join('');
 const categoryCards=(d.strategy_book||[]).map(b=>{const count=(d.group_counts||{})[b.group]||0; return `<button class="category-card ${b.group===active&&activeStrategy==='全部'?'active':''}" onclick="active='${esc(b.group)}';activeStrategy='全部';showAllRows=false;render()"><span class="cat-name">${esc(b.group)}</span><span class="cat-count">${count}</span><div class="cat-items">${esc((b.items||[]).join(' · '))}</div><div class="cat-note">点击查看本类候选</div></button>`}).join('');
 const strategyNav=(d.strategy_book||[]).map(b=>`<div class="strategy-group"><div class="strategy-group-title">${esc(b.group)} · ${esc((b.items||[]).length)} 种</div><div class="strategy-chips">${(b.items||[]).map(it=>{const hit=rows.filter(r=>(r.strategy||'').includes(it)).length; return `<button class="strategy-chip ${activeStrategy===it?'active':''}" onclick="active='${esc(b.group)}';activeStrategy='${esc(it)}';showAllRows=false;render()">${esc(it)}<em>${hit}</em></button>`}).join('')}</div></div>`).join('');
 const filteredBase=rows.filter(rowVisible); const filtered=priorityOnly?filteredBase.filter(r=>['核心推荐','优先关注'].includes(r.level)):filteredBase; const displayRows=showAllRows?filtered:filtered.slice(0,20); const leaders=Object.entries(d.group_top||{}).map(([g,r])=>`<div class="leader-card"><div class="muted small">${esc(g)}</div><b>${esc(r.code)} ${esc(r.name)}</b><div class="small">${esc(r.strategy)}｜规则评分 ${esc(r.score)}｜RPS ${esc(r.rps_combo||'-')}</div><div class="bar"><i style="width:${Math.min(100,parseFloat(r.score)||0)}%"></i></div></div>`).join('');
 const books=(d.strategy_book||[]).map(b=>`<div class="book-item"><div class="pill">${esc(b.group)}</div><div class="items">${esc((b.items||[]).join(' / '))}</div><div class="small muted">规则：${esc(b.rule)}</div><div class="small" style="color:#ffdca0">风控：${esc(b.risk)}</div></div>`).join('');
 const trs=displayRows.map((r,i)=>`<tr><td>${i+1}</td><td><button class="code-link" title="查看日线、周线、月线和均线" onclick="openKline('${esc(r.code)}')">${esc(r.code)}</button></td><td>${esc(r.name)}</td><td><span class="badge ${esc(r.level_class)}">${esc(r.level)}</span></td><td>${esc(r.strategy_group)}</td><td>${esc(r.strategy)}</td><td><b>${esc(r.score)}</b></td><td>${esc(r.rps_combo||'-')}</td><td class="detail-col">${esc(r.rps60||'-')}</td><td>${esc(r.close)}</td><td class="detail-col">${esc(r.pct_today)}%</td><td class="detail-col">${esc(r.turnover)}%</td><td>${esc(r.buy_zone)}</td><td>${esc(r.stop_loss)}</td><td class="detail-col">${esc(r.target)}</td><td class="detail-col">${esc(r.risk_reward)}</td><td class="detail-col">${esc(r.risk_tags||'')}</td><td class="detail-col"><span class="exec-stage">${esc(r.execution_stage||'-')}</span></td><td class="reason" title="${esc(r.risk_budget_hint||'')}">${esc(r.action)}</td><td class="reason" title="${esc(r.reason)}">${esc(r.reason)}</td></tr>`).join('');
 const autoCfg=(d.watchlist||{}).auto_sync||{}; const quickNav=`<nav class="quick-nav"><div class="quick-links"><a href="#overview">今日总览</a><a href="#fundamentals">个股基本面</a><a href="#strategies">战法导航</a><a href="#feedback">历史反馈</a><a href="#candidates">候选列表</a><a href="/opening">早盘确认</a><a href="/positions">交易复盘</a></div><div class="quick-state"><span>信号日 ${esc(d.date||'-')}</span><span class="${autoCfg.enabled?'on':'off'}">${autoCfg.enabled?'自动监控已开启':'自动监控未开启'}</span><span>监控 ${(d.watchlist||{}).count||0} 只</span></div></nav>`;
 const reports=(d.reports||[]).slice(0,8).map(r=>`<div class="hist-row"><span>${esc(r.date)}｜${r.count}只</span><span class="muted">池:${esc(r.universe_count||'-')} 深筛:${esc(r.kline_scanned_count||'-')} 最高:${esc(r.top_score)}</span></div>`).join('');
 el.innerHTML=`<section class="hero"><div class="title"><span class="eyebrow">A股量化驾驶舱 · ${esc(d.date)}</span><h1>全市场战法分类推荐</h1><p>先看市场环境与执行层级，再看候选；量化排序不是买入指令。</p></div><div class="actions"><a class="btn primary" href="#candidates">查看今日候选</a><a class="btn" href="/opening">开盘建仓确认</a><a class="btn" href="/positions">交易复盘台</a><button class="btn" onclick="document.getElementById('fund-code').focus();location.hash='fundamentals'">查个股基本面</button><button class="btn" onclick="runScan()">手动补跑扫描</button><button class="btn" onclick="showReport()">查看报告</button></div></section>${quickNav}<div id="overview">${overviewBlock}</div>${fundamentalBlock}<section class="card category-panel"><div class="category-head"><div><h2>战法分类导航</h2><p>数字是当日命中该分类的股票数；一只股票可同时命中多个分类，因此分类数量不能相加当作候选总数。</p></div><span class="pill">已收录 ${(d.strategy_book||[]).reduce((n,b)=>n+(b.items||[]).length,0)} 种战法</span></div><div class="category-grid">${categoryCards}</div></section><section class="card strategy-panel ${strategyExpanded?'':'collapsed'}" id="strategies"><div class="category-head"><div><h2>具体战法导航</h2><p>按具体战法筛选当日信号，零命中也会保留，便于确认战法是否上线。</p></div><div class="switches"><button class="filter-btn" onclick="strategyExpanded=!strategyExpanded;render()">${strategyExpanded?'收起战法':'展开18种战法'}</button><button class="filter-btn ${activeStrategy==='全部'?'active':''}" onclick="active='全部';activeStrategy='全部';showAllRows=false;render()">查看全部</button></div></div><div class="strategy-groups">${strategyNav}</div></section><section class="grid"><div><div class="card" id="feedback">${feedbackBlock}</div><div class="card" id="candidates" style="margin-top:16px"><div class="category-head"><div><h2>候选列表</h2><p>默认只展示前20条，先复核核心/优先对象；表头与代码列已固定，横向滚动可看完整风控字段。</p></div><span class="pill">当前 ${filtered.length} 条</span></div><div class="tabs">${tabs}</div><div class="toolbar"><input class="search" placeholder="搜索代码/名称/战法/理由" value="${esc(q)}" oninput="q=this.value;showAllRows=false;render()"><div class="switches"><button class="filter-btn ${priorityOnly?'active':''}" onclick="priorityOnly=!priorityOnly;showAllRows=false;render()">只看核心/优先</button><button class="filter-btn ${showAllRows?'active':''}" onclick="showAllRows=!showAllRows;render()">${showAllRows?'收起到前20':'展示全部'}</button><button class="filter-btn ${compactTable?'active':''}" onclick="compactTable=!compactTable;render()">${compactTable?'决策视图':'完整字段'}</button><span class="pill">显示 ${displayRows.length}/${filtered.length}</span><span class="pill">数据 ${esc(d.signal_file)}</span></div></div><div class="table-wrap"><table class="${compactTable?'compact-table':''}"><thead><tr><th>#</th><th>代码</th><th>名称</th><th>等级</th><th>分类</th><th>战法</th><th>规则评分</th><th>RPS</th><th class="detail-col">RPS60</th><th>收盘</th><th class="detail-col">涨跌</th><th class="detail-col">换手</th><th>买入区间</th><th>止损</th><th class="detail-col">目标</th><th class="detail-col">盈亏比</th><th class="detail-col">风险标签</th><th class="detail-col">执行层级</th><th>执行动作</th><th>理由</th></tr></thead><tbody>${trs||'<tr><td colspan="20" class="muted">当前筛选条件没有候选</td></tr>'}</tbody></table></div></div></div><aside class="side"><div class="card"><h3>分类龙头/组内最高分</h3><div class="leader">${leaders||'<span class="muted">暂无</span>'}</div></div><div class="card"><h3>日常使用顺序</h3><ol class="usage-steps"><li><b>先看市场状态</b>，弱势优先等待。</li><li><b>再看执行层级</b>，不把信号日当买点。</li><li><b>复核基本面</b>，排除明显财务风险。</li><li><b>定义止损和风险预算</b>，避免同题材集中。</li><li><b>每周看历史反馈</b>，不凭一两次结果评价战法。</li></ol></div><div class="card"><h3>运行状态</h3>${statusHtml(d.run_state)}</div><div class="card"><h3>历史扫描</h3><div class="hist">${reports||'<span class="muted">暂无历史</span>'}</div></div></aside></section><div class="footer">免责声明：本系统只做量化候选筛选、基本面信息整理和复盘反馈，不保证收益或胜率，不构成投资建议。任何交易需自行判断并严格止损。</div>`}
function statusHtml(s){s=s||{};return `<div class="small"><div>状态：<b>${s.running?'运行中':'空闲'}</b></div><div>开始：${esc(s.last_started||'-')}</div><div>结束：${esc(s.last_finished||'-')}</div><div>退出码：${esc(s.last_returncode??'-')}</div><div class="muted">日志：${esc(s.last_log||'-')}</div><div style="color:#ff9b9b">${esc(s.last_error||'')}</div></div>`}
async function runScan(){let url='/api/run?full=1&top=80';const token=getAdminToken();const opt={method:'POST',headers:{}};if(token)opt.headers['X-Quant-Token']=token;const r=await fetch(url,opt);if(r.status===401){const t=prompt('请输入扫描令牌');if(t){setAdminToken(t);return runScan();}return;}const d=await r.json();alert(d.message||'已提交');setTimeout(()=>location.reload(),1600)}

function monitorRowCode(row){const direct=(row.querySelector('.code-link')?.textContent||'').trim();if(/^(?:00|30|60|68)\d{4}$/.test(direct))return direct;const m=(row.innerText||'').match(/(?:^|\s)((?:00|30|60|68)\d{4})(?=\s|$)/);return m?m[1]:''}
function monitorSaveState(text,kind=''){const el=document.getElementById('monSaveState');if(el){el.textContent=text;el.dataset.kind=kind}}
function saveMonitor(){monitorPending=true;monitorSaveVersion++;monitorSaveState('保存中…','saving');clearTimeout(monitorSaveTimer);monitorSaveTimer=setTimeout(flushMonitorSave,250)}
async function flushMonitorSave(){if(monitorSaving||!monitorPending)return;monitorSaving=true;try{while(monitorPending){monitorPending=false;const version=monitorSaveVersion,codes=[...monitorCodes],h={'Content-Type':'application/json'};const token=getAdminToken();if(token)h['X-Quant-Token']=token;let r;try{r=await fetch('/api/watchlist',{method:'POST',headers:h,body:JSON.stringify({mode:'replace',codes})})}catch(e){monitorSaveState('网络异常，未保存','error');alert('监控清单保存失败：网络异常');break}if(r.status===401){const t=prompt('请输入管理令牌');if(!t){monitorSaveState('未保存','error');break}setAdminToken(t);monitorPending=true;continue}const d=await r.json();if(!r.ok||!d.ok){monitorSaveState('保存失败','error');alert(d.message||'保存失败');break}if(version===monitorSaveVersion&&!monitorPending){monitorCodes=new Set((d.watchlist||{}).codes||[]);window.__DATA__.watchlist=d.watchlist||{};monitorSaveState('已保存','ok');decorateMonitors()}}}finally{monitorSaving=false;if(monitorPending){clearTimeout(monitorSaveTimer);monitorSaveTimer=setTimeout(flushMonitorSave,50)}}}
async function setMonitorAutoSync(enabled){const h={'Content-Type':'application/json'};const token=getAdminToken();if(token)h['X-Quant-Token']=token;let r;try{r=await fetch('/api/watchlist',{method:'POST',headers:h,body:JSON.stringify({mode:'auto_sync',auto_sync:{enabled:enabled}})})}catch(e){alert('网络异常，自动同步设置未保存');return}if(r.status===401){const t=prompt('请输入管理令牌');if(!t){decorateMonitors();return}setAdminToken(t);return setMonitorAutoSync(enabled)}const d=await r.json();if(!r.ok||!d.ok){alert(d.message||'自动同步设置失败');decorateMonitors();return}window.__DATA__.watchlist=d.watchlist||{};monitorCodes=new Set((d.watchlist||{}).codes||[]);monitorAutoSync=!!(((d.watchlist||{}).auto_sync||{}).enabled);monitorDraft.clear();decorateMonitors()}
function monitorVisible(add){[...document.querySelectorAll('#candidates tbody tr')].map(monitorRowCode).filter(Boolean).forEach(c=>add?monitorDraft.add(c):monitorDraft.delete(c));decorateMonitors()}
function applyMonitorDraft(add){if(!monitorDraft.size){alert('请先勾选至少一只股票');return}for(const code of monitorDraft)add?monitorCodes.add(code):monitorCodes.delete(code);monitorDraft.clear();decorateMonitors();saveMonitor()}
function decorateMonitors(){if(monitorDecorating)return;const app=document.getElementById('app'),card=document.getElementById('candidates'),table=card&&card.querySelector('table'),toolbar=card&&card.querySelector('.toolbar');if(!table||!toolbar)return;monitorDecorating=true;if(monitorObserver)monitorObserver.disconnect();try{let bar=card.querySelector('.monitor-toolbar');if(!bar){bar=document.createElement('div');bar.className='monitor-toolbar';toolbar.insertAdjacentElement('afterend',bar)}const oldState=(document.getElementById('monSaveState')||{}).textContent||'已同步';bar.innerHTML=`<b>早盘建仓监控</b><span class="small muted">监控中 ${monitorCodes.size} 只｜已勾选 ${monitorDraft.size} 只</span><label class="monitor-auto" title="开启后，每日收盘扫描成功即自动同步前12只高分候选（纯涨停情绪类除外）；不会覆盖你手动添加的股票。"><input type="checkbox" id="monAuto" ${monitorAutoSync?'checked':''}> 自动同步今日高分股</label><span id="monSaveState" class="monitor-save-state">${oldState}</span><button class="filter-btn" id="monAll">全选当前</button><button class="filter-btn" id="monNo">取消勾选</button><button class="filter-btn monitor-add" id="monAdd">＋ 添加所选到监控</button><button class="filter-btn" id="monRemove">移除所选</button><button class="filter-btn" id="monClear">清空手动监控</button><button class="filter-btn" id="monToken">清除管理令牌</button><a class="filter-btn" href="/opening">前往早盘监控（${monitorCodes.size}）</a><a class="filter-btn" href="/positions">交易复盘台</a>`;bar.querySelector('#monAuto').onchange=e=>setMonitorAutoSync(e.target.checked);bar.querySelector('#monAll').onclick=()=>monitorVisible(true);bar.querySelector('#monNo').onclick=()=>monitorVisible(false);bar.querySelector('#monAdd').onclick=()=>applyMonitorDraft(true);bar.querySelector('#monRemove').onclick=()=>applyMonitorDraft(false);bar.querySelector('#monToken').onclick=clearAdminToken;bar.querySelector('#monClear').onclick=()=>{if(confirm('清空后将隐藏当前自动候选，下一交易日按开关重新同步，是否继续？')){monitorCodes.clear();monitorDraft.clear();decorateMonitors();saveMonitor()}};let head=table.querySelector('thead tr');if(head&&!head.querySelector('[data-mh]')){let th=document.createElement('th');th.dataset.mh='1';th.textContent='勾选操作';head.insertBefore(th,head.firstChild)}for(const row of table.querySelectorAll('tbody tr')){const code=monitorRowCode(row);if(!code)continue;let cell=row.querySelector('[data-mc]');if(!cell){cell=document.createElement('td');cell.dataset.mc='1';row.insertBefore(cell,row.firstChild)}cell.innerHTML=`<label class="monitor-choice"><input class="watch-check" type="checkbox" ${monitorDraft.has(code)?'checked':''}><span>${monitorCodes.has(code)?'已监控':'选择'}</span></label>`;cell.firstChild.querySelector('input').onchange=e=>{e.target.checked?monitorDraft.add(code):monitorDraft.delete(code);decorateMonitors()}}}finally{monitorDecorating=false;if(monitorObserver&&app)monitorObserver.observe(app,{childList:true,subtree:true})}}
function setupMonitorObserver(){const app=document.getElementById('app');if(!app)return;let timer;monitorObserver=new MutationObserver(()=>{clearTimeout(timer);timer=setTimeout(decorateMonitors,30)});monitorObserver.observe(app,{childList:true,subtree:true});decorateMonitors()}

function showReport(){document.getElementById('reportText').textContent=window.__DATA__.report_excerpt||'';document.getElementById('modal').classList.add('show')}function hideReport(){document.getElementById('modal').classList.remove('show')}function openKlineFromUrl(){const url=new URL(location.href),code=url.searchParams.get('kline')||'';if(!/^(?:00|30|60|68)\d{4}$/.test(code))return;url.searchParams.delete('kline');history.replaceState(null,'',url.pathname+(url.search||'')+url.hash);setTimeout(()=>openKline(code),40)}render();setupMonitorObserver();openKlineFromUrl();
'''

class Handler(BaseHTTPRequestHandler):
    server_version="AshareQuantWeb"
    sys_version=""
    def setup(self):
        super().setup()
        self.connection.settimeout(15)
    def client_key(self) -> str:
        return str(self.client_address[0])
    def log_message(self,fmt,*args): sys.stderr.write("%s - - [%s] %s\n"%(self.address_string(),self.log_date_time_string(),fmt%args))
    def end_headers(self):
        # Apply hardening headers to successful responses and BaseHTTPRequestHandler-generated errors alike.
        self.send_header("Cache-Control","no-store"); self.send_header("X-Content-Type-Options","nosniff"); self.send_header("X-Frame-Options","DENY"); self.send_header("Referrer-Policy","no-referrer")
        self.send_header("Permissions-Policy","camera=(), microphone=(), geolocation=(), payment=()")
        self.send_header("Cross-Origin-Opener-Policy","same-origin"); self.send_header("Cross-Origin-Resource-Policy","same-origin"); self.send_header("X-Permitted-Cross-Domain-Policies","none"); self.send_header("X-DNS-Prefetch-Control","off")
        self.send_header("Content-Security-Policy","default-src 'self'; base-uri 'none'; frame-ancestors 'none'; object-src 'none'; form-action 'self'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'")
        super().end_headers()
    def send_body(self,body:bytes,status:int=200,content_type:str="text/html; charset=utf-8",extra_headers:Optional[Dict[str,str]]=None):
        self.send_response(status)
        self.send_header("Content-Type",content_type); self.send_header("Content-Length",str(len(body)))
        for key,value in (extra_headers or {}).items(): self.send_header(key,value)
        self.end_headers()
        try: self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, TimeoutError): pass
    def json(self,obj,status:int=200,extra_headers:Optional[Dict[str,str]]=None): self.send_body(json.dumps(obj,ensure_ascii=False,indent=2).encode(),status,"application/json; charset=utf-8",extra_headers)
    def authorized(self):
        if not WEB_TOKEN: return True
        supplied = self.headers.get("X-Quant-Token","").strip()
        auth = self.headers.get("Authorization","").strip()
        if not supplied and auth.lower().startswith("bearer "): supplied = auth[7:].strip()
        return bool(supplied) and hmac.compare_digest(supplied, WEB_TOKEN)
    def auth_error(self):
        allowed,retry=consume_rate(f"auth:{self.client_key()}",12,300)
        if not allowed: return self.json({"ok":False,"message":"令牌错误次数过多，请稍后再试"},429,{"Retry-After":str(retry)})
        return self.json({"ok":False,"message":"需要有效的管理令牌"},401)
    def read_json_body(self,limit:int=20000) -> Dict[str,Any]:
        if "application/json" not in self.headers.get("Content-Type","").lower(): raise TypeError("请求必须使用 application/json")
        if self.headers.get("Transfer-Encoding"): raise ValueError("不支持 Transfer-Encoding")
        if self.headers.get("Content-Length") is None: raise ValueError("缺少 Content-Length")
        try: length=int(self.headers.get("Content-Length","0"))
        except ValueError: raise ValueError("Content-Length 无效")
        if length < 0 or length > limit: raise OverflowError(f"请求体不能超过 {limit} 字节")
        try: value=json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        except (UnicodeDecodeError,json.JSONDecodeError): raise ValueError("JSON 格式无效")
        if not isinstance(value,dict): raise ValueError("JSON 顶层必须是对象")
        return value
    def do_GET(self):
        u=urlparse(self.path); path=u.path
        if path in ("/","/index.html"): return self.send_body(page_html(load_latest_payload()).encode())
        if path=="/opening/detail":
            allowed,retry=consume_rate(f"opening-detail:{self.client_key()}",20,60)
            global_allowed,global_retry=consume_rate("opening-detail:global",120,60)
            if not allowed or not global_allowed:
                return self.send_body("详情查询过于频繁，请稍后再试".encode(),429,"text/plain; charset=utf-8",{"Retry-After":str(max(retry,global_retry,1))})
            filename = (parse_qs(u.query).get("file") or [""])[0]
            return self.send_body(opening_detail_page_html(opening_detail_payload(filename)).encode())
        if path=="/opening":
            allowed,retry=consume_rate(f"opening-page:{self.client_key()}",30,60)
            global_allowed,global_retry=consume_rate("opening-page:global",180,60)
            if not allowed or not global_allowed:
                return self.send_body("刷新过于频繁，请稍后再试".encode(),429,"text/plain; charset=utf-8",{"Retry-After":str(max(retry,global_retry,1))})
            return self.send_body(opening_page_html(opening_check_payload()).encode())
        if path in ("/positions", "/journal"):
            allowed,retry=consume_rate(f"journal-page:{self.client_key()}",60,60)
            if not allowed: return self.send_body("刷新过于频繁，请稍后再试".encode(),429,"text/plain; charset=utf-8",{"Retry-After":str(retry)})
            return self.send_body(trade_journal_page_html().encode())
        if path=="/api/trades":
            if not self.authorized(): return self.auth_error()
            allowed,retry=consume_rate(f"trades-get:{self.client_key()}",60,60)
            if not allowed: return self.json({"ok":False,"message":"交易复盘查询过于频繁"},429,{"Retry-After":str(retry)})
            return self.json(trade_journal_payload())
        if path=="/api/kline":
            allowed,retry=consume_rate(f"kline:{self.client_key()}",60,300)
            global_allowed,global_retry=consume_rate("kline:global",300,60)
            if not allowed or not global_allowed:
                return self.json({"ok":False,"message":"K线查询过于频繁，请稍后再试"},429,{"Retry-After":str(max(retry,global_retry,1))})
            try:
                qs=parse_qs(u.query); code=(qs.get("code") or [""])[0]; period=(qs.get("period") or ["day"])[0]; limit=(qs.get("limit") or ["120"])[0]; force=(qs.get("refresh") or ["0"])[0] == "1"
                return self.json(kline_payload(code,period,limit,force_refresh=force))
            except ValueError as exc: return self.json({"ok":False,"message":str(exc)},400)
            except Exception: traceback.print_exc(); return self.json({"ok":False,"message":"K线数据暂不可用，请稍后重试"},503)
        if path=="/api/fundamentals":
            allowed,retry=consume_rate(f"fund:{self.client_key()}",30,300)
            global_allowed,global_retry=consume_rate("fund:global",180,60)
            if not allowed or not global_allowed:
                return self.json({"ok":False,"message":"基本面查询过于频繁，请稍后再试"},429,{"Retry-After":str(max(retry,global_retry,1))})
            try:
                code = (parse_qs(u.query).get("code") or [""])[0]
                return self.json(fundamental_payload(code))
            except ValueError as e:
                return self.json({"ok": False, "message": str(e)}, 400)
            except Exception as e:
                traceback.print_exc()
                return self.json({"ok": False, "message": "基本面数据暂不可用，请稍后重试"}, 503)
        if path=="/healthz": return self.json({"ok":True,"service":"quant-web","time":now_cn().strftime("%Y-%m-%d %H:%M:%S"),"scan_running":bool(public_run_state(RUN_STATE, RUN_LOCK).get("running"))})
        if path=="/api/latest": return self.json(load_latest_payload())
        if path=="/api/opening":
            allowed,retry=consume_rate(f"opening:{self.client_key()}",60,60)
            if not allowed: return self.json({"ok":False,"message":"开盘核验刷新过于频繁，请稍后再试"},429,{"Retry-After":str(retry)})
            return self.json(opening_check_payload())
        if path=="/api/opening/history":
            allowed,retry=consume_rate(f"opening-history:{self.client_key()}",60,60)
            if not allowed: return self.json({"ok":False,"message":"核验轨迹查询过于频繁"},429,{"Retry-After":str(retry)})
            return self.json({"ok": True, "items": opening_history_payload()})
        if path=="/api/watchlist":
            allowed,retry=consume_rate(f"watchlist-get:{self.client_key()}",120,60)
            if not allowed: return self.json({"ok":False,"message":"监控清单查询过于频繁"},429,{"Retry-After":str(retry)})
            return self.json({"ok": True, "watchlist": watchlist_payload()})
        if path=="/api/reports": return self.json(all_reports())
        if path.startswith("/reports/"):
            name=Path(unquote(path.split("/reports/",1)[1])).name; p=REPORT_DIR/name
            allowed = bool(re.match(r"^(?:report_\d{8}\.md|signals_\d{8}\.csv)$",name))
            if not allowed or not p.exists() or not p.is_file() or p.stat().st_size > 10 * 1024 * 1024: return self.send_error(404,"report not found")
            ctype="text/markdown; charset=utf-8" if p.suffix.lower()==".md" else "text/csv; charset=utf-8"
            return self.send_body(p.read_bytes(),200,ctype)
        return self.send_error(404,"not found")
    def do_POST(self):
        u=urlparse(self.path)
        if u.path=="/api/trades":
            if not self.authorized(): return self.auth_error()
            allowed,retry=consume_rate(f"trades-post:{self.client_key()}",40,60)
            if not allowed: return self.json({"ok":False,"message":"交易复盘操作过于频繁"},429,{"Retry-After":str(retry)})
            try:
                body=self.read_json_body(limit=12000)
                return self.json(trade_journal_action(body))
            except TypeError as exc: return self.json({"ok":False,"message":str(exc)},415)
            except OverflowError as exc: return self.json({"ok":False,"message":str(exc)},413)
            except ValueError as exc: return self.json({"ok":False,"message":str(exc)},400)
            except Exception: traceback.print_exc(); return self.json({"ok":False,"message":"交易复盘保存失败，请查看服务日志"},503)
        if u.path=="/api/watchlist":
            if not self.authorized(): return self.auth_error()
            try:
                body=self.read_json_body()
                mode=str(body.get("mode") or "replace")
                if mode == "auto_sync":
                    watch = set_auto_sync(body.get("auto_sync") or {"enabled": body.get("enabled", False)})
                    return self.json({"ok":True,"message":"自动同步设置已更新","watchlist":watch})
                watch = update_watch_codes(mode, body.get("codes") or [])
                return self.json({"ok":True,"message":"早盘监控清单已更新","watchlist":watch})
            except TypeError as exc: return self.json({"ok":False,"message":str(exc)},415)
            except OverflowError as exc: return self.json({"ok":False,"message":str(exc)},413)
            except ValueError as exc: return self.json({"ok":False,"message":str(exc)},400)
            except Exception as exc: traceback.print_exc(); return self.json({"ok":False,"message":"保存失败，请查看服务日志"},503)
        if u.path=="/api/run":
            if not self.authorized(): return self.auth_error()
            try:
                qs=parse_qs(u.query); top=max(1,min(200,int((qs.get("top") or [DEFAULT_TOP])[0]))); max_stocks=max(1,min(6000,int((qs.get("max_stocks") or [DEFAULT_MAX_STOCKS])[0]))); full=(qs.get("full") or ["1" if DEFAULT_FULL else "0"])[0].lower() in ("1","true","yes"); workers=max(1,min(32,int((qs.get("workers") or [DEFAULT_WORKERS])[0])))
            except (TypeError,ValueError): return self.json({"ok":False,"message":"扫描参数无效"},400)
            ok=run_scan_background(top=top,max_stocks=max_stocks,full=full,workers=workers)
            return self.json({"ok":ok,"message":"已开始后台全市场扫描，完成后刷新查看。" if ok else "已有扫描正在运行。","state":public_run_state(RUN_STATE, RUN_LOCK)})
        return self.send_error(404,"not found")

class HardenedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._request_slots = threading.BoundedSemaphore(MAX_CONCURRENT_REQUESTS)

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._request_slots.acquire(blocking=False):
            try:
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\nRetry-After: 3\r\nContent-Length: 18\r\n\r\nserver busy, retry")
            except OSError:
                pass
            self.close_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._request_slots.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


def main()->int:
    ap=argparse.ArgumentParser(); ap.add_argument("--host",default=os.environ.get("QUANT_WEB_HOST","0.0.0.0")); ap.add_argument("--port",type=int,default=int(os.environ.get("QUANT_WEB_PORT","8766"))); ap.add_argument("--no-scheduler",action="store_true"); ap.add_argument("--run-on-start",action="store_true")
    args=ap.parse_args()
    if not ENGINE.exists(): print(f"缺少量化脚本：{ENGINE}",file=sys.stderr); return 2
    if not WEB_TOKEN and args.host not in ("127.0.0.1","localhost","::1"):
        print("拒绝在未配置 QUANT_WEB_TOKEN 时监听公网地址；本地测试请使用 --host 127.0.0.1。",file=sys.stderr)
        return 3
    if args.run_on_start and not latest_signal_file(): run_scan_background()
    if not args.no_scheduler:
        threading.Thread(target=scheduler_loop,args=(DEFAULT_SCHEDULE,),daemon=True).start()
        threading.Thread(target=opening_scheduler_loop,args=(OPENING_SCHEDULE,),daemon=True).start()
    httpd=HardenedThreadingHTTPServer((args.host,args.port),Handler)
    print(f"A股量化推荐网站已启动：http://{args.host}:{args.port}")
    print(f"报告目录：{REPORT_DIR}")
    print(f"收盘扫描：{DEFAULT_SCHEDULE}；开盘核验：{OPENING_SCHEDULE}；默认全市场：{DEFAULT_FULL}；token：{'已开启' if WEB_TOKEN else '未开启'}")
    try: httpd.serve_forever()
    except KeyboardInterrupt: print('bye')
    return 0
if __name__=='__main__': raise SystemExit(main())
