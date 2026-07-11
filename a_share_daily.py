# A股每日技术战法筛选脚本
# 用法：python a_share_daily.py --top 30 --max-stocks 800
# 依赖：pip install pandas requests openpyxl

from __future__ import annotations

import argparse
import concurrent.futures as futures
import csv
import datetime as dt
import json
import math
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("QUANT_WEB_DATA_DIR", str(BASE_DIR))).expanduser().resolve()
REPORT_DIR = DATA_DIR / "a_share_daily_reports"
CACHE_DIR = DATA_DIR / ".a_share_cache"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

EASTMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/",
}

SPOT_URL = "https://push2.eastmoney.com/api/qt/clist/get"
KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
SINA_SPOT_URL = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
EASTMONEY_OK = True

INDEX_SECIDS = {
    "上证指数": "1.000001",
    "深证成指": "0.399001",
    "创业板指": "0.399006",
    "沪深300": "1.000300",
    "中证500": "1.000905",
}


def now_cn() -> dt.datetime:
    # Python 3.12: no zoneinfo dependency required for local usage. Use UTC+8.
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) + dt.timedelta(hours=8)


def safe_float(x: Any, default: float = math.nan) -> float:
    if x is None or x == "-":
        return default
    try:
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "-":
            return default
        return int(float(x))
    except Exception:
        return default


def code_to_secid(code: str) -> str:
    code = str(code).zfill(6)
    # 东方财富：沪市/科创板一般 market=1，深市/创业板/北交所常用 market=0
    if code.startswith(("5", "6", "9")):
        return f"1.{code}"
    return f"0.{code}"


def fetch_json(url: str, params: Dict[str, Any], timeout: int = 12, retries: int = 3) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=EASTMONEY_HEADERS, timeout=timeout)
            r.raise_for_status()
            text = r.text.strip()
            # Some Eastmoney endpoints may wrap JSONP; this endpoint usually does not.
            if text.startswith("jQuery") or text.startswith("var "):
                text = text[text.find("{") : text.rfind("}") + 1]
            return json.loads(text)
        except Exception as e:
            last_err = e
            time.sleep(0.6 * (i + 1))
    raise RuntimeError(f"fetch failed: {url} params={params} err={last_err}")


def fetch_spot_all_eastmoney() -> pd.DataFrame:
    fields = "f12,f14,f2,f3,f4,f5,f6,f7,f8,f9,f10,f15,f16,f17,f18,f20,f21,f23,f24,f25,f62"
    # 沪深A股：深A/创业板、沪A/科创板。可按需补北交所。
    fs = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
    all_rows: List[Dict[str, Any]] = []
    page = 1
    page_size = 5000
    while True:
        params = {
            "pn": page,
            "pz": page_size,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f6",
            "fs": fs,
            "fields": fields,
        }
        js = fetch_json(SPOT_URL, params)
        data = js.get("data") or {}
        rows = data.get("diff") or []
        all_rows.extend(rows)
        total = safe_int(data.get("total"), len(all_rows))
        if not rows or len(all_rows) >= total:
            break
        page += 1
    records = []
    for r in all_rows:
        records.append(
            {
                "code": str(r.get("f12", "")).zfill(6),
                "name": r.get("f14", ""),
                "price": safe_float(r.get("f2")),
                "pct": safe_float(r.get("f3")),
                "change": safe_float(r.get("f4")),
                "volume": safe_float(r.get("f5")),
                "amount": safe_float(r.get("f6")),
                "amplitude": safe_float(r.get("f7")),
                "turnover": safe_float(r.get("f8")),
                "pe": safe_float(r.get("f9")),
                "vol_ratio": safe_float(r.get("f10")),
                "high": safe_float(r.get("f15")),
                "low": safe_float(r.get("f16")),
                "open": safe_float(r.get("f17")),
                "prev_close": safe_float(r.get("f18")),
                "total_mv": safe_float(r.get("f20")),
                "float_mv": safe_float(r.get("f21")),
                "pb": safe_float(r.get("f23")),
                "ret_60d": safe_float(r.get("f24")),
                "ret_ytd": safe_float(r.get("f25")),
                "main_net": safe_float(r.get("f62")),
            }
        )
    return pd.DataFrame(records)



def fetch_spot_all_sina() -> pd.DataFrame:
    """新浪行情中心备用数据源：用于东方财富接口不可用时。市值字段单位为万元，这里统一转为元。"""
    all_rows: List[Dict[str, Any]] = []
    page = 1
    page_size = 100
    while True:
        params = {
            "page": page,
            "num": page_size,
            "sort": "amount",
            "asc": 0,
            "node": "hs_a",
            "symbol": "",
            "_s_r_a": "page",
        }
        try:
            r = requests.get(SINA_SPOT_URL, params=params, headers={"User-Agent": EASTMONEY_HEADERS["User-Agent"], "Referer": "https://finance.sina.com.cn/"}, timeout=12)
            r.raise_for_status()
            rows = json.loads(r.text)
        except Exception as e:
            raise RuntimeError(f"sina spot fetch failed page={page}: {e}")
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        page += 1
        if page > 80:  # A股约五千多只，防止接口异常无限翻页
            break
    records = []
    for r in all_rows:
        prev = safe_float(r.get("settlement"))
        trade = safe_float(r.get("trade"))
        high = safe_float(r.get("high"))
        low = safe_float(r.get("low"))
        amp = (high - low) / prev * 100 if prev and not pd.isna(prev) else math.nan
        records.append(
            {
                "code": str(r.get("code", "")).zfill(6),
                "name": r.get("name", ""),
                "price": trade,
                "pct": safe_float(r.get("changepercent")),
                "change": safe_float(r.get("pricechange")),
                "volume": safe_float(r.get("volume")),
                "amount": safe_float(r.get("amount")),
                "amplitude": amp,
                "turnover": safe_float(r.get("turnoverratio")),
                "pe": safe_float(r.get("per")),
                "vol_ratio": math.nan,
                "high": high,
                "low": low,
                "open": safe_float(r.get("open")),
                "prev_close": prev,
                "total_mv": safe_float(r.get("mktcap")) * 10000,
                "float_mv": safe_float(r.get("nmc")) * 10000,
                "pb": safe_float(r.get("pb")),
                "ret_60d": math.nan,
                "ret_ytd": math.nan,
                "main_net": 0.0,
            }
        )
    return pd.DataFrame(records)


def fetch_spot_all() -> pd.DataFrame:
    global EASTMONEY_OK
    try:
        df = fetch_spot_all_eastmoney()
        EASTMONEY_OK = True
        return df
    except Exception as e:
        print(f"[提示] 东方财富实时列表接口暂不可用，切换到新浪行情备用源：{e}", file=sys.stderr)
        EASTMONEY_OK = False
        return fetch_spot_all_sina()

def parse_kline_rows(rows: List[str]) -> pd.DataFrame:
    parsed = []
    for line in rows:
        parts = line.split(",")
        if len(parts) < 11:
            continue
        parsed.append(
            {
                "date": pd.to_datetime(parts[0]),
                "open": safe_float(parts[1]),
                "close": safe_float(parts[2]),
                "high": safe_float(parts[3]),
                "low": safe_float(parts[4]),
                "volume": safe_float(parts[5]),
                "amount": safe_float(parts[6]),
                "amplitude": safe_float(parts[7]),
                "pct": safe_float(parts[8]),
                "change": safe_float(parts[9]),
                "turnover": safe_float(parts[10]),
            }
        )
    df = pd.DataFrame(parsed)
    if not df.empty:
        df = df.sort_values("date").reset_index(drop=True)
    return df



def market_code_for_tencent(code: str, is_index: bool = False) -> str:
    if is_index:
        # 腾讯指数代码前缀：上证/沪深300/中证500为sh，深成指/创业板为sz
        secid = INDEX_SECIDS.get(code, code)
        idx_code = secid.split(".")[-1]
        return ("sh" if secid.startswith("1.") else "sz") + idx_code
    code = str(code).zfill(6)
    return ("sh" if code.startswith(("5", "6", "9")) else "sz") + code


def parse_tencent_kline(js: Dict[str, Any], tcode: str) -> pd.DataFrame:
    data = (js.get("data") or {}).get(tcode) or {}
    rows = data.get("qfqday") or data.get("day") or []
    parsed = []
    prev_close = math.nan
    for r in rows:
        if len(r) < 6:
            continue
        d, open_, close, high, low, vol = r[:6]
        open_f, close_f, high_f, low_f, vol_f = map(safe_float, [open_, close, high, low, vol])
        pct = (close_f / prev_close - 1) * 100 if prev_close and not pd.isna(prev_close) else math.nan
        change = close_f - prev_close if prev_close and not pd.isna(prev_close) else math.nan
        amp = (high_f - low_f) / prev_close * 100 if prev_close and not pd.isna(prev_close) else math.nan
        parsed.append(
            {
                "date": pd.to_datetime(d),
                "open": open_f,
                "close": close_f,
                "high": high_f,
                "low": low_f,
                "volume": vol_f,
                "amount": math.nan,
                "amplitude": amp,
                "pct": pct,
                "change": change,
                "turnover": math.nan,
            }
        )
        prev_close = close_f
    df = pd.DataFrame(parsed)
    if not df.empty:
        df = df.sort_values("date").reset_index(drop=True)
    return df


def fetch_kline_tencent(code: str, days: int = 190, is_index: bool = False) -> pd.DataFrame:
    tcode = market_code_for_tencent(code, is_index=is_index)
    # qfqday 不支持指数时会自动返回 day；保留更长一点方便指标滚动。
    params = {"param": f"{tcode},day,,,{max(days + 30, 220)},qfq"}
    r = requests.get(TENCENT_KLINE_URL, params=params, headers={"User-Agent": EASTMONEY_HEADERS["User-Agent"], "Referer": "https://gu.qq.com/"}, timeout=8)
    r.raise_for_status()
    js = json.loads(r.text)
    if js.get("code") not in (0, "0"):
        raise RuntimeError(f"tencent kline failed: {js.get('msg')}")
    return parse_tencent_kline(js, tcode).tail(days).reset_index(drop=True)


def fetch_kline_eastmoney(secid: str, days: int = 190) -> pd.DataFrame:
    end = (now_cn().date() + dt.timedelta(days=3)).strftime("%Y%m%d")
    beg = (now_cn().date() - dt.timedelta(days=420)).strftime("%Y%m%d")
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101,
        "fqt": 1,
        "beg": beg,
        "end": end,
    }
    js = fetch_json(KLINE_URL, params, timeout=7, retries=2)
    data = js.get("data") or {}
    rows = data.get("klines") or []
    return parse_kline_rows(rows).tail(days).reset_index(drop=True)


def fetch_kline(code: str, days: int = 190, use_cache: bool = True, is_index: bool = False) -> pd.DataFrame:
    secid = INDEX_SECIDS.get(code, code_to_secid(code)) if not is_index else INDEX_SECIDS.get(code, code)
    # K线优先用东方财富；即使实时列表接口失败，历史K线接口通常仍可用。
    today = now_cn().date().isoformat()
    cache_paths = [CACHE_DIR / f"kline_{re.sub(r'[^0-9A-Za-z_.-]', '_', provider + '_' + secid)}.csv" for provider in ["em", "tx"]]
    if use_cache:
        for cache_path in cache_paths:
            if cache_path.exists():
                try:
                    mtime = dt.datetime.fromtimestamp(cache_path.stat().st_mtime).date().isoformat()
                    if mtime == today:
                        df = pd.read_csv(cache_path, parse_dates=["date"])
                        if len(df) >= min(60, days // 2):
                            return df.tail(days).reset_index(drop=True)
                except Exception:
                    pass
    provider = "em"
    try:
        df = fetch_kline_eastmoney(secid, days=days)
    except Exception:
        provider = "tx"
        df = fetch_kline_tencent(code, days=days, is_index=is_index)
    if not df.empty:
        cache_path = CACHE_DIR / f"kline_{re.sub(r'[^0-9A-Za-z_.-]', '_', provider + '_' + secid)}.csv"
        try:
            df.to_csv(cache_path, index=False, encoding="utf-8-sig")
        except Exception:
            pass
    return df.tail(days).reset_index(drop=True)

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for n in [5, 10, 20, 30, 60, 120, 200]:
        df[f"ma{n}"] = df["close"].rolling(n).mean()
        df[f"vma{n}"] = df["volume"].rolling(n).mean()
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.rolling(14).mean()
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, math.nan)
    df["rsi14"] = 100 - 100 / (1 + rs)
    mid = df["close"].rolling(20).mean()
    std = df["close"].rolling(20).std()
    df["boll_mid"] = mid
    df["boll_upper"] = mid + 2 * std
    df["boll_lower"] = mid - 2 * std
    df["hh20_prev"] = df["high"].shift(1).rolling(20).max()
    df["ll20_prev"] = df["low"].shift(1).rolling(20).min()
    df["hh60_prev"] = df["high"].shift(1).rolling(60).max()
    df["hh120_prev"] = df["high"].shift(1).rolling(120).max()
    df["hh250_prev"] = df["high"].shift(1).rolling(min(250, max(2, len(df)-1))).max()
    df["ll60_prev"] = df["low"].shift(1).rolling(60).min()
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd_diff"] = ema12 - ema26
    df["macd_dea"] = df["macd_diff"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = 2 * (df["macd_diff"] - df["macd_dea"])
    df["range20_pct"] = (df["high"].rolling(20).max() / df["low"].rolling(20).min() - 1) * 100
    df["range60_pct"] = (df["high"].rolling(60).max() / df["low"].rolling(60).min() - 1) * 100
    return df


def is_bad_name(name: str) -> bool:
    """排除风险警示、退市整理及上市初期 N/C 前缀，避免误伤名称中偶含字母 N 的公司。"""
    text = re.sub(r"\s+", "", str(name or "")).upper()
    return text.startswith(("*ST", "ST", "N", "C", "退")) or "退市" in text


def limit_threshold(code: str, name: str) -> float:
    # 约略处理：创业板/科创板20%，普通A股10%；ST通常5%，但已排除。
    if str(code).startswith(("300", "301", "688", "689")):
        return 19.0
    return 9.3




def strategy_group_name(strategy: str) -> str:
    """把市面常见战法归到可展示/可复盘的几类。"""
    if any(k in strategy for k in ["一剑封喉", "仙人指路", "老鸭头", "红三兵", "反包"]):
        return "K线形态类"
    if any(k in strategy for k in ["平台", "唐奇安", "VCP", "突破"]):
        return "突破类"
    if any(k in strategy for k in ["回踩", "低吸", "首阴", "回马枪"]):
        return "回踩低吸类"
    if any(k in strategy for k in ["多头", "动量", "RPS", "新高"]):
        return "趋势动量类"
    if any(k in strategy for k in ["首板", "涨停", "龙头"]):
        return "涨停情绪类"
    if any(k in strategy for k in ["超跌", "反弹", "布林"]):
        return "超跌反转类"
    return "综合类"

def fmt_money(x: float) -> str:
    if pd.isna(x):
        return "-"
    if abs(x) >= 1e8:
        return f"{x/1e8:.1f}亿"
    if abs(x) >= 1e4:
        return f"{x/1e4:.0f}万"
    return f"{x:.0f}"


def pct_dist(a: float, b: float) -> float:
    if not b or pd.isna(a) or pd.isna(b):
        return math.nan
    return (a / b - 1) * 100


def market_regime(use_cache: bool = True) -> Tuple[str, float, List[str]]:
    notes = []
    scores = []
    for name, secid in INDEX_SECIDS.items():
        try:
            df = fetch_kline(secid, days=160, use_cache=use_cache, is_index=True)
            if len(df) < 80:
                continue
            df = add_indicators(df)
            last = df.iloc[-1]
            trend = 0.0
            if last.close > last.ma20:
                trend += 0.5
            if last.ma20 > last.ma60:
                trend += 0.7
            if last.close > last.ma60:
                trend += 0.5
            if len(df) >= 6 and last.close > df.iloc[-6].close:
                trend += 0.3
            if last.close < last.ma20:
                trend -= 0.4
            scores.append(trend)
            notes.append(f"{name}: 收盘{last.close:.2f}, 20日线{last.ma20:.2f}, 60日线{last.ma60:.2f}")
        except Exception as e:
            notes.append(f"{name}: 获取失败({e})")
    avg = sum(scores) / len(scores) if scores else 0
    if avg >= 1.5:
        label = "强势/可积极筛选"
        factor = 1.08
    elif avg >= 0.8:
        label = "中性偏强/正常筛选"
        factor = 1.0
    elif avg >= 0.2:
        label = "震荡/轻仓优先"
        factor = 0.88
    else:
        label = "弱势/防守观察"
        factor = 0.72
    return label, factor, notes



def strategy_regime_multiplier(group: str, market_label: str) -> float:
    """按市场状态调整战法权重；这是候选排序权重，不是收益预测。"""
    group = group or "综合类"
    if "强势" in market_label:
        weights = {"趋势动量类": 1.08, "突破类": 1.10, "回踩低吸类": 1.00, "K线形态类": 1.00, "涨停情绪类": 0.92, "超跌反转类": 0.88}
    elif "中性偏强" in market_label:
        weights = {"趋势动量类": 1.04, "突破类": 1.04, "回踩低吸类": 1.00, "K线形态类": 1.00, "涨停情绪类": 0.88, "超跌反转类": 0.92}
    elif "震荡" in market_label:
        weights = {"趋势动量类": 0.93, "突破类": 0.88, "回踩低吸类": 1.06, "K线形态类": 0.98, "涨停情绪类": 0.72, "超跌反转类": 1.02}
    else:
        weights = {"趋势动量类": 0.74, "突破类": 0.68, "回踩低吸类": 0.80, "K线形态类": 0.75, "涨停情绪类": 0.52, "超跌反转类": 0.88}
    return weights.get(group, 0.90)


def risk_budget_hint(market_label: str, group: str, atr_pct: float) -> str:
    """非个性化的风险预算提示，用于避免把候选清单误当成满仓指令。"""
    if "弱势" in market_label:
        return "防守：优先观望；若自行交易，单笔计划亏损不高于账户0.25%"
    if "震荡" in market_label:
        return "轻仓：单笔计划亏损不高于账户0.35%，避免同题材集中"
    if "中性偏强" in market_label:
        return "常规：单笔计划亏损不高于账户0.50%，单一题材控制集中度"
    hint = "进攻仍控险：单笔计划亏损不高于账户0.60%"
    if group == "涨停情绪类" or atr_pct >= 6.5:
        hint = "高波动：即使强市也按轻仓，单笔计划亏损不高于账户0.35%"
    return hint


def execution_stage(market_label: str, group: str, risk_text: str, close: float, buy_low: float, buy_high: float) -> str:
    if "弱势" in market_label:
        return "防守观察"
    if group == "涨停情绪类" or "接近涨停不追高" in risk_text:
        return "观察确认"
    if close < buy_low or close > buy_high:
        return "等待价格确认"
    if "震荡" in market_label:
        return "轻仓确认后执行"
    return "次日条件确认"


def risk_tags(code: str, name: str, pct_today: float, turnover: float, atr_pct: float, rsi: float) -> str:
    tags = []
    th = limit_threshold(code, name)
    if pct_today >= th - 0.5:
        tags.append("接近涨停不追高")
    if turnover >= 18:
        tags.append("换手过热")
    if atr_pct >= 6.5:
        tags.append("波动偏大")
    if rsi >= 76:
        tags.append("RSI过热")
    if pct_today <= -5:
        tags.append("当日弱势")
    return "、".join(tags) if tags else "常规"

def compute_stock_metrics(hist: pd.DataFrame) -> Dict[str, float]:
    if hist is None or hist.empty or len(hist) < 80:
        return {}
    df = add_indicators(hist)
    last = df.iloc[-1]
    close = safe_float(last.close)
    def ret(n: int) -> float:
        if len(df) <= n or not df.iloc[-(n+1)].close:
            return math.nan
        return close / df.iloc[-(n+1)].close - 1
    high250 = df["high"].tail(min(250, len(df))).max()
    low60 = df["low"].tail(min(60, len(df))).min()
    atr_pct = safe_float(last.atr14) / close * 100 if close else math.nan
    trend_quality = 0.0
    if not pd.isna(last.ma20) and close > last.ma20: trend_quality += 18
    if not pd.isna(last.ma60) and close > last.ma60: trend_quality += 18
    if not pd.isna(last.ma120) and close > last.ma120: trend_quality += 15
    if not pd.isna(last.ma20) and len(df) >= 6 and last.ma20 > df.iloc[-6].ma20: trend_quality += 18
    if not pd.isna(last.ma60) and len(df) >= 11 and last.ma60 > df.iloc[-11].ma60 * 0.995: trend_quality += 12
    if high250 and not pd.isna(high250): trend_quality += max(0, min(12, (close / high250 - 0.86) / 0.14 * 12))
    if low60 and not pd.isna(low60): trend_quality += max(0, min(7, (close / low60 - 1) / 0.28 * 7))
    return {
        "ret20": ret(20), "ret60": ret(60), "ret120": ret(120),
        "near_52w": close / high250 if high250 and not pd.isna(high250) else math.nan,
        "atr_pct": atr_pct, "trend_quality": trend_quality,
    }

def evaluate_stock(row: pd.Series, hist: pd.DataFrame, market_factor: float, market_label: str, ranks: Optional[Dict[str, float]] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    ranks = ranks or {}
    if hist is None or len(hist) < 125:
        return out
    df = add_indicators(hist)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    code, name = str(row["code"]).zfill(6), str(row["name"])

    close = float(last.close)
    if close <= 0 or pd.isna(last.ma20) or pd.isna(last.ma60) or pd.isna(last.atr14):
        return out
    amount = safe_float(row.get("amount", last.amount))
    turnover = safe_float(row.get("turnover", last.turnover))
    pct_today = safe_float(row.get("pct", last.pct))
    vol_ratio20 = float(last.volume / last.vma20) if last.vma20 and not pd.isna(last.vma20) else math.nan
    atr_pct = float(last.atr14 / close * 100) if close else math.nan
    rsi = float(last.rsi14) if not pd.isna(last.rsi14) else 50.0
    ret20 = close / df.iloc[-21].close - 1 if len(df) >= 21 and df.iloc[-21].close else math.nan
    ret60 = close / df.iloc[-61].close - 1 if len(df) >= 61 and df.iloc[-61].close else math.nan
    ret120 = close / df.iloc[-121].close - 1 if len(df) >= 121 and df.iloc[-121].close else math.nan
    high120_prev = df["high"].shift(1).rolling(120).max().iloc[-1] if len(df) >= 121 else math.nan
    high250_proxy = df["high"].shift(1).rolling(min(250, len(df)-1)).max().iloc[-1] if len(df) >= 121 else math.nan
    near_52w_high = close / high250_proxy if high250_proxy and not pd.isna(high250_proxy) else math.nan
    rps20 = safe_float(ranks.get("rps20"), math.nan)
    rps60 = safe_float(ranks.get("rps60"), math.nan)
    rps120 = safe_float(ranks.get("rps120"), math.nan)
    rps_combo = safe_float(ranks.get("rps_combo"), math.nan)
    trend_quality = safe_float(ranks.get("trend_quality"), 50.0)

    # 流动性与风险基础分：流动性充足、波动不过大更适合普通人执行。
    liquidity_score = min(12, max(0, math.log10(max(amount, 1)) - 7) * 8)  # 1亿约8分
    if turnover < 0.8:
        liquidity_score -= 3
    if turnover > 18:
        liquidity_score -= 4
    risk_penalty = 0
    if atr_pct > 7:
        risk_penalty += 6
    elif atr_pct > 5:
        risk_penalty += 3
    if pct_today > limit_threshold(code, name) - 0.3:
        risk_penalty += 4  # 当日涨停不盲追，列观察即可
    if pct_today < -6:
        risk_penalty += 5
    if safe_float(row.get("pe")) < 0:
        risk_penalty += 2

    def add_signal(strategy: str, base: float, reason: List[str], buy_low: float, buy_high: float,
                   stop: float, target: float, action: str = "观察/次日按条件执行",
                   group: Optional[str] = None) -> None:
        resolved_group = group or strategy_group_name(strategy)
        regime_multiplier = strategy_regime_multiplier(resolved_group, market_label)
        score = (base + liquidity_score - risk_penalty) * market_factor * regime_multiplier
        rr = (target - close) / max(close - stop, 0.01)
        if rr < 1.1:
            score -= 5
        risk_text = risk_tags(code, name, pct_today, turnover, atr_pct, rsi)
        stage = execution_stage(market_label, resolved_group, risk_text, close, buy_low, buy_high)
        out.append(
            {
                "date": last.date.date().isoformat(),
                "code": code,
                "name": name,
                "strategy_group": resolved_group,
                "strategy": strategy,
                "score": round(score, 1),
                "action": action,
                "execution_stage": stage,
                "risk_budget_hint": risk_budget_hint(market_label, resolved_group, atr_pct),
                "regime_multiplier": round(regime_multiplier, 2),
                "close": round(close, 3),
                "pct_today": round(pct_today, 2),
                "turnover": round(turnover, 2),
                "amount": round(amount, 2),
                "buy_zone": f"{buy_low:.2f}~{buy_high:.2f}",
                "stop_loss": round(max(stop, 0.01), 3),
                "target": round(target, 3),
                "risk_reward": round(rr, 2),
                "atr_pct": round(atr_pct, 2),
                "rsi14": round(rsi, 1),
                "vol_ratio20": round(vol_ratio20, 2) if not pd.isna(vol_ratio20) else "-",
                "rps20": round(rps20, 1) if not pd.isna(rps20) else "-",
                "rps60": round(rps60, 1) if not pd.isna(rps60) else "-",
                "rps120": round(rps120, 1) if not pd.isna(rps120) else "-",
                "rps_combo": round(rps_combo, 1) if not pd.isna(rps_combo) else "-",
                "trend_quality": round(trend_quality, 1),
                "risk_tags": risk_text,
                "reason": "；".join(reason),
            }
        )

    # 1. 放量平台突破：趋势策略里更强调“价格突破+量能确认+收在高位”。
    near_high = (last.close - last.low) / max(last.high - last.low, 0.01)
    if (
        close > last.hh20_prev * 1.002
        and vol_ratio20 >= 1.45
        and near_high >= 0.62
        and close > last.ma20
        and last.ma20 >= last.ma60 * 0.985
        and amount >= 8e7
        and -1 <= pct_today <= min(9.0, limit_threshold(code, name) - 0.5)
    ):
        base = 68
        if close > last.hh60_prev:
            base += 5
        if last.ma20 > last.ma60:
            base += 4
        add_signal(
            "放量平台突破",
            base,
            [
                f"收盘突破20日高点{last.hh20_prev:.2f}",
                f"量能为20日均量{vol_ratio20:.1f}倍",
                "收盘位于日内相对高位",
            ],
            buy_low=max(last.hh20_prev * 0.995, close * 0.985),
            buy_high=close * 1.015,
            stop=max(last.hh20_prev * 0.965, close - 1.6 * last.atr14),
            target=close + 2.2 * last.atr14,
            action="次日不高开过多时回踩/突破确认",
        )

    # 2. 趋势回踩20日线：多头结构中缩量回踩，等待重新转强。
    low_touch_ma20 = last.low <= last.ma20 * 1.018 and close >= last.ma20 * 0.992
    ma_rising = last.ma20 > df.iloc[-6].ma20 and last.ma60 >= df.iloc[-6].ma60 * 0.995
    not_extended = close <= last.ma20 * 1.055
    if (
        close > last.ma60
        and last.ma20 > last.ma60 * 1.01
        and low_touch_ma20
        and ma_rising
        and not_extended
        and 42 <= rsi <= 66
        and (pd.isna(vol_ratio20) or vol_ratio20 <= 1.35)
        and amount >= 6e7
        and pct_today >= -3.5
    ):
        base = 66
        if last.close > last.open:
            base += 3
        if prev.close < prev.ma20 and last.close >= last.ma20:
            base += 4
        add_signal(
            "趋势回踩20日线",
            base,
            [
                "20/60日均线保持多头",
                f"低点触及20日线{last.ma20:.2f}附近后收回",
                f"RSI={rsi:.0f}未过热",
            ],
            buy_low=last.ma20 * 0.992,
            buy_high=min(close * 1.01, last.ma20 * 1.025),
            stop=min(last.ma60 * 0.985, close - 1.3 * last.atr14),
            target=close + 1.9 * last.atr14,
            action="回踩不破20日线且分时转强",
        )

    # 3. 均线多头动量：强趋势但不过度偏离，适合趋势跟踪观察。
    ma_stack = last.ma5 > last.ma10 > last.ma20 > last.ma60
    if (
        ma_stack
        and last.ma20 > df.iloc[-6].ma20
        and close > last.ma5 * 0.995
        and pct_dist(close, last.ma20) <= 8.5
        and 48 <= rsi <= 72
        and amount >= 8e7
        and pct_today <= min(7.5, limit_threshold(code, name) - 1.0)
    ):
        base = 62
        if close > prev.close and last.volume > last.vma20 * 0.9:
            base += 4
        add_signal(
            "均线多头动量",
            base,
            [
                "5/10/20/60日均线多头排列",
                "20日线向上",
                f"股价距20日线{pct_dist(close, last.ma20):.1f}%",
            ],
            buy_low=max(last.ma5 * 0.985, close * 0.985),
            buy_high=close * 1.008,
            stop=max(last.ma20 * 0.975, close - 1.4 * last.atr14),
            target=close + 1.8 * last.atr14,
            action="趋势未破5/10日线时低吸，不追急拉",
        )

    # 4. 首板强势观察：涨停本身不等于买点，只给次日观察条件。
    th = limit_threshold(code, name)
    prev2 = df.iloc[-3] if len(df) >= 3 else prev
    consecutive_before = prev.pct >= th and prev2.pct >= th
    if (
        last.pct >= th
        and not consecutive_before
        and close >= last.high * 0.995
        and 2 <= turnover <= 18
        and vol_ratio20 >= 1.2
        and amount >= 1.2e8
    ):
        base = 56
        if close > last.ma60 and last.ma20 > last.ma60 * 0.98:
            base += 4
        add_signal(
            "首板强势观察",
            base,
            [
                f"当日接近涨停/涨幅{last.pct:.1f}%",
                "非连续多板，避免过热接力",
                "仅用于次日竞价和承接观察",
            ],
            buy_low=close * 0.975,
            buy_high=close * 1.015,
            stop=max(last.low * 0.985, close - 1.5 * last.atr14),
            target=close + 2.0 * last.atr14,
            action="高风险：次日只看承接，不合格放弃",
        )

    # 5. 超跌企稳反弹：只在出现站回5日/放量时提示，分数通常低于趋势类。
    recent_dd = close / df["close"].tail(60).max() - 1
    if (
        recent_dd <= -0.18
        and prev.close < prev.ma5
        and close > last.ma5
        and last.low <= last.boll_lower * 1.02
        and vol_ratio20 >= 1.05
        and rsi <= 48
        and amount >= 6e7
        and pct_today > 0
    ):
        base = 52
        add_signal(
            "超跌企稳反弹",
            base,
            [
                f"60日回撤{recent_dd*100:.1f}%",
                "触及布林下轨后站回5日线",
                "有一定放量修复迹象",
            ],
            buy_low=max(last.ma5 * 0.99, close * 0.98),
            buy_high=close * 1.005,
            stop=min(last.low * 0.985, close - 1.2 * last.atr14),
            target=min(last.ma20, close + 1.6 * last.atr14),
            action="反弹型：快进快出，弱市慎用",
        )


    # 6. 欧奈尔/RPS风格强势新高：市面常称“强者恒强”“52周新高”，要求中期涨幅、均线结构与量能共振。
    if (
        not pd.isna(high120_prev)
        and close >= high120_prev * 0.985
        and close > last.ma20 > last.ma60 > last.ma120 * 0.96
        and (ret60 >= 0.12 or (not pd.isna(rps60) and rps60 >= 82))
        and (ret120 >= 0.18 or (not pd.isna(rps120) and rps120 >= 82))
        and (pd.isna(rps_combo) or rps_combo >= 78)
        and 50 <= rsi <= 76
        and pct_dist(close, last.ma20) <= 12
        and amount >= 1.0e8
        and pct_today <= min(8.0, limit_threshold(code, name) - 1.0)
    ):
        base = 68
        if not pd.isna(rps_combo):
            base += min(10, max(0, (rps_combo - 75) / 3))
        if vol_ratio20 >= 1.15:
            base += 3
        if close >= high250_proxy * 0.99:
            base += 3
        add_signal(
            "RPS强势新高",
            base,
            [
                f"接近/突破120日新高{high120_prev:.2f}",
                f"RPS综合{rps_combo:.0f}，60日/120日RPS {rps60:.0f}/{rps120:.0f}",
                f"60日涨幅{ret60*100:.1f}%，120日涨幅{ret120*100:.1f}%",
                "20/60/120日趋势结构向上",
            ],
            buy_low=max(last.ma5 * 0.985, close * 0.982),
            buy_high=close * 1.01,
            stop=max(last.ma20 * 0.965, close - 1.5 * last.atr14),
            target=close + 2.1 * last.atr14,
            action="强趋势票只做回踩/小幅突破确认，不追长阳",
            group="趋势动量类",
        )

    # 7. 海龟/唐奇安通道突破：更偏中短趋势跟踪，要求60日高点突破但ATR不过热。
    if (
        close > last.hh60_prev * 1.003
        and last.ma20 > last.ma60 * 1.005
        and 1.05 <= vol_ratio20 <= 2.8
        and 2.0 <= atr_pct <= 6.5
        and amount >= 8e7
        and pct_today <= min(8.5, limit_threshold(code, name) - 0.8)
    ):
        base = 67
        if ret20 > 0.08 and ret60 > 0.12:
            base += 4
        add_signal(
            "唐奇安60日突破",
            base,
            [
                f"收盘突破60日高点{last.hh60_prev:.2f}",
                f"ATR波动{atr_pct:.1f}%未极端",
                "适合用通道/均线做趋势跟踪",
            ],
            buy_low=max(last.hh60_prev * 0.992, close * 0.985),
            buy_high=close * 1.012,
            stop=max(last.ma20 * 0.97, close - 1.7 * last.atr14),
            target=close + 2.4 * last.atr14,
            action="突破后不跌回60日高点再考虑，跌回则放弃",
            group="突破类",
        )

    # 8. 涨停回踩低吸：A股短线常见“首板/涨停后缩量回踩”，不追板，等待回踩承接。
    recent = df.tail(7).copy()
    recent_limit = recent[recent["pct"] >= th]
    if not recent_limit.empty:
        lb = recent_limit.iloc[-1]
        days_after = int((last.date - lb.date).days) if hasattr(last.date, "date") else 0
        pullback_from_limit = close / lb.close - 1 if lb.close else math.nan
        if (
            last.date > lb.date
            and -0.085 <= pullback_from_limit <= -0.015
            and close >= max(last.ma10 * 0.985, lb.low * 0.98)
            and (pd.isna(vol_ratio20) or vol_ratio20 <= 1.25)
            and amount >= 7e7
            and pct_today > -4.5
        ):
            base = 58
            if last.close > last.open:
                base += 4
            if close > last.ma20:
                base += 3
            add_signal(
                "涨停后缩量回踩",
                base,
                [
                    f"近7日出现涨停，当前较涨停收盘回撤{pullback_from_limit*100:.1f}%",
                    "回踩未有效跌破10/20日均线或涨停K线低点",
                    "属于短线情绪低吸，必须看板块延续",
                ],
                buy_low=max(last.ma10 * 0.99, close * 0.985),
                buy_high=close * 1.006,
                stop=min(lb.low * 0.975, close - 1.25 * last.atr14),
                target=min(lb.close * 1.08, close + 2.0 * last.atr14),
                action="只在缩量企稳、板块仍强时低吸，放量破位放弃",
                group="回踩低吸类",
            )

    # 9. 缩量回踩前高：突破后第一次回踩平台上沿，市面常称“突破回踩确认”。
    prev_hh20 = df["high"].shift(2).rolling(20).max().iloc[-2] if len(df) >= 35 else math.nan
    had_breakout = prev.close > prev_hh20 * 1.002 if not pd.isna(prev_hh20) else False
    if (
        had_breakout
        and last.low <= prev_hh20 * 1.025
        and close >= prev_hh20 * 0.99
        and (pd.isna(vol_ratio20) or vol_ratio20 <= 1.15)
        and close > last.ma20
        and amount >= 6e7
        and -3.5 <= pct_today <= 3.5
    ):
        base = 63
        if last.close > last.open:
            base += 3
        add_signal(
            "突破回踩前高",
            base,
            [
                f"前一交易日突破平台{prev_hh20:.2f}",
                "今日回踩平台上沿附近未破",
                "回踩阶段量能未明显放大",
            ],
            buy_low=prev_hh20 * 0.995,
            buy_high=min(close * 1.008, prev_hh20 * 1.025),
            stop=min(prev_hh20 * 0.965, close - 1.25 * last.atr14),
            target=close + 1.9 * last.atr14,
            action="回踩确认类，只有不破前高/20日线才执行",
            group="回踩低吸类",
        )


    # 10. 波动收缩突破（VCP/箱体收敛）：先横盘缩量，再放量突破，过滤追涨式长阳。
    range20 = safe_float(last.get("range20_pct") if hasattr(last, "get") else getattr(last, "range20_pct", math.nan), math.nan)
    range60 = safe_float(last.get("range60_pct") if hasattr(last, "get") else getattr(last, "range60_pct", math.nan), math.nan)
    vol_dry = df["volume"].tail(5).mean() <= df["vma20"].tail(5).mean() * 0.92 if not pd.isna(last.vma20) else False
    if (
        not pd.isna(range20) and not pd.isna(range60)
        and range20 <= max(10.5, range60 * 0.72)
        and close > last.hh20_prev * 1.003
        and 1.15 <= vol_ratio20 <= 2.4
        and close > last.ma20 > last.ma60 * 0.985
        and amount >= 8e7
        and pct_today <= min(7.8, limit_threshold(code, name) - 0.8)
    ):
        base = 66
        if vol_dry:
            base += 4
        if not pd.isna(rps60) and rps60 >= 75:
            base += 4
        add_signal(
            "VCP波动收缩突破", base,
            [f"20日振幅{range20:.1f}%低于60日振幅{range60:.1f}%", "横盘收敛后突破20日箱体", f"量能为20日均量{vol_ratio20:.1f}倍"],
            buy_low=max(last.hh20_prev * 0.995, close * 0.985), buy_high=close * 1.01,
            stop=max(last.ma20 * 0.972, close - 1.45 * last.atr14), target=close + 2.2 * last.atr14,
            action="只做突破后不回落箱体的确认，次日放量滞涨放弃", group="突破类")

    # 11. MACD零轴二次金叉：趋势中回调后动能再启动，避免零轴下方弱反弹。
    macd_cross = prev.macd_diff <= prev.macd_dea and last.macd_diff > last.macd_dea
    if (
        macd_cross
        and last.macd_diff > 0 and last.macd_dea > -0.02 * close
        and close > last.ma20 > last.ma60 * 0.995
        and -2.0 <= pct_today <= 6.5
        and 45 <= rsi <= 70
        and amount >= 7e7
    ):
        base = 61
        if not pd.isna(rps20) and rps20 >= 70:
            base += 5
        if last.close > last.open:
            base += 3
        add_signal(
            "MACD零轴二次金叉", base,
            ["MACD在零轴附近/上方重新金叉", "价格仍在20/60日均线趋势结构内", f"RSI={rsi:.0f}未过热"],
            buy_low=max(last.ma20 * 0.992, close * 0.985), buy_high=close * 1.008,
            stop=max(last.ma60 * 0.982, close - 1.35 * last.atr14), target=close + 1.8 * last.atr14,
            action="趋势延续型，只在回踩不破20日线时执行", group="趋势动量类")

    # 12. 强势股首阴低吸：高RPS强趋势中首次阴线回踩，不做弱股抄底。
    prior_red_count = int((df.tail(6).iloc[:-1]["close"] < df.tail(6).iloc[:-1]["open"]).sum()) if len(df) >= 6 else 9
    first_red = last.close < last.open and prior_red_count <= 1
    if (
        first_red
        and close > last.ma10 * 0.985 and close > last.ma20 * 1.005
        and (not pd.isna(rps60) and rps60 >= 78)
        and 0.75 <= vol_ratio20 <= 1.35
        and -4.2 <= pct_today <= -0.2
        and amount >= 8e7
    ):
        base = 60
        if close >= high120_prev * 0.94:
            base += 4
        add_signal(
            "强势股首阴低吸", base,
            [f"60日RPS {rps60:.0f}，仍属强势阵营", "趋势股首次阴线回踩10/20日线附近", "量能未明显恐慌放大"],
            buy_low=max(last.ma10 * 0.99, close * 0.985), buy_high=close * 1.006,
            stop=max(last.ma20 * 0.965, close - 1.25 * last.atr14), target=close + 1.75 * last.atr14,
            action="低吸型，次日不能快速转强就放弃", group="回踩低吸类")

    # 13. 一剑封喉：放量实体阳线一举突破前高与短中期均线，属于强势突破的K线形态确认。
    body = abs(last.close - last.open)
    upper_shadow = last.high - max(last.close, last.open)
    body_pct = body / max(prev.close, 0.01) * 100
    if (
        last.close > last.open
        and body_pct >= 3.2
        and close > max(prev.high, last.ma5, last.ma10, last.ma20) * 1.005
        and near_high >= 0.70
        and 1.25 <= vol_ratio20 <= 3.2
        and last.ma20 >= last.ma60 * 0.98
        and amount >= 1.0e8
        and 3.0 <= pct_today <= min(8.8, limit_threshold(code, name) - 0.7)
    ):
        base = 67
        if not pd.isna(rps60) and rps60 >= 75:
            base += 4
        if close > last.hh60_prev:
            base += 3
        add_signal(
            "一剑封喉",
            base,
            [
                f"实体阳线{body_pct:.1f}%，收盘同时站上短中期均线",
                f"突破前一日高点{prev.high:.2f}并收于日内高位",
                f"量能为20日均量{vol_ratio20:.1f}倍",
            ],
            buy_low=max(prev.high * 0.995, close * 0.985),
            buy_high=close * 1.012,
            stop=max(last.ma20 * 0.97, last.low * 0.985),
            target=close + 2.2 * last.atr14,
            action="强阳突破只等次日不跌回实体中位/前高再执行",
            group="K线形态类",
        )

    # 14. 仙人指路：上升趋势中留下较长上影、收盘未走弱，次日需放量突破当日高点确认。
    upper_to_body = upper_shadow / max(body, close * 0.004)
    if (
        close > last.ma20 > last.ma60 * 0.985
        and upper_to_body >= 1.35
        and last.high >= last.hh20_prev * 0.985
        and close >= last.open * 0.988
        and close >= last.ma10 * 0.99
        and 0.75 <= vol_ratio20 <= 2.0
        and amount >= 7e7
        and -1.8 <= pct_today <= 5.8
    ):
        base = 59
        if not pd.isna(rps60) and rps60 >= 70:
            base += 4
        if last.ma20 > last.ma60:
            base += 3
        add_signal(
            "仙人指路",
            base,
            [
                f"上影线为实体{upper_to_body:.1f}倍，盘中测试前高压力",
                "收盘仍守住10/20日均线，趋势未破",
                "仅在次日放量越过当日高点时确认",
            ],
            buy_low=max(close * 0.99, last.ma10 * 0.995),
            buy_high=last.high * 1.008,
            stop=max(last.ma20 * 0.972, last.low * 0.982),
            target=close + 1.8 * last.atr14,
            action="观察型：次日放量突破今日高点才执行，跌破低点取消",
            group="K线形态类",
        )

    # 15. 老鸭头启动：均线在60日线上方经历短暂回压后，5/10日线重新金叉向上。
    recent_ma_cross_down = bool((df.tail(18)["ma5"] <= df.tail(18)["ma10"]).any()) if len(df) >= 18 else False
    ma_recross = prev.ma5 <= prev.ma10 and last.ma5 > last.ma10
    if (
        recent_ma_cross_down and ma_recross
        and last.ma10 >= last.ma60 * 1.005
        and close > last.ma20 * 0.995
        and last.ma20 >= df.iloc[-6].ma20 * 0.995
        and 0.85 <= vol_ratio20 <= 2.2
        and 42 <= rsi <= 68
        and amount >= 7e7
        and -1.0 <= pct_today <= 6.5
    ):
        base = 61
        if not pd.isna(rps60) and rps60 >= 68:
            base += 4
        add_signal(
            "老鸭头启动",
            base,
            [
                "5/10日线短暂回压后重新金叉",
                "10/20日均线仍处于60日线上方",
                "回调未破中期趋势，出现再启动条件",
            ],
            buy_low=max(last.ma10 * 0.99, close * 0.986),
            buy_high=close * 1.01,
            stop=max(last.ma60 * 0.978, close - 1.35 * last.atr14),
            target=close + 1.9 * last.atr14,
            action="等待5/10日线金叉后分时转强，不追高开",
            group="K线形态类",
        )

    # 16. 红三兵：三根连续上攻阳线，但限定涨幅、趋势和量能，防止把加速末端误判为买点。
    three = df.tail(3)
    if len(three) == 3:
        red_three = bool((three["close"] > three["open"]).all() and three["close"].is_monotonic_increasing)
        total_move = close / three.iloc[0].open - 1 if three.iloc[0].open else math.nan
        if (
            red_three
            and 0.04 <= total_move <= 0.16
            and close > last.ma20 >= last.ma60 * 0.98
            and last.volume >= last.vma20 * 1.05
            and pct_dist(close, last.ma20) <= 9.0
            and amount >= 8e7
            and rsi <= 73
        ):
            base = 60
            if not pd.isna(rps20) and rps20 >= 72:
                base += 4
            add_signal(
                "红三兵趋势续涨",
                base,
                [
                    f"连续三根阳线，三日累计上行{total_move*100:.1f}%",
                    "收盘保持在20日线之上，未明显乖离",
                    "最后一日量能较20日均量放大",
                ],
                buy_low=max(last.ma5 * 0.988, close * 0.986),
                buy_high=close * 1.008,
                stop=max(last.ma10 * 0.972, close - 1.3 * last.atr14),
                target=close + 1.7 * last.atr14,
                action="只做趋势中继，次日冲高回落跌破5日线则不参与",
                group="K线形态类",
            )

    # 17. 涨停回马枪（N字反包）：涨停后的缩量整理后，以阳线反包前日高点重新转强。
    recent12 = df.tail(12).copy()
    recent_limit12 = recent12[recent12["pct"] >= th]
    if not recent_limit12.empty:
        lb2 = recent_limit12.iloc[-1]
        lb_pos = df.index.get_loc(lb2.name)
        after_lb = df.iloc[lb_pos + 1:]
        if 2 <= len(after_lb) <= 8:
            retrace = after_lb["low"].min() / lb2.close - 1 if lb2.close else math.nan
            if (
                close > prev.high * 1.005
                and close > last.open
                and close >= lb2.close * 0.985
                and -0.11 <= retrace <= -0.018
                and vol_ratio20 >= 1.15
                and amount >= 1.0e8
                and 2.0 <= pct_today <= min(8.5, limit_threshold(code, name) - 0.7)
            ):
                base = 64
                if close > last.ma20:
                    base += 3
                add_signal(
                    "涨停回马枪",
                    base,
                    [
                        "近12日出现涨停，随后完成缩量整理",
                        f"整理低点较涨停收盘回撤{retrace*100:.1f}%后重新转强",
                        "当日阳线反包前一日高点，形成N字再启动",
                    ],
                    buy_low=max(prev.high * 0.995, close * 0.986),
                    buy_high=close * 1.01,
                    stop=max(last.ma20 * 0.97, after_lb["low"].min() * 0.98),
                    target=close + 2.0 * last.atr14,
                    action="短线情绪型：只做次日不跌回反包阳线中位的确认",
                    group="涨停情绪类",
                )

    # 18. 反包阳线：前日明显回落后，当日放量收复前日实体与高点，且必须有趋势/位置约束。
    if (
        prev.pct <= -2.8
        and last.pct >= 2.8
        and close > max(prev.high, prev.open) * 1.003
        and close >= last.ma20 * 0.985
        and vol_ratio20 >= 1.2
        and amount >= 8e7
        and rsi <= 72
    ):
        base = 58
        if last.ma20 > last.ma60 * 0.99:
            base += 4
        if not pd.isna(rps20) and rps20 >= 65:
            base += 3
        add_signal(
            "反包阳线修复",
            base,
            [
                f"前日下跌{prev.pct:.1f}%后，当日上涨{last.pct:.1f}%",
                "收盘收复前日实体并突破前日高点",
                "量能放大，属于修复确认而非单纯超跌",
            ],
            buy_low=max(prev.high * 0.995, close * 0.985),
            buy_high=close * 1.008,
            stop=max(last.ma20 * 0.968, last.low * 0.982),
            target=close + 1.7 * last.atr14,
            action="修复型信号，次日不能守住反包实体中位则放弃",
            group="K线形态类",
        )

    return out


def build_universe(spot: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    df = spot.copy()
    if df.empty:
        return df
    df = df[df["code"].str.match(r"^(00|30|60|68)\d{4}$", na=False)]
    df = df[~df["name"].map(is_bad_name)]
    df = df[df["price"].between(args.min_price, args.max_price, inclusive="both")]
    df = df[df["amount"] >= args.min_amount * 1e8]
    df = df[df["turnover"].between(args.min_turnover, args.max_turnover, inclusive="both")]
    df = df[df["float_mv"] >= args.min_float_mv * 1e8]
    # 兼顾流动性、强度和主力净额；不只按涨幅，避免全是高位票。
    df["universe_score"] = (
        df["amount"].rank(pct=True) * 45
        + df["turnover"].rank(pct=True) * 20
        + df["pct"].clip(-8, 8).rank(pct=True) * 20
        + df["main_net"].fillna(0).rank(pct=True) * 15
    )
    df = df.sort_values(["universe_score", "amount"], ascending=False)
    if not args.full:
        df = df.head(args.max_stocks)
    return df.reset_index(drop=True)


def scan(args: argparse.Namespace) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    started = now_cn()
    spot = fetch_spot_all()
    universe = build_universe(spot, args)
    print(f"[进度] 实时行情 {len(spot)} 只，基础过滤后 {len(universe)} 只进入K线深筛", flush=True)
    label, m_factor, m_notes = market_regime(use_cache=not args.no_cache)
    print(f"[进度] 市场环境：{label}，开始抓取K线/计算RPS", flush=True)
    meta = {
        "run_time_cn": started.strftime("%Y-%m-%d %H:%M:%S"),
        "spot_count": len(spot),
        "universe_count": len(universe),
        "scan_mode": "全市场基础过滤后扫描" if args.full else f"热度/流动性预筛前{args.max_stocks}只",
        "filters": {
            "market": "沪深A股主板/创业板/科创板，代码00/30/60/68；不含北交所、ETF、可转债",
            "exclude": "ST/*ST/退市/新股简称N或C、价格和流动性不足标的",
            "min_amount_yi": args.min_amount,
            "min_float_mv_yi": args.min_float_mv,
            "turnover_range_pct": [args.min_turnover, args.max_turnover],
        },
        "market_label": label,
        "market_factor": m_factor,
        "market_notes": m_notes,
        "execution_policy": "评分同时受大盘环境与战法类别调节；弱势市场仅作防守观察，情绪类信号默认需人工确认。",
        "errors": [],
    }
    try:
        idx_df = fetch_kline("上证指数", days=5, use_cache=not args.no_cache, is_index=True)
        if not idx_df.empty:
            meta["latest_trade_date"] = idx_df.iloc[-1].date.date().isoformat()
    except Exception:
        pass
    fetched: List[Dict[str, Any]] = []

    def fetch_worker(item: Tuple[int, pd.Series]) -> Dict[str, Any]:
        _, row = item
        try:
            hist = fetch_kline(str(row.code).zfill(6), days=260, use_cache=not args.no_cache)
            return {"row": row, "hist": hist, "metrics": compute_stock_metrics(hist)}
        except Exception as e:
            return {"_error": f"{row.get('code')} {row.get('name')}: {e}"}

    with futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(fetch_worker, item) for item in universe.iterrows()]
        done = 0
        for fut in futures.as_completed(futs):
            done += 1
            try:
                item = fut.result()
            except Exception as e:
                item = {"_error": f"future: {e}"}
            if "_error" in item:
                meta["errors"].append(item["_error"])
            elif item.get("hist") is not None and not item.get("hist").empty:
                fetched.append(item)
            if done % 100 == 0 or done == len(futs):
                print(f"[进度] K线深筛 {done}/{len(futs)}，有效 {len(fetched)}，错误 {len(meta['errors'])}", flush=True)

    meta["kline_scanned_count"] = len(fetched)
    meta["failed_count"] = len(meta["errors"])

    metric_rows = []
    for i, item in enumerate(fetched):
        m = dict(item.get("metrics") or {})
        m["idx"] = i
        metric_rows.append(m)
    ranks_by_idx: Dict[int, Dict[str, float]] = {}
    if metric_rows:
        mf = pd.DataFrame(metric_rows)
        for col, outcol in [("ret20", "rps20"), ("ret60", "rps60"), ("ret120", "rps120")]:
            if col in mf.columns:
                mf[outcol] = mf[col].rank(pct=True, na_option="bottom") * 100
        rps_cols = [c for c in ["rps20", "rps60", "rps120"] if c in mf.columns]
        if rps_cols:
            mf["rps_combo"] = mf[rps_cols].mean(axis=1)
        for _, r in mf.iterrows():
            ranks_by_idx[int(r["idx"])] = {k: safe_float(r.get(k), math.nan) for k in ["rps20", "rps60", "rps120", "rps_combo", "trend_quality", "near_52w", "atr_pct"]}
        meta["rps_reference_count"] = len(mf)

    signals: List[Dict[str, Any]] = []
    for i, item in enumerate(fetched):
        try:
            signals.extend(evaluate_stock(item["row"], item["hist"], m_factor, label, ranks_by_idx.get(i, {})))
        except Exception as e:
            row = item.get("row")
            meta["errors"].append(f"{getattr(row, 'code', '')} {getattr(row, 'name', '')}: evaluate {e}")

    print(f"[进度] RPS排名完成，开始战法匹配", flush=True)
    df = pd.DataFrame(signals)
    if not df.empty:
        # 同一只股票多个战法，保留最高分，同时把其它战法并入备注。
        df = df.sort_values("score", ascending=False)
        merged = []
        for code, g in df.groupby("code", sort=False):
            top = g.iloc[0].to_dict()
            if len(g) > 1:
                top["strategy"] = "+".join(g["strategy"].tolist()[:3])
                if "strategy_group" in g.columns:
                    top["strategy_group"] = "+".join(sorted(set(g["strategy_group"].dropna().astype(str).tolist())))
                top["reason"] += "；兼具信号: " + ", ".join(g["strategy"].tolist()[1:])
                top["score"] = min(99, round(float(top["score"]) + min(6, 2 * (len(g) - 1)), 1))
            merged.append(top)
        df = pd.DataFrame(merged).sort_values(["score", "amount"], ascending=False).head(args.top).reset_index(drop=True)
    return df, meta


def find_previous_signal_file(today_date: str) -> Optional[Path]:
    files = sorted(REPORT_DIR.glob("signals_*.csv"), reverse=True)
    for p in files:
        m = re.search(r"signals_(\d{8})\.csv", p.name)
        if not m:
            continue
        if m.group(1) < today_date:
            return p
    return None


def review_previous(today_yyyymmdd: str, use_cache: bool = True, topn: int = 12) -> Tuple[pd.DataFrame, Optional[Path]]:
    prev_file = find_previous_signal_file(today_yyyymmdd)
    if not prev_file:
        return pd.DataFrame(), None
    try:
        prev = pd.read_csv(prev_file, dtype={"code": str})
    except Exception:
        return pd.DataFrame(), prev_file
    rows = []
    for _, s in prev.head(topn).iterrows():
        code = str(s["code"]).zfill(6)
        try:
            hist = fetch_kline(code, days=60, use_cache=use_cache)
            if hist.empty:
                continue
            sig_date = pd.to_datetime(s["date"])
            after = hist[hist["date"] >= sig_date]
            if after.empty:
                continue
            # signal day close as reference; if there are later bars, evaluate through latest.
            sig_close = safe_float(s.get("close"))
            latest = after.iloc[-1]
            post = hist[hist["date"] > sig_date]
            max_high = post["high"].max() if not post.empty else math.nan
            min_low = post["low"].min() if not post.empty else math.nan
            ret_latest = (latest.close / sig_close - 1) * 100 if sig_close else math.nan
            ret_high = (max_high / sig_close - 1) * 100 if sig_close and not pd.isna(max_high) else math.nan
            dd_low = (min_low / sig_close - 1) * 100 if sig_close and not pd.isna(min_low) else math.nan
            stop = safe_float(s.get("stop_loss"))
            target = safe_float(s.get("target"))
            # 仅用日线高低价复盘，若同一根K线同时触及目标与止损，无法判断先后顺序，必须单独标注。
            target_hit = bool(not pd.isna(max_high) and target and max_high >= target)
            stop_hit = bool(not pd.isna(min_low) and stop and min_low <= stop)
            status = "跟踪中"
            if target_hit and stop_hit:
                status = "目标/止损均触及（日线先后未知）"
            elif target_hit:
                status = "曾达目标"
            elif stop_hit:
                status = "曾触止损"
            elif not pd.isna(ret_latest):
                if ret_latest >= 3:
                    status = "浮盈"
                elif ret_latest <= -3:
                    status = "浮亏"
            rows.append(
                {
                    "code": code,
                    "name": s.get("name", ""),
                    "strategy": s.get("strategy", ""),
                    "signal_date": pd.to_datetime(s["date"]).date().isoformat(),
                    "signal_close": round(sig_close, 3),
                    "latest_date": latest.date.date().isoformat(),
                    "latest_close": round(float(latest.close), 3),
                    "ret_latest_pct": round(ret_latest, 2) if not pd.isna(ret_latest) else "-",
                    "max_high_pct": round(ret_high, 2) if not pd.isna(ret_high) else "-",
                    "max_drawdown_pct": round(dd_low, 2) if not pd.isna(dd_low) else "-",
                    "target_hit": "是" if target_hit else "否",
                    "stop_hit": "是" if stop_hit else "否",
                    "status": status,
                }
            )
        except Exception:
            continue
    return pd.DataFrame(rows), prev_file


def review_summary(review: pd.DataFrame) -> Dict[str, Any]:
    """汇总“信号日收盘为基准”的观察反馈；不把它表述为真实成交收益。"""
    if review is None or review.empty:
        return {"sample_count": 0, "observed_count": 0, "positive_count": 0, "positive_rate_pct": None, "avg_observation_return_pct": None, "target_hit_count": 0, "stop_hit_count": 0}
    observed = review.copy()
    observed["_ret"] = pd.to_numeric(observed.get("ret_latest_pct"), errors="coerce")
    observed = observed.dropna(subset=["_ret"])
    target_hits = int((review.get("target_hit", pd.Series(dtype=str)) == "是").sum())
    stop_hits = int((review.get("stop_hit", pd.Series(dtype=str)) == "是").sum())
    positive = int((observed["_ret"] > 0).sum()) if not observed.empty else 0
    return {
        "sample_count": int(len(review)),
        "observed_count": int(len(observed)),
        "positive_count": positive,
        "positive_rate_pct": round(positive / len(observed) * 100, 1) if len(observed) else None,
        "avg_observation_return_pct": round(float(observed["_ret"].mean()), 2) if len(observed) else None,
        "target_hit_count": target_hits,
        "stop_hit_count": stop_hits,
    }


def df_to_markdown(df: pd.DataFrame, cols: List[str]) -> str:
    """Small dependency-free markdown table renderer; avoids pandas optional tabulate dependency."""
    if df is None or df.empty or not cols:
        return "（无）"
    show = df[cols].copy()
    headers = [str(c) for c in cols]

    def cell(v: Any) -> str:
        if pd.isna(v) if not isinstance(v, (list, dict, tuple)) else False:
            return "-"
        txt = str(v).replace("\n", " ").replace("|", "\\|")
        return txt

    rows = [[cell(v) for v in row] for row in show.itertuples(index=False, name=None)]
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for r in rows:
        lines.append("| " + " | ".join(r) + " |")
    return "\n".join(lines)


def write_outputs(signals: pd.DataFrame, meta: Dict[str, Any], review: pd.DataFrame, prev_file: Optional[Path]) -> Tuple[Path, Path]:
    run_dt = now_cn()
    # 如果行情最新交易日可从信号date取，否则用运行日。
    signal_date = signals["date"].iloc[0] if not signals.empty and "date" in signals else meta.get("latest_trade_date", run_dt.date().isoformat())
    ymd = signal_date.replace("-", "")
    signals_path = REPORT_DIR / f"signals_{ymd}.csv"
    report_path = REPORT_DIR / f"report_{ymd}.md"
    meta_path = REPORT_DIR / f"meta_{ymd}.json"
    feedback_path: Optional[Path] = None
    if review is not None and not review.empty and prev_file is not None:
        m = re.search(r"signals_(\d{8})\.csv", prev_file.name)
        source_ymd = m.group(1) if m else "unknown"
        feedback_path = REPORT_DIR / f"feedback_{source_ymd}_asof_{ymd}.csv"
        try:
            review.to_csv(feedback_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
            meta["feedback"] = {
                "source_signal_file": prev_file.name,
                "feedback_file": feedback_path.name,
                "as_of_date": signal_date,
                "definition": "以信号日收盘为基准的日线观察，不等同于真实成交收益",
                **review_summary(review),
            }
        except Exception as e:
            meta["feedback_error"] = str(e)

    if not signals.empty:
        signals.to_csv(signals_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    else:
        pd.DataFrame().to_csv(signals_path, index=False, encoding="utf-8-sig")

    lines = []
    lines.append(f"# A股每日技术战法筛选报告（{signal_date}）")
    lines.append("")
    lines.append(f"运行时间（北京时间）：{meta['run_time_cn']}")
    lines.append(f"全市场数量：{meta['spot_count']}；扫描股票数：{meta['universe_count']}；扫描模式：{meta.get('scan_mode','-')}")
    lines.append(f"市场环境：**{meta['market_label']}**（仓位/评分因子 {meta['market_factor']:.2f}）")
    lines.append("")
    lines.append("## 大盘状态")
    for n in meta.get("market_notes", []):
        lines.append(f"- {n}")
    lines.append("")
    if review is not None and not review.empty:
        lines.append("## 上次信号反馈")
        if prev_file:
            lines.append(f"来源：{prev_file.name}")
        lines.append("说明：以下为以**信号日收盘**为基准的日线观察，并非真实成交收益；目标价与止损价若同日均触及，日线无法判断先后。")
        lines.append(df_to_markdown(review, ["code", "name", "strategy", "signal_date", "latest_date", "ret_latest_pct", "max_high_pct", "max_drawdown_pct", "target_hit", "stop_hit", "status"]))
        sm = review_summary(review)
        lines.append(f"样本 {sm['sample_count']} 只；有可观察收盘数据 {sm['observed_count']} 只；正收益占比 {sm['positive_rate_pct'] if sm['positive_rate_pct'] is not None else '-'}%；平均观察收益 {sm['avg_observation_return_pct'] if sm['avg_observation_return_pct'] is not None else '-'}%；曾达目标 {sm['target_hit_count']} 只；曾触止损 {sm['stop_hit_count']} 只。")
        lines.append("")
    lines.append("## 今日候选（按综合评分排序）")
    if signals.empty:
        lines.append("今天没有通过过滤条件的强信号。弱市或无信号时，建议空仓/轻仓等待，而不是降低标准。")
    else:
        display = signals.copy()
        display["amount"] = display["amount"].map(fmt_money)
        cols = ["code", "name", "strategy_group", "strategy", "score", "rps_combo", "level", "action", "close", "pct_today", "turnover", "amount", "buy_zone", "stop_loss", "target", "risk_reward", "risk_tags"]
        cols = [c for c in cols if c in display.columns]
        lines.append(df_to_markdown(display, cols))
        lines.append("")
        lines.append("## 入选理由")
        for _, r in signals.iterrows():
            lines.append(f"- **{r['code']} {r['name']}**（{r['strategy']}，评分 {r['score']}）：{r['reason']}。")
    lines.append("")
    lines.append("## 执行纪律")
    lines.append("1. 脚本只做技术面候选筛选，不保证胜率，不构成投资建议。")
    lines.append("2. 单只标的计划亏损建议控制在账户权益的 0.5%~1.0%；触及止损先退出再复盘。")
    lines.append("3. 若大盘状态为“弱势/防守观察”，只看最高评分且减仓，宁可错过不要硬做。")
    lines.append("4. 次日若高开超过买入区间上沿或竞价/开盘放量下杀，直接放弃，不追价。")
    lines.append("5. 涨停/首板信号风险最高，只作为观察，必须结合集合竞价承接和板块强度人工确认。")
    lines.append("")
    if meta.get("errors"):
        lines.append("## 数据获取错误（已跳过）")
        for e in meta["errors"][:20]:
            lines.append(f"- {e}")
        if len(meta["errors"]) > 20:
            lines.append(f"- ... 另有 {len(meta['errors'])-20} 条")
    report_path.write_text("\n".join(lines), encoding="utf-8")
    try:
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return report_path, signals_path


def main() -> int:
    p = argparse.ArgumentParser(description="A股每日技术战法筛选与上次信号反馈")
    p.add_argument("--top", type=int, default=30, help="报告输出候选数量")
    p.add_argument("--max-stocks", type=int, default=1200, help="默认只扫描流动性较好的前N只，--full则忽略")
    p.add_argument("--full", action="store_true", help="扫描全部过滤后的股票，耗时更久")
    p.add_argument("--workers", type=int, default=12, help="并发抓取线程数，太高可能被限流")
    p.add_argument("--min-amount", type=float, default=0.6, help="最低成交额，单位：亿元")
    p.add_argument("--min-float-mv", type=float, default=25, help="最低流通市值，单位：亿元")
    p.add_argument("--min-turnover", type=float, default=0.6, help="最低换手率%")
    p.add_argument("--max-turnover", type=float, default=22, help="最高换手率%，过高视为博弈过热")
    p.add_argument("--min-price", type=float, default=3.0, help="最低股价")
    p.add_argument("--max-price", type=float, default=300.0, help="最高股价")
    p.add_argument("--no-cache", action="store_true", help="不使用当日缓存，强制重新抓取K线")
    p.add_argument("--review-only", action="store_true", help="只回顾上一次信号，不重新筛选")
    args = p.parse_args()

    today_ymd = now_cn().strftime("%Y%m%d")
    if args.review_only:
        review, prev_file = review_previous(today_ymd, use_cache=not args.no_cache, topn=args.top)
        print(df_to_markdown(review, review.columns.tolist() if not review.empty else []))
        return 0

    signals, meta = scan(args)
    current_signal_ymd = (str(signals["date"].iloc[0]).replace("-", "") if not signals.empty and "date" in signals else today_ymd)
    review, prev_file = review_previous(current_signal_ymd, use_cache=not args.no_cache, topn=min(args.top, 80))
    report_path, signals_path = write_outputs(signals, meta, review, prev_file)

    print(f"完成。市场环境：{meta['market_label']}；候选数：{len(signals)}")
    print(f"报告：{report_path}")
    print(f"信号CSV：{signals_path}")
    if not signals.empty:
        brief = signals.head(min(10, len(signals))).copy()
        brief["amount"] = brief["amount"].map(fmt_money)
        print("\n今日前10：")
        print(df_to_markdown(brief, ["code", "name", "strategy", "score", "action", "close", "pct_today", "buy_zone", "stop_loss", "target"]))
    else:
        print("今天没有强信号。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
