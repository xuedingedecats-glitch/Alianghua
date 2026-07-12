import csv
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import app


class WebSecurityTests(unittest.TestCase):
    def _write_signal(self, root: Path, ymd: str, meta: dict, row_date: str | None = None) -> Path:
        reports = root / "a_share_daily_reports"
        reports.mkdir(parents=True, exist_ok=True)
        signal = reports / f"signals_{ymd}.csv"
        with signal.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["code", "date", "score", "buy_zone", "stop_loss"])
            writer.writeheader()
            writer.writerow({"code": "000001", "date": row_date or f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}", "score": "80", "buy_zone": "10~11", "stop_loss": "9.5"})
        (reports / f"meta_{ymd}.json").write_text(json.dumps(meta), encoding="utf-8")
        return signal

    def test_quality_rejects_stale_trade_date(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            signal = self._write_signal(root, "20260710", {"latest_trade_date": "2026-07-10", "universe_count": 100, "kline_scanned_count": 98, "failed_count": 2})
            with mock.patch.object(app, "REPORT_DIR", root / "a_share_daily_reports"):
                result = app.signal_data_quality(signal, require_today=True)
            self.assertFalse(result["ok"])
            self.assertIn("不是今天", result["reason"])

    def test_quality_rejects_high_failure_rate(self):
        today = app.now_cn().strftime("%Y%m%d")
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            signal = self._write_signal(root, today, {"latest_trade_date": app.now_cn().date().isoformat(), "universe_count": 100, "kline_scanned_count": 70, "failed_count": 30})
            with mock.patch.object(app, "REPORT_DIR", root / "a_share_daily_reports"):
                result = app.signal_data_quality(signal, require_today=True)
            self.assertFalse(result["ok"])
            self.assertIn("失败率", result["reason"])

    def test_quality_accepts_complete_same_day_data(self):
        current = dt.datetime(2026, 7, 10, 16, 0, 0)
        today = current.strftime("%Y%m%d")
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            signal = self._write_signal(root, today, {"latest_trade_date": current.date().isoformat(), "universe_count": 100, "kline_scanned_count": 98, "failed_count": 2})
            with mock.patch.object(app, "REPORT_DIR", root / "a_share_daily_reports"), mock.patch.object(app, "now_cn", return_value=current):
                result = app.signal_data_quality(signal, require_today=True)
            self.assertTrue(result["ok"], result)

    def test_page_uses_session_scoped_token(self):
        html = app.page_html({"has_data": False, "watchlist": {}})
        self.assertIn("sessionStorage.getItem('quant_token')", html)
        self.assertNotIn("localStorage.setItem('quant_token'", html)


    def test_opening_history_reads_valid_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            report_dir = Path(td)
            out = report_dir / "opening_checks"
            out.mkdir(parents=True)
            payload = {"checked_at": "2026-07-10 09:45:00", "source_date": "2026-07-09", "summary": {"total": 8, "eligible": 2, "wait": 3, "avoid": 2, "unready": 1}}
            (out / "opening_20260710_0945.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            with mock.patch.object(app, "REPORT_DIR", report_dir):
                items = app.opening_history_payload()
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["eligible"], 2)
            self.assertEqual(items[0]["checked_at"], "2026-07-10 09:45:00")

    def test_opening_history_skips_broken_and_oversized_files(self):
        with tempfile.TemporaryDirectory() as td:
            report_dir = Path(td)
            out = report_dir / "opening_checks"
            out.mkdir(parents=True)
            (out / "opening_20260710_0950.json").write_text("{broken", encoding="utf-8")
            with (out / "opening_20260710_0955.json").open("wb") as fh:
                fh.truncate(2 * 1024 * 1024 + 1)
            with mock.patch.object(app, "REPORT_DIR", report_dir):
                self.assertEqual(app.opening_history_payload(), [])

    def test_opening_page_contains_risk_export_and_history_tools(self):
        payload = {
            "session": {"active": True, "label": "盘中核验", "time": "09:45", "next_action": "继续观察"},
            "summary": {"total": 1, "eligible": 1, "wait": 0, "avoid": 0},
            "watchlist": {"count": 0, "max": 100, "items": [], "auto_sync": {"enabled": False, "top_n": 12}},
            "history": [{"checked_at": "2026-07-10 09:45:00", "source_date": "2026-07-09", "total": 1, "eligible": 1, "wait": 0, "avoid": 0, "unready": 0}],
            "rows": [{
                "code": "000001", "name": "测试股份", "status": "可计划内执行", "score": 80,
                "level": "优先", "strategy_group": "趋势", "strategy": "测试战法", "buy_zone": "10.00~10.50",
                "zone_low": 10.0, "stop_loss": 9.5, "source": "昨日推荐", "reason": "条件通过", "action": "小仓分批",
                "checks": [], "quote": {"price": 10.2, "open": 10.1, "pct": 1.0, "gap_pct": 0.5, "source": "测试源", "trade_time": "09:45"},
            }],
            "monitor_mode": "默认高分候选", "limit": 12, "schedule": "09:35 / 09:45", "source_date": "2026-07-09",
            "checked_at": "2026-07-10 09:45:00", "message": "测试",
        }
        page = app.opening_page_html(payload)
        for text in ("riskCapital", "riskPct", "maxPosPct", "exportOpeningCsv", "仓位预案", "最近自动核验轨迹"):
            self.assertIn(text, page)
        self.assertIn("sessionStorage.setItem('opening_risk_settings'", page)
        self.assertNotIn("胜率优先", page)


if __name__ == "__main__":
    unittest.main()
