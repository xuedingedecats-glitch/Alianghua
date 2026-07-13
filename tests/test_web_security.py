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


    def test_homepage_includes_candidate_comparison_and_risk_budget_tools(self):
        page = app.page_html({"has_data": False, "watchlist": {}})
        for marker in (
            'id="compareModal"',
            'id="budgetModal"',
            'openCandidateCompare',
            'openRiskBudget',
            'renderRiskBudget',
            '对比已勾选',
            '风险预算',
        ):
            self.assertIn(marker, page)

    def test_opening_history_reads_valid_snapshot(self):
        with tempfile.TemporaryDirectory() as td:
            report_dir = Path(td)
            out = report_dir / "opening_checks"
            out.mkdir(parents=True)
            payload = {"checked_at": "2026-07-10 09:45:00", "source_date": "2026-07-09", "summary": {"total": 8, "eligible": 2, "wait": 3, "avoid": 2, "unready": 1}}
            (out / "opening_20260710_0945.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            with mock.patch.object(app, "REPORT_DIR", report_dir), mock.patch.object(app, "now_cn", return_value=dt.datetime(2026, 7, 10, 12, 0)):
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
            with mock.patch.object(app, "REPORT_DIR", report_dir), mock.patch.object(app, "now_cn", return_value=dt.datetime(2026, 7, 10, 12, 0)):
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
        for text in ("riskCapital", "riskPct", "maxPosPct", "portfolioPct", "portfolioRiskPct", "portfolioSummary", "buildPortfolioPlans", "exportOpeningCsv", "仓位预案", "最近三日自动核验轨迹", "查看详情", "/opening/detail?file=", "近3日"):
            self.assertIn(text, page)
        self.assertIn("sessionStorage.setItem('opening_risk_settings'", page)
        self.assertIn("function openOpeningKline", page)
        self.assertIn("onclick=\"openOpeningKline(&quot;000001&quot;)\"", page)
        self.assertIn("data-opening-code", page)
        self.assertIn("在新标签页打开K线图", page)
        self.assertIn("盘中即时建仓评估", page)
        self.assertIn("/api/entry-check?code=", page)
        self.assertNotIn("胜率优先", page)

    def test_intraday_entry_session_allows_afternoon_window(self):
        with mock.patch.object(app, "now_cn", return_value=dt.datetime(2026, 7, 13, 13, 30)):
            result = app.intraday_entry_session()
        self.assertTrue(result["active"])
        self.assertEqual(result["mode"], "afternoon")

    def test_intraday_entry_payload_uses_current_quote_and_intraday_support(self):
        current = dt.datetime(2026, 7, 13, 13, 30)
        app.INTRADAY_ENTRY_CACHE.clear()
        plan = {
            "code": "000001", "name": "测试股份", "score": 90, "is_custom": True,
            "strategy": "MA20/MA60趋势 + 盘中承接", "strategy_group": "自定义保守趋势确认",
            "buy_zone": "10.00~11.00", "stop_loss": 9.5, "setup_ok": True, "risk_tags": "自定义代码；需通过趋势门槛",
            "setup_reasons": ["✓ 收盘位于MA20上方", "✓ 中期均线多头"],
        }
        quote = {"ok": True, "stale": False, "code": "000001", "name": "测试股份", "price": 10.5, "pct": 1.2, "open": 10.3, "high": 10.6, "low": 10.2, "trade_date": "2026-07-13"}
        bars = [
            {"trade_date": "2026-07-13", "time": "13:25", "open": 10.3, "close": 10.42, "high": 10.5, "low": 10.3, "volume": 100, "amount": 104200, "average": 10.40},
            {"trade_date": "2026-07-13", "time": "13:30", "open": 10.42, "close": 10.5, "high": 10.6, "low": 10.4, "volume": 120, "amount": 126000, "average": 10.41},
        ]
        with mock.patch.object(app, "now_cn", return_value=current), mock.patch.object(app, "_recent_signal_plan", return_value=(None, "")), mock.patch.object(app, "custom_watch_plan", return_value=plan), mock.patch.object(app, "fetch_live_quote", return_value=quote), mock.patch.object(app, "_chart_intraday_bars", return_value=(bars, "测试股份", "测试源", 10.38)):
            result = app.intraday_entry_payload("000001")
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "可小仓试仓")
        self.assertEqual(result["intraday"]["latest_time"], "13:30")
        self.assertIn("分时均价承接", [x["name"] for x in result["checks"]])

    def test_intraday_entry_payload_does_not_reuse_nontrading_quote(self):
        app.INTRADAY_ENTRY_CACHE.clear()
        with mock.patch.object(app, "now_cn", return_value=dt.datetime(2026, 7, 13, 15, 10)), mock.patch.object(app, "fetch_live_quote") as fetch:
            result = app.intraday_entry_payload("000001")
        self.assertEqual(result["status"], "暂不评估")
        fetch.assert_not_called()

    def test_opening_session_only_allows_early_morning_window(self):
        cases = [
            (dt.datetime(2026, 7, 13, 9, 32), False, "auction"),
            (dt.datetime(2026, 7, 13, 10, 0), True, "morning"),
            (dt.datetime(2026, 7, 13, 14, 0), False, "afternoon"),
        ]
        for current, active, mode in cases:
            with self.subTest(current=current), mock.patch.object(app, "now_cn", return_value=current):
                result = app.opening_session()
                self.assertEqual(result["active"], active)
                self.assertEqual(result["mode"], mode)

    def test_opening_rejects_signal_older_than_fourteen_days(self):
        current = dt.datetime(2026, 7, 20, 10, 0)
        with tempfile.TemporaryDirectory() as td:
            report_dir = Path(td)
            signal = report_dir / "signals_20260701.csv"
            signal.write_text("code,date,score,buy_zone,stop_loss\n000001,2026-07-01,80,10~11,9.5\n", encoding="utf-8")
            session = {"active": True, "mode": "morning", "label": "早盘核验窗口", "time": "", "next_action": ""}
            empty_watch = {"codes": [], "count": 0, "max": 100, "items": [], "auto_sync": {}}
            with mock.patch.object(app, "REPORT_DIR", report_dir), mock.patch.object(app, "now_cn", return_value=current), mock.patch.object(app, "opening_session", return_value=session), mock.patch.object(app, "watchlist_payload", return_value=empty_watch), mock.patch.object(app, "fetch_live_quote") as fetch:
                result = app.opening_check_payload()
            self.assertFalse(result["ok"])
            self.assertIn("超过安全时效", result["message"])
            fetch.assert_not_called()

    def test_opening_refresh_is_single_flight(self):
        current = dt.datetime(2026, 7, 13, 10, 0)
        with tempfile.TemporaryDirectory() as td:
            report_dir = Path(td)
            signal = report_dir / "signals_20260710.csv"
            signal.write_text("code,date,score,buy_zone,stop_loss\n000001,2026-07-10,80,10~11,9.5\n", encoding="utf-8")
            session = {"active": True, "mode": "morning", "label": "早盘核验窗口", "time": "", "next_action": ""}
            empty_watch = {"codes": [], "count": 0, "max": 100, "items": [], "auto_sync": {}}
            app.OPENING_REFRESH_LOCK.acquire()
            try:
                with mock.patch.object(app, "REPORT_DIR", report_dir), mock.patch.object(app, "now_cn", return_value=current), mock.patch.object(app, "opening_session", return_value=session), mock.patch.object(app, "watchlist_payload", return_value=empty_watch), mock.patch.object(app, "fetch_live_quote") as fetch:
                    result = app.opening_check_payload()
            finally:
                app.OPENING_REFRESH_LOCK.release()
            self.assertTrue(result.get("refreshing"))
            fetch.assert_not_called()

    def test_opening_history_keeps_only_three_calendar_days(self):
        with tempfile.TemporaryDirectory() as td:
            report_dir = Path(td); out = report_dir / "opening_checks"; out.mkdir(parents=True)
            for day in ("20260713", "20260712", "20260711", "20260710"):
                (out / f"opening_{day}_0935.json").write_text(json.dumps({"checked_at": day, "summary": {}}), encoding="utf-8")
            with mock.patch.object(app, "REPORT_DIR", report_dir), mock.patch.object(app, "now_cn", return_value=dt.datetime(2026, 7, 13, 12, 0)):
                items = app.opening_history_payload(20)
            self.assertEqual([x["file"] for x in items], ["opening_20260713_0935.json", "opening_20260712_0935.json", "opening_20260711_0935.json"])
            self.assertFalse((out / "opening_20260710_0935.json").exists())

    def test_opening_detail_compares_snapshot_and_current_quote(self):
        with tempfile.TemporaryDirectory() as td:
            report_dir = Path(td); out = report_dir / "opening_checks"; out.mkdir(parents=True)
            name = "opening_20260713_0935.json"
            snapshot = {"checked_at": "2026-07-13 09:35:00", "source_date": "2026-07-10", "summary": {"total": 1}, "rows": [{
                "code": "000001", "name": "平安银行", "status": "可计划内执行", "score": 88, "strategy": "测试战法", "strategy_group": "趋势动量类", "reason": "条件通过", "action": "小仓分批", "buy_zone": "10~11", "stop_loss": 9.5,
                "checks": [{"name": "计划价格", "pass": True, "text": "通过"}],
                "monitor_baseline": {"price": 10.0, "added_at": "2026-07-10 15:35:00", "source": "手动添加监控", "price_source": "公开行情快照"},
                "quote": {"price": 10.5, "trade_time": "2026-07-13 09:35:00", "source": "历史快照"},
            }]}
            (out / name).write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
            quote = {"ok": True, "price": 11.0, "trade_time": "2026-07-13 10:00:00", "source": "当前公开行情"}
            with mock.patch.object(app, "REPORT_DIR", report_dir), mock.patch.object(app, "fetch_live_quote", return_value=quote):
                result = app.opening_detail_payload(name)
            self.assertTrue(result["ok"])
            row = result["rows"][0]
            self.assertEqual(row["snapshot_change_pct"], 5.0)
            self.assertEqual(row["current_change_pct"], 10.0)
            self.assertEqual(row["current_quote"]["source"], "当前公开行情")
            page = app.opening_detail_page_html(result)
            for label in ("加入监控基准价", "本次核验价（历史快照）", "当前行情（打开详情时获取）", "openDetailKline"):
                self.assertIn(label, page)

    def test_opening_detail_rejects_invalid_file_name_without_quote_request(self):
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.object(app, "REPORT_DIR", Path(td)), mock.patch.object(app, "fetch_live_quote") as fetch:
                result = app.opening_detail_payload("../../opening_20260713_0935.json")
            self.assertFalse(result["ok"])
            fetch.assert_not_called()

    def test_manual_watch_add_captures_monitor_baseline(self):
        with tempfile.TemporaryDirectory() as td:
            watch_file = Path(td) / "opening_watchlist.json"
            quote = {"ok": True, "price": 10.25, "name": "测试股份", "source": "测试行情"}
            with mock.patch.object(app, "WATCHLIST_FILE", watch_file), mock.patch.object(app, "fetch_live_quote", return_value=quote), mock.patch.object(app, "latest_signal_file", return_value=None):
                result = app.update_watch_codes("add", ["000001"])
                state = app.load_watch_state()
            self.assertIn("000001", result["codes"])
            self.assertEqual(state["monitor_baselines"]["000001"]["price"], 10.25)
            self.assertEqual(state["monitor_baselines"]["000001"]["source"], "手动添加监控")

    def test_opening_snapshot_is_written_atomically(self):
        with tempfile.TemporaryDirectory() as td:
            report_dir = Path(td)
            with mock.patch.object(app, "REPORT_DIR", report_dir):
                saved = app.save_opening_check({"rows": [{"code": "000001"}], "summary": {"total": 1}})
            self.assertIsNotNone(saved)
            self.assertTrue(saved.exists())
            self.assertFalse(saved.with_suffix(".tmp").exists())

    def test_public_bind_requires_management_token(self):
        with mock.patch.object(app, "WEB_TOKEN", ""), mock.patch("sys.argv", ["app.py", "--host", "0.0.0.0", "--no-scheduler"]):
            self.assertEqual(app.main(), 3)


if __name__ == "__main__":
    unittest.main()
