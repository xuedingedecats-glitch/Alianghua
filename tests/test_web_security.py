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


if __name__ == "__main__":
    unittest.main()
