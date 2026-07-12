import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import app


class TradeJournalTests(unittest.TestCase):
    def _add(self):
        return app.trade_journal_action({
            "mode": "add", "code": "000001", "name": "平安银行",
            "signal_date": "2026-07-10", "entry_date": "2026-07-11",
            "entry_price": 10.0, "shares": 1000, "initial_stop_loss": 9.5,
            "note": "规则通过后分批建仓"
        })

    def test_add_update_close_and_delete_trade(self):
        with tempfile.TemporaryDirectory() as td, mock.patch.object(app, "DATA_DIR", Path(td)), mock.patch.object(app, "TRADE_JOURNAL_FILE", Path(td) / "trade_journal.json"):
            result = self._add()
            self.assertEqual(result["summary"]["open_count"], 1)
            row = result["open"][0]
            self.assertEqual(row["initial_risk"], 500.0)
            self.assertEqual(row["cost"], 10000.0)
            result = app.trade_journal_action({"mode": "update", "id": row["id"], "current_stop_loss": 10.2})
            self.assertEqual(result["open"][0]["current_risk"], 0.0)
            self.assertEqual(result["open"][0]["protected_profit"], 200.0)
            result = app.trade_journal_action({"mode": "close", "id": row["id"], "exit_date": "2026-07-12", "exit_price": 10.8})
            closed = result["closed"][0]
            self.assertEqual(closed["pnl"], 800.0)
            self.assertEqual(closed["return_pct"], 8.0)
            self.assertEqual(closed["r_multiple"], 1.6)
            result = app.trade_journal_action({"mode": "delete", "id": row["id"]})
            self.assertEqual(result["summary"]["closed_count"], 0)

    def test_invalid_fields_are_rejected(self):
        cases = [
            ({"code": "123456"}, "沪深A股"),
            ({"entry_date": "2026/07/11"}, "YYYY-MM-DD"),
            ({"entry_price": 0}, "建仓价"),
            ({"shares": 1.5}, "整数"),
            ({"initial_stop_loss": 10.5}, "低于建仓价"),
        ]
        base = {"mode":"add","code":"000001","entry_date":"2026-07-11","entry_price":10,"shares":100,"initial_stop_loss":9}
        with tempfile.TemporaryDirectory() as td, mock.patch.object(app, "DATA_DIR", Path(td)), mock.patch.object(app, "TRADE_JOURNAL_FILE", Path(td) / "trade_journal.json"):
            for change, message in cases:
                body = dict(base); body.update(change)
                with self.assertRaisesRegex(ValueError, message): app.trade_journal_action(body)

    def test_missing_id_and_bad_dates_are_rejected(self):
        with tempfile.TemporaryDirectory() as td, mock.patch.object(app, "DATA_DIR", Path(td)), mock.patch.object(app, "TRADE_JOURNAL_FILE", Path(td) / "trade_journal.json"):
            with self.assertRaisesRegex(ValueError, "不存在"): app.trade_journal_action({"mode":"update","id":"missing","current_stop_loss":9})
            row = self._add()["open"][0]
            with self.assertRaisesRegex(ValueError, "不能早于"): app.trade_journal_action({"mode":"close","id":row["id"],"exit_date":"2026-07-10","exit_price":10})

    def test_broken_and_oversized_file_degrade_safely(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "trade_journal.json"
            with mock.patch.object(app, "TRADE_JOURNAL_FILE", path):
                path.write_text("{broken", encoding="utf-8")
                payload = app.trade_journal_payload()
                self.assertFalse(payload["ok"]); self.assertEqual(payload["open"], [])
                with self.assertRaisesRegex(ValueError, "暂不可写"): self._add()
                with path.open("wb") as fh: fh.truncate(app.MAX_TRADE_FILE_BYTES + 1)
                self.assertFalse(app.trade_journal_payload()["ok"])

    def test_page_contains_required_sections_and_session_token(self):
        page = app.trade_journal_page_html()
        for text in ("交易复盘台", "新增执行记录", "当前持仓", "已结束交易", "sessionStorage", "不会自动下单"):
            self.assertIn(text, page)
        self.assertNotIn("localStorage.setItem('quant_token'", page)


if __name__ == "__main__":
    unittest.main()
