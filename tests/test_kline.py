import unittest
from unittest import mock

import app


class KlineTests(unittest.TestCase):
    def setUp(self):
        with app.KLINE_CACHE_LOCK:
            app.KLINE_CACHE.clear()

    @staticmethod
    def bars(count=140):
        rows = []
        for i in range(count):
            day = app.dt.date(2025, 1, 1) + app.dt.timedelta(days=i)
            close = float(i + 1)
            rows.append({
                "date": day.isoformat(),
                "open": close - 0.5,
                "close": close,
                "high": close + 1,
                "low": close - 1,
                "volume": float(100 + i),
            })
        return rows

    def test_day_period_keeps_daily_rows(self):
        rows = self.bars(3)
        self.assertEqual(app._aggregate_kline(rows, "day"), rows)
        self.assertIsNot(app._aggregate_kline(rows, "day")[0], rows[0])

    def test_week_aggregation_uses_first_open_last_close_and_extremes(self):
        rows = [
            {"date": "2026-07-06", "open": 10.0, "close": 10.5, "high": 11.0, "low": 9.8, "volume": 100.0},
            {"date": "2026-07-07", "open": 10.6, "close": 10.2, "high": 11.3, "low": 10.0, "volume": 120.0},
            {"date": "2026-07-10", "open": 10.3, "close": 11.0, "high": 11.2, "low": 10.1, "volume": 130.0},
        ]
        result = app._aggregate_kline(rows, "week")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {"date": "2026-07-10", "open": 10.0, "close": 11.0, "high": 11.3, "low": 9.8, "volume": 350.0})

    def test_month_aggregation_splits_months(self):
        rows = [
            {"date": "2026-06-30", "open": 9.0, "close": 9.5, "high": 10.0, "low": 8.8, "volume": 80.0},
            {"date": "2026-07-01", "open": 10.0, "close": 10.4, "high": 10.8, "low": 9.9, "volume": 100.0},
            {"date": "2026-07-31", "open": 10.5, "close": 11.0, "high": 11.5, "low": 10.2, "volume": 120.0},
        ]
        result = app._aggregate_kline(rows, "month")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1]["open"], 10.0)
        self.assertEqual(result[1]["close"], 11.0)
        self.assertEqual(result[1]["volume"], 220.0)

    def test_moving_averages_begin_at_expected_row(self):
        result = app._with_moving_averages(self.bars(60))
        self.assertIsNone(result[3]["ma5"])
        self.assertEqual(result[4]["ma5"], 3.0)
        self.assertEqual(result[9]["ma10"], 5.5)
        self.assertEqual(result[19]["ma20"], 10.5)
        self.assertEqual(result[59]["ma60"], 30.5)

    def test_kline_payload_validates_code_period_and_limit(self):
        with self.assertRaises(ValueError):
            app.kline_payload("123456", "day", 120)
        with self.assertRaises(ValueError):
            app.kline_payload("000001", "year", 120)
        with self.assertRaises(ValueError):
            app.kline_payload("000001", "day", "bad")

    def test_kline_payload_clamps_limit_and_uses_period_warmup(self):
        source = self.bars(400)
        with mock.patch.object(app, "_chart_daily_bars", return_value=(source, "平安银行", "测试源")) as fetch:
            low = app.kline_payload("000001", "day", 1)
        self.assertEqual(len(low["rows"]), 40)
        fetch.assert_called_once_with("000001", 100)

    def test_kline_payload_allows_extended_history_up_to_640(self):
        source = self.bars(700)
        with mock.patch.object(app, "_chart_daily_bars", return_value=(source, "平安银行", "测试源")) as fetch:
            result = app.kline_payload("000001", "day", 999)
        self.assertEqual(len(result["rows"]), 640)
        fetch.assert_called_once_with("000001", 700)

    def test_month_payload_requests_enough_daily_history_for_ma60(self):
        source = self.bars(400)
        with mock.patch.object(app, "_chart_daily_bars", return_value=(source, "平安银行", "测试源")) as fetch:
            app.kline_payload("000001", "month", 120)
        fetch.assert_called_once_with("000001", 4320)

    def test_payload_cache_avoids_duplicate_source_request(self):
        source = self.bars(140)
        with mock.patch.object(app, "_chart_daily_bars", return_value=(source, "平安银行", "测试源")) as fetch:
            first = app.kline_payload("000001", "day", 120)
            second = app.kline_payload("000001", "day", 120)
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(first, second)

    def test_page_contains_periods_mas_and_kline_click(self):
        data = {"has_data": True, "rows": [{"code": "000001", "name": "平安银行"}], "watchlist": {}, "group_order": ["全部"], "strategy_book": []}
        html = app.page_html(data)
        for text in ('id="klineModal"', "日线", "周线", "月线", "全屏", "120根", "240根", "480根", "＋ 放大", "－ 缩小", "较早", "较新", "function openKline", "[5,10,20,60]", "MA${n}"):
            self.assertIn(text, html)
        self.assertIn("onclick=\"openKline('${esc(r.code)}')\"", app.SCRIPT)
        self.assertNotIn("openKline('${esc(r.code)}','${esc(r.name)}')", app.SCRIPT)

    def test_kline_modal_stays_above_sticky_navigation(self):
        self.assertIn("z-index:1000", app.STYLE)
        self.assertIn("body.kline-open{overflow:hidden}", app.STYLE)
        self.assertIn("function toggleKlineFullscreen", app.SCRIPT)

    def test_kline_supports_zoom_pan_and_dynamic_history_limit(self):
        self.assertIn("function setKlineLimit(limit)", app.SCRIPT)
        self.assertIn("function zoomKline(factor)", app.SCRIPT)
        self.assertIn("function panKline(direction)", app.SCRIPT)
        self.assertIn("limit=${klineState.limit}", app.SCRIPT)
        self.assertIn("canvas.onwheel", app.SCRIPT)

    def test_crosshair_repaints_base_before_drawing(self):
        self.assertIn("geom=paintKlineBase(canvas,c,rows,w,h);document.getElementById('klineInfo')", app.SCRIPT)
        self.assertIn("drawKlineCross(c,w,geom,idx,rows)", app.SCRIPT)


if __name__ == "__main__":
    unittest.main()
