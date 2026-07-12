import unittest

import a_share_daily as q
import app


class QuantLogicTests(unittest.TestCase):
    def test_stock_code_cleaning(self):
        self.assertEqual(app.clean_stock_code("sz000938"), "000938")
        self.assertEqual(app.clean_stock_code("600519.SH"), "600519")
        with self.assertRaises(ValueError):
            app.clean_stock_code("430047")
        with self.assertRaises(ValueError):
            app.clean_stock_code("not-a-code")

    def test_strategy_group_mapping(self):
        self.assertEqual(q.strategy_group_name("一剑封喉"), "K线形态类")
        self.assertEqual(q.strategy_group_name("放量平台突破"), "突破类")
        self.assertEqual(q.strategy_group_name("超跌企稳反弹"), "超跌反转类")

    def test_consensus_bonus_counts_independent_groups_only(self):
        self.assertEqual(q.strategy_consensus_bonus(["突破类", "突破类"]), 0)
        self.assertEqual(q.strategy_consensus_bonus(["突破类", "趋势动量类"]), 2)
        self.assertEqual(q.strategy_consensus_bonus(["突破类", "趋势动量类", "回踩低吸类", "K线形态类", "综合类"]), 6)


if __name__ == "__main__":
    unittest.main()
