import unittest

from demeter_fetch.processor_uniswap.uniswap_utils import x96_sqrt_to_decimal


# x96_sqrt_to_decimal


class UniUtilTest(unittest.TestCase):
    def test_x96_sqrt_to_decimal(self):
        # token0->usdc, token1->weth
        val = x96_sqrt_to_decimal(1438663542842353560857615249833810, 6, 18, False)
        self.assertEqual(val // 1, 3032.0)
