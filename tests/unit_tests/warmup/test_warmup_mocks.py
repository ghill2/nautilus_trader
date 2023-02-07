from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from tests.unit_tests.warmup.warmup_mocks import MockWarmupIndicator


class TestMockWarmupIndicator:
    def test_indicator_completes_warmup_on_expected_bar(self):
        n = 10
        bars = [
                   Bar(
                       BarType.from_str("EUR/USD.SIM-1-MINUTE-BID-EXTERNAL"),
                       Price(1.234, 4),
                       Price(1.234, 4),
                       Price(1.234, 4),
                       Price(1.234, 4),
                       Quantity(5, 0),
                       0,
                       0,
                   ),
               ] * n
        indicator = MockWarmupIndicator(n)
        for i, bar in enumerate(bars):
            indicator.handle_bar(bar)
            if i == n - 2:
                assert not indicator.initialized
            if i == n - 1:
                assert indicator.initialized
