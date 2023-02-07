from nautilus_trader.indicators.base.indicator import Indicator

from nautilus_trader.indicators.base.indicator import IndicatorConfig


class MockWarmupIndicator(Indicator):
    """
    A test indicator that completes warmup after a specified amount of bars
    """

    def __init__(self, config: IndicatorConfig):
        super().__init__([], config=config)
        self.i = 0
        self.config = config
    def handle_bar(self, bar):
        self.i += 1
        if self.config.count == self.i:
            self._set_initialized(True)
            assert self.initialized

    def __repr__(self):
        return f"{self.__class__.__name__}({self.config})"


