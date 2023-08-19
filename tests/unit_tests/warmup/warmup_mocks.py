from nautilus_trader.indicators.base.indicator import Indicator

from nautilus_trader.warmup.tree import WarmupConfig


class MockWarmupIndicator(Indicator):
    """
    A test indicator that completes warmup after a specified amount of bars
    """

    def __init__(self, warmup_config: WarmupConfig):
        super().__init__([], warmup_config=warmup_config)
        self.i = 0

    def handle_bar(self, bar):
        self.i += 1
        if self.warmup_config.count == self.i:
            self._set_initialized(True)
            assert self.initialized

    def __repr__(self):
        return f"{self.__class__.__name__}({self.warmup_config})"


