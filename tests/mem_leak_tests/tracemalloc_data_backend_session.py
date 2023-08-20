#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2023 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

# from nautilus_trader.persistence.wranglers import TradeTickDataWrangler
from nautilus_trader.test_kit.fixtures.memory import snapshot_memory

# from nautilus_trader.test_kit.providers import TestDataProvider
from nautilus_trader.test_kit.providers import TestInstrumentProvider


ETHUSDT_BINANCE = TestInstrumentProvider.ethusdt_binance()
from nautilus_trader.core.nautilus_pyo3.persistence import NautilusDataType
from nautilus_trader import TEST_DATA_DIR
from nautilus_trader.core.nautilus_pyo3.persistence import DataBackendSession
from nautilus_trader.test_kit.performance import PerformanceBench
from nautilus_trader.persistence.wranglers import list_from_capsule

@snapshot_memory(1)
def run(*args, **kwargs):
    session = DataBackendSession()
    
    files = [
        TEST_DATA_DIR / "quote_tick_eurusd_2019_sim_rust.parquet",
        TEST_DATA_DIR / "quote_tick_usdjpy_2019_sim_rust.parquet",
    ] * 10

    for i, file in enumerate(files):
        session.add_file(str(i), file, NautilusDataType.QuoteTick)

    result = session.to_query_result()
    
    def run_streaming():
        processed = 0
        for batch in result:
            batch = list_from_capsule(batch)
            processed += len(batch)
            # print(len(data))
        
    PerformanceBench.profile_function(
                        target=run_streaming,
                        runs=1,
                        iterations=1,
                    )

if __name__ == "__main__":
    run()
