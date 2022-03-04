# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2022 Nautech Systems Pty Ltd. All rights reserved.
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

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import orjson

from nautilus_trader.adapters.binance.core.constants import BINANCE_VENUE
from nautilus_trader.adapters.binance.core.enums import BinanceAccountType
from nautilus_trader.adapters.binance.core.functions import format_symbol
from nautilus_trader.adapters.binance.core.functions import parse_symbol
from nautilus_trader.adapters.binance.core.rules import VALID_ORDER_TYPES_FUTURES
from nautilus_trader.adapters.binance.core.rules import VALID_ORDER_TYPES_SPOT
from nautilus_trader.adapters.binance.core.rules import VALID_TIF
from nautilus_trader.adapters.binance.http.api.account import BinanceAccountHttpAPI
from nautilus_trader.adapters.binance.http.api.market import BinanceMarketHttpAPI
from nautilus_trader.adapters.binance.http.api.user import BinanceUserDataHttpAPI
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient
from nautilus_trader.adapters.binance.http.error import BinanceError
from nautilus_trader.adapters.binance.parsing.common import binance_order_type_futures
from nautilus_trader.adapters.binance.parsing.common import binance_order_type_spot
from nautilus_trader.adapters.binance.parsing.common import parse_order_type
from nautilus_trader.adapters.binance.parsing.http import parse_account_balances_futures_http
from nautilus_trader.adapters.binance.parsing.http import parse_account_balances_spot_http
from nautilus_trader.adapters.binance.parsing.http import parse_account_margins_http
from nautilus_trader.adapters.binance.parsing.websocket import parse_account_balances_futures_ws
from nautilus_trader.adapters.binance.parsing.websocket import parse_account_balances_spot_ws
from nautilus_trader.adapters.binance.providers import BinanceInstrumentProvider
from nautilus_trader.adapters.binance.websocket.client import BinanceWebSocketClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.clock import LiveClock
from nautilus_trader.common.logging import LogColor
from nautilus_trader.common.logging import Logger
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.execution.reports import TradeReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.c_enums.order_type import OrderTypeParser
from nautilus_trader.model.c_enums.trailing_offset_type import TrailingOffsetTypeParser
from nautilus_trader.model.c_enums.trigger_type import TriggerTypeParser
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OMSType
from nautilus_trader.model.enums import OrderSideParser
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForceParser
from nautilus_trader.model.enums import TrailingOffsetType
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders.base import Order
from nautilus_trader.model.orders.limit import LimitOrder
from nautilus_trader.model.orders.market import MarketOrder
from nautilus_trader.model.orders.stop_limit import StopLimitOrder
from nautilus_trader.model.orders.stop_market import StopMarketOrder
from nautilus_trader.model.orders.trailing_stop_market import TrailingStopMarketOrder
from nautilus_trader.msgbus.bus import MessageBus


class BinanceExecutionClient(LiveExecutionClient):
    """
    Provides an execution client for the `Binance` exchange.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : BinanceHttpClient
        The binance HTTP client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    logger : Logger
        The logger for the client.
    instrument_provider : BinanceInstrumentProvider
        The instrument provider.
    account_type : BinanceAccountType
        The account type for the client.
    base_url_ws : str, optional
        The base URL for the WebSocket client.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: BinanceHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        logger: Logger,
        instrument_provider: BinanceInstrumentProvider,
        account_type: BinanceAccountType = BinanceAccountType.SPOT,
        base_url_ws: Optional[str] = None,
    ):
        super().__init__(
            loop=loop,
            client_id=ClientId(BINANCE_VENUE.value),
            venue=BINANCE_VENUE,
            oms_type=OMSType.NETTING,
            instrument_provider=instrument_provider,
            account_type=AccountType.CASH,
            base_currency=None,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
        )

        self._client = client
        self._set_account_id(AccountId(BINANCE_VENUE.value, "master"))

        self._binance_account_type = account_type
        self._log.info(f"Account type: {self._binance_account_type.value}.", LogColor.BLUE)

        # HTTP API
        self._http_account = BinanceAccountHttpAPI(client=self._client, account_type=account_type)
        self._http_market = BinanceMarketHttpAPI(client=self._client, account_type=account_type)
        self._http_user = BinanceUserDataHttpAPI(client=self._client, account_type=account_type)

        # Listen keys
        self._ping_listen_keys_interval: int = 60 * 5  # Once every 5 mins (hardcode)
        self._ping_listen_keys_task: Optional[asyncio.Task] = None
        self._listen_key: Optional[str] = None

        # WebSocket API
        self._ws = BinanceWebSocketClient(
            loop=loop,
            clock=clock,
            logger=logger,
            handler=self._handle_user_ws_message,
            base_url=base_url_ws,
        )

        # Hot caches
        self._instrument_ids: Dict[str, InstrumentId] = {}

        self._log.info(f"Base URL HTTP {self._client._base_url}.", LogColor.BLUE)
        self._log.info(f"Base URL WebSocket {base_url_ws}.", LogColor.BLUE)

    def connect(self) -> None:
        """
        Connect the client to Binance.
        """
        self._log.info("Connecting...")
        self._loop.create_task(self._connect())

    def disconnect(self) -> None:
        """
        Disconnect the client from Binance.
        """
        self._log.info("Disconnecting...")
        self._loop.create_task(self._disconnect())

    async def _connect(self) -> None:
        # Connect HTTP client
        if not self._client.connected:
            await self._client.connect()
        try:
            await self._instrument_provider.initialize()
        except BinanceError as ex:
            self._log.exception("Error on connect", ex)
            return

        # Authenticate API key and update account(s)
        response: Dict[str, Any] = await self._http_account.account(recv_window=5000)

        self._authenticate_api_key(response=response)
        self._update_account_state(response=response)

        # Get listen keys
        if self._binance_account_type in (BinanceAccountType.SPOT, BinanceAccountType.MARGIN):
            response = await self._http_user.create_listen_key()
        else:
            response = await self._http_user.create_listen_key_futures()

        self._listen_key = response["listenKey"]
        self._ping_listen_keys_task = self._loop.create_task(self._ping_listen_keys())

        # Connect WebSocket client
        self._ws.subscribe(key=self._listen_key)
        await self._ws.connect()

        self._set_connected(True)
        self._log.info("Connected.")

    def _authenticate_api_key(self, response: Dict[str, Any]) -> None:
        if response["canTrade"]:
            self._log.info("Binance API key authenticated.", LogColor.GREEN)
            self._log.info(f"API key {self._client.api_key} has trading permissions.")
        else:
            self._log.error("Binance API key does not have trading permissions.")

    def _update_account_state(self, response: Dict[str, Any]) -> None:
        if self._binance_account_type.is_futures:
            balances = parse_account_balances_futures_http(raw_balances=response["assets"])
            margins = parse_account_margins_http(raw_balances=response["assets"])
        else:
            balances = parse_account_balances_spot_http(raw_balances=response["balances"])
            margins = []

        self.generate_account_state(
            balances=balances,
            margins=margins,
            reported=True,
            ts_event=response["updateTime"],
        )

    async def _ping_listen_keys(self) -> None:
        while True:
            self._log.debug(
                f"Scheduled `ping_listen_keys` to run in " f"{self._ping_listen_keys_interval}s."
            )
            await asyncio.sleep(self._ping_listen_keys_interval)
            if self._listen_key:
                self._log.debug(f"Pinging WebSocket listen key {self._listen_key}...")
                if self._binance_account_type.is_futures:
                    await self._http_user.ping_listen_key_futures(self._listen_key)
                else:
                    await self._http_user.ping_listen_key(self._listen_key)

    async def _disconnect(self) -> None:
        # Cancel tasks
        if self._ping_listen_keys_task:
            self._log.debug("Canceling `ping_listen_keys` task...")
            self._ping_listen_keys_task.cancel()

        # Disconnect WebSocket clients
        if self._ws.is_connected:
            await self._ws.disconnect()

        # Disconnect HTTP client
        if self._client.connected:
            await self._client.disconnect()

        self._set_connected(False)
        self._log.info("Disconnected.")

    # -- EXECUTION REPORTS -------------------------------------------------------------------------

    async def generate_order_status_report(
        self,
        venue_order_id: VenueOrderId = None,
    ) -> Optional[OrderStatusReport]:
        """
        Generate an order status report for the given venue order ID.

        If the order is not found, or an error occurs, then logs and returns
        ``None``.

        Parameters
        ----------
        venue_order_id : VenueOrderId, optional
            The venue order ID (assigned by the venue) query filter.

        Returns
        -------
        OrderStatusReport or ``None``

        """
        self._log.warning("Cannot generate OrderStatusReport: not yet implemented.")

        return None

    async def generate_order_status_reports(
        self,
        instrument_id: InstrumentId = None,
        start: datetime = None,
        end: datetime = None,
        open_only: bool = False,
    ) -> List[OrderStatusReport]:
        """
        Generate a list of order status reports with optional query filters.

        The returned list may be empty if no orders match the given parameters.

        Parameters
        ----------
        instrument_id : InstrumentId, optional
            The instrument ID query filter.
        start : datetime, optional
            The start datetime query filter.
        end : datetime, optional
            The end datetime query filter.
        open_only : bool, default False
            If the query is for open orders only.

        Returns
        -------
        list[OrderStatusReport]

        """
        self._log.warning("Cannot generate OrderStatusReports: not yet implemented.")

        return []

    async def generate_trade_reports(
        self,
        instrument_id: InstrumentId = None,
        venue_order_id: VenueOrderId = None,
        start: datetime = None,
        end: datetime = None,
    ) -> List[TradeReport]:
        """
        Generate a list of trade reports with optional query filters.

        The returned list may be empty if no trades match the given parameters.

        Parameters
        ----------
        instrument_id : InstrumentId, optional
            The instrument ID query filter.
        venue_order_id : VenueOrderId, optional
            The venue order ID (assigned by the venue) query filter.
        start : datetime, optional
            The start datetime query filter.
        end : datetime, optional
            The end datetime query filter.

        Returns
        -------
        list[TradeReport]

        """
        self._log.warning("Cannot generate TradeReports: not yet implemented.")

        return []

    async def generate_position_status_reports(
        self,
        instrument_id: InstrumentId = None,
        start: datetime = None,
        end: datetime = None,
    ) -> List[PositionStatusReport]:
        """
        Generate a list of position status reports with optional query filters.

        The returned list may be empty if no positions match the given parameters.

        Parameters
        ----------
        instrument_id : InstrumentId, optional
            The instrument ID query filter.
        start : datetime, optional
            The start datetime query filter.
        end : datetime, optional
            The end datetime query filter.

        Returns
        -------
        list[PositionStatusReport]

        """
        self._log.warning("Cannot generate PositionStatusReports: not yet implemented.")

        return []

    # -- COMMAND HANDLERS --------------------------------------------------------------------------

    def submit_order(self, command: SubmitOrder) -> None:
        order: Order = command.order

        # Check order type valid
        if self._binance_account_type.is_spot and order.type not in VALID_ORDER_TYPES_SPOT:
            self._log.error(
                f"Cannot submit order: {OrderTypeParser.to_str_py(order.type)} "
                f"orders not supported by the Binance exchange for SPOT accounts. "
                f"Use any of {[OrderTypeParser.to_str_py(t) for t in VALID_ORDER_TYPES_SPOT]}",
            )
            return
        elif self._binance_account_type.is_futures and order.type not in VALID_ORDER_TYPES_FUTURES:
            self._log.error(
                f"Cannot submit order: {OrderTypeParser.to_str_py(order.type)} "
                f"orders not supported by the Binance exchange for FUTURES accounts. "
                f"Use any of {[OrderTypeParser.to_str_py(t) for t in VALID_ORDER_TYPES_FUTURES]}",
            )
            return

        # Check time in force valid
        if order.time_in_force not in VALID_TIF:
            self._log.error(
                f"Cannot submit order: "
                f"{TimeInForceParser.to_str_py(order.time_in_force)} "
                f"not supported by the exchange. Use any of {VALID_TIF}.",
            )
            return

        # Check post-only
        if (
            self._binance_account_type.is_spot
            and order.type == OrderType.STOP_LIMIT
            and order.is_post_only
        ):
            self._log.error(
                "Cannot submit order: STOP_LIMIT `post_only` orders not supported by the Binance exchange for SPOT accounts. "
                "This order may become a liquidity TAKER."
            )
            return
        elif (
            self._binance_account_type.is_futures
            and order.is_post_only
            and order.type != OrderType.LIMIT
        ):
            self._log.error(
                f"Cannot submit order: {OrderTypeParser.to_str_py(order.type)} `post_only` order. "
                "Only LIMIT `post_only` orders supported by the Binance exchange for FUTURES accounts."
            )
            return

        self._loop.create_task(self._submit_order(order))

    def submit_order_list(self, command: SubmitOrderList) -> None:
        self._loop.create_task(self._submit_order_list(command))

    def modify_order(self, command: ModifyOrder) -> None:
        self._log.error(  # pragma: no cover
            "Cannot modify order: Not supported by the exchange.",
        )

    def cancel_order(self, command: CancelOrder) -> None:
        self._loop.create_task(self._cancel_order(command))

    def cancel_all_orders(self, command: CancelAllOrders) -> None:
        self._loop.create_task(self._cancel_all_orders(command))

    async def _submit_order(self, order: Order) -> None:
        self._log.debug(f"Submitting {order}.")

        # Generate event here to ensure correct ordering of events
        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        try:
            if self._binance_account_type.is_spot:
                await self._submit_order_spot(order)
            else:
                await self._submit_order_futures(order)
        except BinanceError as ex:
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=ex.message,  # type: ignore  # TODO(cs): Improve errors
                ts_event=self._clock.timestamp_ns(),
            )

    async def _submit_order_spot(self, order: Order) -> None:
        if order.type == OrderType.MARKET:
            await self._submit_market_order_spot(order)
        elif order.type == OrderType.LIMIT:
            await self._submit_limit_order_spot(order)
        elif order.type in (OrderType.STOP_LIMIT, OrderType.LIMIT_IF_TOUCHED):
            await self._submit_stop_limit_order_spot(order)

    async def _submit_order_futures(self, order: Order) -> None:
        if order.type == OrderType.MARKET:
            await self._submit_market_order_futures(order)
        elif order.type == OrderType.LIMIT:
            await self._submit_limit_order_futures(order)
        elif order.type in (OrderType.STOP_MARKET, OrderType.MARKET_IF_TOUCHED):
            await self._submit_stop_market_order_futures(order)
        elif order.type in (OrderType.STOP_LIMIT, OrderType.LIMIT_IF_TOUCHED):
            await self._submit_stop_limit_order_futures(order)
        elif order.type == OrderType.TRAILING_STOP_MARKET:
            await self._submit_trailing_stop_market_order_futures(order)

    ############################################################################
    # SPOT - Submit Order
    ############################################################################
    async def _submit_market_order_spot(self, order: MarketOrder) -> None:
        await self._http_account.new_order_spot(
            symbol=format_symbol(order.instrument_id.symbol.value),
            side=OrderSideParser.to_str_py(order.side),
            type="MARKET",
            quantity=str(order.quantity),
            new_client_order_id=order.client_order_id.value,
            recv_window=5000,
        )

    async def _submit_limit_order_spot(self, order: LimitOrder) -> None:
        time_in_force = TimeInForceParser.to_str_py(order.time_in_force)
        if order.is_post_only:
            time_in_force = None

        await self._http_account.new_order_spot(
            symbol=format_symbol(order.instrument_id.symbol.value),
            side=OrderSideParser.to_str_py(order.side),
            type=binance_order_type_spot(order),
            time_in_force=time_in_force,
            quantity=str(order.quantity),
            price=str(order.price),
            iceberg_qty=str(order.display_qty) if order.display_qty is not None else None,
            new_client_order_id=order.client_order_id.value,
            recv_window=5000,
        )

    async def _submit_stop_limit_order_spot(self, order: StopLimitOrder) -> None:
        await self._http_account.new_order_spot(
            symbol=format_symbol(order.instrument_id.symbol.value),
            side=OrderSideParser.to_str_py(order.side),
            type=binance_order_type_spot(order),
            time_in_force=TimeInForceParser.to_str_py(order.time_in_force),
            quantity=str(order.quantity),
            price=str(order.price),
            stop_price=str(order.trigger_price),
            iceberg_qty=str(order.display_qty) if order.display_qty is not None else None,
            new_client_order_id=order.client_order_id.value,
            recv_window=5000,
        )

    ############################################################################
    # FUTURES - Submit Order
    ############################################################################
    async def _submit_market_order_futures(self, order: MarketOrder) -> None:
        await self._http_account.new_order_futures(
            symbol=format_symbol(order.instrument_id.symbol.value),
            side=OrderSideParser.to_str_py(order.side),
            type="MARKET",
            quantity=str(order.quantity),
            new_client_order_id=order.client_order_id.value,
            recv_window=5000,
        )

    async def _submit_limit_order_futures(self, order: LimitOrder) -> None:
        time_in_force = TimeInForceParser.to_str_py(order.time_in_force)
        if order.is_post_only:
            time_in_force = "GTX"

        await self._http_account.new_order_futures(
            symbol=format_symbol(order.instrument_id.symbol.value),
            side=OrderSideParser.to_str_py(order.side),
            type=binance_order_type_futures(order),
            time_in_force=time_in_force,
            quantity=str(order.quantity),
            price=str(order.price),
            reduce_only=order.is_reduce_only,  # Cannot be sent with Hedge-Mode or closePosition
            new_client_order_id=order.client_order_id.value,
            recv_window=5000,
        )

    async def _submit_stop_market_order_futures(self, order: StopMarketOrder) -> None:
        if order.trigger_type in (TriggerType.DEFAULT, TriggerType.LAST):
            working_type = "CONTRACT_PRICE"
        elif order.trigger_type == TriggerType.MARK:
            working_type = "MARK_PRICE"
        else:
            self._log.error(
                f"Cannot submit order: invalid `order.trigger_type`, was "
                f"{TriggerTypeParser.to_str_py(order.trigger_price)}. {order}",
            )
            return

        await self._http_account.new_order_futures(
            symbol=format_symbol(order.instrument_id.symbol.value),
            side=OrderSideParser.to_str_py(order.side),
            type=binance_order_type_futures(order),
            time_in_force=TimeInForceParser.to_str_py(order.time_in_force),
            quantity=str(order.quantity),
            stop_price=str(order.trigger_price),
            working_type=working_type,
            reduce_only=order.is_reduce_only,  # Cannot be sent with Hedge-Mode or closePosition
            new_client_order_id=order.client_order_id.value,
            recv_window=5000,
        )

    async def _submit_stop_limit_order_futures(self, order: StopMarketOrder) -> None:
        if order.trigger_type in (TriggerType.DEFAULT, TriggerType.LAST):
            working_type = "CONTRACT_PRICE"
        elif order.trigger_type == TriggerType.MARK:
            working_type = "MARK_PRICE"
        else:
            self._log.error(
                f"Cannot submit order: invalid `order.trigger_type`, was "
                f"{TriggerTypeParser.to_str_py(order.trigger_price)}. {order}",
            )
            return

        await self._http_account.new_order_futures(
            symbol=format_symbol(order.instrument_id.symbol.value),
            side=OrderSideParser.to_str_py(order.side),
            type=binance_order_type_futures(order),
            time_in_force=TimeInForceParser.to_str_py(order.time_in_force),
            quantity=str(order.quantity),
            price=str(order.price),
            stop_price=str(order.trigger_price),
            working_type=working_type,
            reduce_only=order.is_reduce_only,  # Cannot be sent with Hedge-Mode or closePosition
            new_client_order_id=order.client_order_id.value,
            recv_window=5000,
        )

    async def _submit_trailing_stop_market_order_futures(
        self, order: TrailingStopMarketOrder
    ) -> None:
        if order.trigger_type in (TriggerType.DEFAULT, TriggerType.LAST):
            working_type = "CONTRACT_PRICE"
        elif order.trigger_type == TriggerType.MARK:
            working_type = "MARK_PRICE"
        else:
            self._log.error(
                f"Cannot submit order: invalid `order.trigger_type`, was "
                f"{TriggerTypeParser.to_str_py(order.trigger_price)}. {order}",
            )
            return

        if order.offset_type not in (TrailingOffsetType.DEFAULT, TrailingOffsetType.BASIS_POINTS):
            self._log.error(
                f"Cannot submit order: invalid `order.offset_type`, was "
                f"{TrailingOffsetTypeParser.to_str_py(order.offset_type)} (use `BASIS_POINTS`). "
                f"{order}",
            )
            return

        await self._http_account.new_order_futures(
            symbol=format_symbol(order.instrument_id.symbol.value),
            side=OrderSideParser.to_str_py(order.side),
            type=binance_order_type_futures(order),
            time_in_force=TimeInForceParser.to_str_py(order.time_in_force),
            quantity=str(order.quantity),
            activation_price=str(order.trigger_price),
            callback_rate=str(order.trailing_offset),
            working_type=working_type,
            reduce_only=order.is_reduce_only,  # Cannot be sent with Hedge-Mode or closePosition
            new_client_order_id=order.client_order_id.value,
            recv_window=5000,
        )

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        for order in command.list:
            if order.linked_order_ids:  # TODO(cs): Implement
                self._log.warning(f"Cannot yet handle OCO conditional orders, {order}.")
            await self._submit_order(order)

    async def _cancel_order(self, command: CancelOrder) -> None:
        self._log.debug(f"Canceling order {command.client_order_id.value}.")

        self.generate_order_pending_cancel(
            strategy_id=command.strategy_id,
            instrument_id=command.instrument_id,
            client_order_id=command.client_order_id,
            venue_order_id=command.venue_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        try:
            if command.venue_order_id is not None:
                await self._http_account.cancel_order(
                    symbol=format_symbol(command.instrument_id.symbol.value),
                    order_id=command.venue_order_id.value,
                )
            else:
                await self._http_account.cancel_order(
                    symbol=format_symbol(command.instrument_id.symbol.value),
                    orig_client_order_id=command.client_order_id.value,
                )
        except BinanceError as ex:
            self._log.error(ex.message)  # type: ignore  # TODO(cs): Improve errors

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        self._log.debug(f"Canceling all orders for {command.instrument_id.value}.")

        # Cancel all in-flight orders
        inflight_orders = self._cache.orders_inflight(
            instrument_id=command.instrument_id,
            strategy_id=command.strategy_id,
        )
        for order in inflight_orders:
            self.generate_order_pending_cancel(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=order.venue_order_id,
                ts_event=self._clock.timestamp_ns(),
            )

        # Cancel all open orders
        open_orders = self._cache.orders_open(
            instrument_id=command.instrument_id,
            strategy_id=command.strategy_id,
        )
        for order in open_orders:
            self.generate_order_pending_cancel(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=order.venue_order_id,
                ts_event=self._clock.timestamp_ns(),
            )

        try:
            if self._binance_account_type.is_spot:
                await self._http_account.cancel_open_orders_spot(
                    symbol=format_symbol(command.instrument_id.symbol.value),
                )
            elif self._binance_account_type.is_futures:
                await self._http_account.cancel_open_orders_futures(
                    symbol=format_symbol(command.instrument_id.symbol.value),
                )
        except BinanceError as ex:
            self._log.error(ex.message)  # type: ignore  # TODO(cs): Improve errors

    def _handle_user_ws_message(self, raw: bytes):
        msg: Dict[str, Any] = orjson.loads(raw)
        data: Dict[str, Any] = msg.get("data")

        # TODO(cs): Uncomment for development
        # self._log.info(str(json.dumps(msg, indent=4)), color=LogColor.GREEN)

        try:
            msg_type: str = data.get("e")
            if msg_type == "outboundAccountPosition":
                self._handle_account_update_spot(data)
            elif msg_type == "executionReport":  # SPOT
                self._handle_execution_report_spot(data)
            elif msg_type == "ACCOUNT_UPDATE":  # FUTURES
                self._handle_account_update_futures(data)
            elif msg_type == "ORDER_TRADE_UPDATE":  # FUTURES
                ts_event = millis_to_nanos(data["E"])
                self._handle_execution_report_futures(data["o"], ts_event)
        except Exception as ex:
            self._log.exception(f"Error on handling {repr(msg)}", ex)

    def _handle_account_update_spot(self, data: Dict[str, Any]):
        self.generate_account_state(
            balances=parse_account_balances_spot_ws(raw_balances=data["B"]),
            margins=[],
            reported=True,
            ts_event=millis_to_nanos(data["u"]),
        )

    def _handle_account_update_futures(self, data: Dict[str, Any]):
        self.generate_account_state(
            balances=parse_account_balances_futures_ws(raw_balances=data["a"]["B"]),
            margins=[],
            reported=True,
            ts_event=millis_to_nanos(data["T"]),
        )

    def _handle_execution_report_spot(self, data: Dict[str, Any]):
        execution_type: str = data["x"]

        # Parse instrument ID
        symbol: str = parse_symbol(data["s"], account_type=self._binance_account_type)
        instrument_id: Optional[InstrumentId] = self._instrument_ids.get(symbol)
        if not instrument_id:
            instrument_id = InstrumentId(Symbol(symbol), BINANCE_VENUE)
            self._instrument_ids[symbol] = instrument_id

        # Parse client order ID
        client_order_id_str: str = data["c"]
        if not client_order_id_str or not client_order_id_str.startswith("O"):
            client_order_id_str = data["C"]
        client_order_id = ClientOrderId(client_order_id_str)

        # Fetch strategy ID
        strategy_id: StrategyId = self._cache.strategy_id_for_order(client_order_id)
        if strategy_id is None:
            # TODO(cs): Implement external order handling
            self._log.error(
                f"Cannot handle trade report: strategy ID for {client_order_id} not found.",
            )
            return

        venue_order_id = VenueOrderId(str(data["i"]))
        order_type_str: str = data["o"]
        ts_event: int = millis_to_nanos(data["E"])

        if execution_type == "NEW":
            self.generate_order_accepted(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )
        elif execution_type in "TRADE":
            instrument: Instrument = self._instrument_provider.find(instrument_id=instrument_id)

            # Determine commission
            commission_asset: str = data["N"]
            commission_amount: str = data["n"]
            if commission_asset is not None:
                commission = Money.from_str(f"{commission_amount} {commission_asset}")
            else:
                # Binance typically charges commission as base asset or BNB
                commission = Money(0, instrument.base_currency)

            self.generate_order_filled(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,  # NETTING accounts
                trade_id=TradeId(str(data["t"])),  # Trade ID
                order_side=OrderSideParser.from_str_py(data["S"]),
                order_type=parse_order_type(order_type_str),
                last_qty=Quantity.from_str(data["l"]),
                last_px=Price.from_str(data["L"]),
                quote_currency=instrument.quote_currency,
                commission=commission,
                liquidity_side=LiquiditySide.MAKER if data["m"] else LiquiditySide.TAKER,
                ts_event=ts_event,
            )
        elif execution_type == "CANCELED" or execution_type == "EXPIRED":
            self.generate_order_canceled(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )

    def _handle_execution_report_futures(self, data: Dict[str, Any], ts_event: int):
        execution_type: str = data["x"]

        # Parse instrument ID
        symbol: str = parse_symbol(data["s"], account_type=self._binance_account_type)
        instrument_id: Optional[InstrumentId] = self._instrument_ids.get(symbol)
        if not instrument_id:
            instrument_id = InstrumentId(Symbol(symbol), BINANCE_VENUE)
            self._instrument_ids[symbol] = instrument_id

        # Parse client order ID
        client_order_id_str: str = data.get("c")
        if not client_order_id_str:
            client_order_id_str = data.get("C")
        client_order_id = ClientOrderId(client_order_id_str)

        # Fetch strategy ID
        strategy_id: StrategyId = self._cache.strategy_id_for_order(client_order_id)
        if strategy_id is None:
            # TODO(cs): Implement external order handling
            self._log.error(
                f"Cannot handle trade report: strategy ID for {client_order_id} not found.",
            )
            return

        venue_order_id = VenueOrderId(str(data["i"]))
        order_type_str: str = data["o"]

        if execution_type == "NEW":
            self.generate_order_accepted(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )
        elif execution_type in "TRADE":
            instrument: Instrument = self._instrument_provider.find(instrument_id=instrument_id)

            # Determine commission
            commission_asset: str = data["N"]
            commission_amount: str = data["n"]
            if commission_asset is not None:
                commission = Money.from_str(f"{commission_amount} {commission_asset}")
            else:
                # Binance typically charges commission as base asset or BNB
                commission = Money(0, instrument.base_currency)

            self.generate_order_filled(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,  # NETTING accounts
                trade_id=TradeId(str(data["t"])),  # Trade ID
                order_side=OrderSideParser.from_str_py(data["S"]),
                order_type=parse_order_type(order_type_str),
                last_qty=Quantity.from_str(data["l"]),
                last_px=Price.from_str(data["L"]),
                quote_currency=instrument.quote_currency,
                commission=commission,
                liquidity_side=LiquiditySide.MAKER if data["m"] else LiquiditySide.TAKER,
                ts_event=ts_event,
            )
        elif execution_type == "CANCELED" or execution_type == "EXPIRED":
            self.generate_order_canceled(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )