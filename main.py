import json
import uuid
import asyncio
import typing as t
import dataclasses

import websockets

api_url = "wss://test.deribit.com/ws/api/v2"
client_id, client_secret = "XXX", "YYY"
ticker = "BTC-PERPETUAL"


class AsyncClient:
    def __init__(self, socket: websockets.WebSocketClientProtocol, *, heartbeat: int):
        self.socket = socket
        self.heartbeat = heartbeat
        self.pending_responses = {}
        self.receiver = None
        self.handle_message = None
        self.handle_heartbeat = None

    def __enter__(self):
        self.receiver = asyncio.create_task(self.receive())
        return self

    def __exit__(self, *args):
        if self.receiver:
            self.receiver.cancel()

    async def receive(self):
        while self.socket.open:
            try:
                message_text = await asyncio.wait_for(self.socket.recv(), timeout=self.heartbeat)
            except asyncio.TimeoutError:
                if self.handle_heartbeat:
                    asyncio.create_task(self.handle_heartbeat())
                continue
            message = json.loads(message_text)
            try:
                response_id = message["id"]
                queue = self.pending_responses[response_id]
                await queue.put(message)
            except KeyError:
                if self.handle_message:
                    asyncio.create_task(self.handle_message(message))

    async def send(self, method: str, **params):
        request = {"jsonrpc": "2.0", "method": method, "params": params}
        request_text = json.dumps(request)
        await self.socket.send(request_text)

    async def request(self, method: str, **params):
        request_id = str(uuid.uuid4())
        request = {"jsonrpc": "2.0", "method": method, "params": params, "id": request_id}
        request_text = json.dumps(request)

        queue = self.pending_responses[request_id] = asyncio.Queue()
        await self.socket.send(request_text)

        futures = [queue.get(), self.receiver]
        done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)

        response = None
        for task in done:
            # Raises an exception if task is self.receiver
            result = task.result()
            if task is not self.receiver:
                response = result

        del self.pending_responses[request_id]
        return response


class OrderBook:
    def __init__(self, client: AsyncClient, instrument_name: str):
        self.client = client
        self.instrument_name = instrument_name
        self.access_token = None

    async def login(self):
        response = await self.client.request(
            "public/auth", grant_type="client_credentials", client_id=client_id, client_secret=client_secret
        )
        try:
            result = response["result"]
            self.access_token = result["access_token"]
            print("auth token", self.access_token)
        except KeyError:
            raise Exception(f"Auth error: {response}")

    async def bid_ask(self):
        response = await self.client.request("public/ticker", instrument_name=self.instrument_name)
        result = response["result"]
        bid, ask = result["best_bid_price"], result["best_ask_price"]
        return bid, ask

    async def subscribe(self):
        await self.client.request(
            "private/subscribe",
            access_token=self.access_token,
            channels=[f"user.orders.{self.instrument_name}.raw"],
        )

    async def limit_order(self, way: t.Literal["buy", "sell"], amount: int, price: float):
        await self.client.request(
            f"private/{way}",
            access_token=self.access_token,
            instrument_name=self.instrument_name,
            amount=amount,
            type="limit",
            price=price,
            post_only=True,
        )

    async def open_orders(self):
        response = await self.client.request(
            "private/get_open_orders_by_instrument",
            access_token=self.access_token,
            instrument_name=self.instrument_name,
            type="limit",
        )
        return [Order(**order) for order in response["result"]]


@dataclasses.dataclass
class Order:
    amount: int
    api: bool
    average_price: float
    commission: float
    creation_timestamp: int
    direction: t.Literal["buy", "sell"]
    filled_amount: int
    instrument_name: str
    is_liquidation: bool
    label: str
    last_update_timestamp: int
    max_show: int
    order_id: str
    order_state: t.Literal["open", "filled", "rejected", "cancelled", "untriggered"]
    order_type: t.Literal["limit", "market", "liquidation"]
    post_only: bool
    price: float
    profit_loss: float
    reduce_only: bool
    replaced: bool
    time_in_force: t.Literal["good_til_cancelled", "fill_or_kill", "immediate_or_cancel"]
    web: bool

    def __repr__(self):
        order = f"{self.order_state} {self.direction} {self.price}"
        if self.filled_amount != self.amount and self.filled_amount != 0:
            order = f"{order} {self.filled_amount}/{self.amount}"
        if self.profit_loss != 0.0:
            order = f"{order} pnl={1e8 * self.profit_loss} sat"
        return f"{type(self).__name__}({order})"


class MarketMaker:
    def __init__(self, book: OrderBook):
        self.book = book
        self.spread = 1.0
        self.amount = 1000

    async def on_start(self):
        await self.book.login()

        bid, ask = await self.book.bid_ask()
        print("bid ask", bid, ask)

        await self.book.subscribe()
        await self.book.limit_order("buy", amount=self.amount, price=bid)
        await self.book.limit_order("sell", amount=self.amount, price=ask)

    async def on_message(self, message: dict):
        if message.get("method") != "subscription":
            return

        params = message["params"]
        order = Order(**params["data"])
        print(order)

        if order.order_state == "filled" and order.order_type != "stop_limit":
            if order.profit_loss > 0.0:
                await self.book.limit_order("buy", amount=self.amount, price=order.price - self.spread)
                await self.book.limit_order("sell", amount=self.amount, price=order.price + self.spread)

    async def on_heartbeat(self):
        bid, ask = await self.book.bid_ask()
        orders = await self.book.open_orders()

        buy_prices = set(order.price for order in orders if order.direction == "buy")
        sell_prices = set(order.price for order in orders if order.direction == "sell")

        if bid not in buy_prices:
            await self.book.limit_order("buy", amount=self.amount, price=bid)
        if ask not in sell_prices:
            await self.book.limit_order("sell", amount=self.amount, price=ask)


async def main():
    async with websockets.connect(api_url) as socket:
        with AsyncClient(socket, heartbeat=10) as client:
            book = OrderBook(client, instrument_name=ticker)
            market_maker = MarketMaker(book)
            client.handle_message = market_maker.on_message
            client.handle_heartbeat = market_maker.on_heartbeat
            await market_maker.on_start()
            while socket.open:
                await client.receiver


asyncio.get_event_loop().run_until_complete(main())
