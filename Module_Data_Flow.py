# -*- coding: utf-8 -*-
"""
Created on Tue Sep  9 22:04:21 2025


[Module_Data_Flow.py]

Первый Модуль в Торговом боте -- его задача это получать поток данных о совершенных сделках
Получать данные будет от биржи по обозначенному инструменту(-ам)


Binance:
wss://fstream.binance.com/ws/btcusdt@depth == Order Book (Depth stream)→ поток апдейтов по bid/ask стакану (лимитные заявки).
wss://fstream.binance.com/ws/btcusdt@trade == Trades stream → каждое исполнение сделки: цена, объём, направление (buy/sell).
wss://fstream.binance.com/ws/btcusdt@kline_1m == Свечи → OHLC + Volume

"""


import asyncio
import json
import websockets

from datetime import datetime, timezone

class BinanceDataFlow:
    def __init__(self, symbols, on_trade, stop_event: asyncio.Event):
        self.symbols = symbols
        self.on_trade = on_trade
        self.stop_event = stop_event
        self.ws = None

    async def _connect(self):
        streams = "/".join([f"{s}@trade" for s in self.symbols])
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        print(f"🔌 Подключаемся к {url}")
        self.ws = await websockets.connect(url)
        return self.ws

    async def _listen(self):
        try:
            async for message in self.ws:
                if self.stop_event.is_set():
                    break  # мягкий выход
                data = json.loads(message)
                payload = data.get("data", {})

                trade = {
                    "timestamp": datetime.fromtimestamp(payload.get("T") / 1000, tz=timezone.utc),
                    "price": float(payload.get("p", 0)),
                    "qty": float(payload.get("q", 0)),
                    "side": "buy" if payload.get("m") is False else "sell"
                }

                await self.on_trade(trade, payload.get("s", "").lower())
        except asyncio.CancelledError:
            print("🛑 Задача _listen отменена")
        except Exception as e:
            print(f"⚠️ Ошибка в _listen: {e}")
        finally:
            if self.ws:
                await self.ws.close()
                print("🔌 WebSocket закрыт")

    async def run(self):
        while not self.stop_event.is_set():
            try:
                await self._connect()
                await self._listen()
            except Exception as e:
                print(f"⚠️ Ошибка подключения: {e}, пробуем переподключиться через 5 секунд...")
                await asyncio.sleep(5)

        # финальное закрытие при stop_event
        if self.ws:
            try:
                await self.ws.close()
                print("🔌 Соединение закрыто (stop_event)")
            except Exception as e:
                print(f"⚠️ Ошибка при закрытии соединения: {e}")