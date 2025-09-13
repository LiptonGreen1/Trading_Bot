# -*- coding: utf-8 -*-
"""
Created on Sat Sep 13 18:06:35 2025


[Module_Trading.py]

"""

import datetime
import json
import uuid
from binance.client import Client
from binance.enums import *

class TradeExecutor:
    def __init__(self, config_path="config.json"):
        # загружаем ключи
        with open(config_path, "r") as f:
            cfg = json.load(f)

        self.api_key = cfg["api_key"]
        self.api_secret = cfg["api_secret"]
        self.testnet = cfg.get("testnet", True)

        # подключаемся к binance futures
        self.client = Client(self.api_key, self.api_secret, testnet=self.testnet)

        # активные сделки
        self.active_trades = {}

        print(f"✅ TradeExecutor подключён (testnet={self.testnet})")

    def on_signal(self, signal: dict):
        """
        Получаем сигнал от модели и открываем сделку на Binance Futures
        """
        trade_id = str(uuid.uuid4())[:8]
        symbol = signal["symbol"].upper()
        direction = signal["direction"]
        target_move = signal.get("target_move", 0.005)  # по умолчанию 0.5%
        qty = signal.get("qty", 0.001)  # размер позиции (BTC: 0.001, ETH: 0.01 и т.п.)
        now = datetime.datetime.utcnow().isoformat()

        side = SIDE_BUY if direction.lower() == "buy" else SIDE_SELL

        try:
            order = self.client.futures_create_order(
                symbol=symbol.upper(),
                type=FUTURE_ORDER_TYPE_MARKET,
                side=side,
                quantity=qty
            )

            print(f"📈 [{now}] OPEN {direction.upper()} {qty} {symbol} "
                  f"| target move {target_move*100:.2f}% | Binance orderId={order['orderId']}")

            # сохраняем сделку
            self.active_trades[trade_id] = {
                "symbol": symbol,
                "direction": direction,
                "opened_at": now,
                "target_move": target_move,
                "status": "OPEN",
                "binance_order": order
            }

            return trade_id

        except Exception as e:
            print(f"❌ Ошибка при открытии ордера: {e}")
            return None

    def list_trades(self):
        """
        Вывести список активных сделок
        """
        return self.active_trades