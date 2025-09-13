# -*- coding: utf-8 -*-
"""
Created on Tue Sep  9 22:34:56 2025

[Module_Main.py]


"""

import asyncio
import nest_asyncio
nest_asyncio.apply()

from Module_Data_Flow import BinanceDataFlow
from Module_Forming_Candles import CandleFormer
from Module_Models_Scanning import SignalManager, DummyModel
from Module_Trading import TradeExecutor


stop_event = asyncio.Event()


"""
SignalManager может работать в двух режимах:
1. С trade_executor — сразу торгует.
2. Без trade_executor — просто пишет в лог (чтобы можно было тестить модели, не отправляя ордера).
    signal_manager = SignalManager()  # без trade_executor
    В этом случае сигналы печатаются, но ордера не открываются.
"""
# создаём трейд-исполнитель (через Binance Futures API)
trade_executor = TradeExecutor(config_path="C:\\Users\Mkx\Desktop\config_binance_testnet.json")
# создаём менеджер сигналов
signal_manager = SignalManager(trade_executor=trade_executor)
# регистрируем модель (или несколько)
signal_manager.register_model(DummyModel(
    "trend_model", ["1m", "5m"], ["ethusdt"] # "btcusdt", 
))


async def console_listener():
    """Отдельная задача для остановки через ввод команды"""
    loop = asyncio.get_event_loop()
    while not stop_event.is_set():
        cmd = await loop.run_in_executor(None, input, ">>> ")  # блокирующий input в отдельном потоке
        if cmd.strip().lower() == "stop":
            print("⏹ Команда 'stop' получена, завершаем работу...")
            stop_event.set()

async def main():
    symbols = ["ethusdt"]  # "btcusdt", 
    timeframes = ["1m", "5m"]

    candle_former = CandleFormer(timeframes=timeframes, signal_manager=signal_manager)

    async def on_trade(trade, symbol):
        if not stop_event.is_set():
            candle_former.process_trade(trade, symbol)

    data_flow = BinanceDataFlow(symbols, on_trade, stop_event)

    # запускаем сразу две задачи: поток данных и слушатель консоли
    await asyncio.gather(
        data_flow.run(),
        console_listener()
    )

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        stop_event.set()
        print("⏹ Остановлено пользователем (Ctrl+C)")
