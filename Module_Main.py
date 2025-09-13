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

stop_event = asyncio.Event()

async def console_listener():
    """Отдельная задача для остановки через ввод команды"""
    loop = asyncio.get_event_loop()
    while not stop_event.is_set():
        cmd = await loop.run_in_executor(None, input, ">>> ")  # блокирующий input в отдельном потоке
        if cmd.strip().lower() == "stop":
            print("⏹ Команда 'stop' получена, завершаем работу...")
            stop_event.set()

async def main():
    symbols = ["btcusdt", "ethusdt"]
    timeframes = ["1m", "5m"]

    candle_former = CandleFormer(timeframes=timeframes)

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
