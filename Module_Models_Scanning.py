# -*- coding: utf-8 -*-
"""
Created on Sat Sep 13 17:36:02 2025

[Module_Models_Scanning.py]

"""

import collections
from typing import List, Dict, Optional

class BaseModel:
    def __init__(self, name: str, timeframes: List[str], symbols: List[str] = ["*"],
                 context_len: int = 20, forecast_horizon: int = 5, min_deviation: float = 0.5):
        self.name = name
        self.timeframes = timeframes
        self.symbols = symbols
        self.context_len = context_len
        self.forecast_horizon = forecast_horizon
        self.min_deviation = min_deviation

    def predict(self, candles: List[dict]) -> Optional[str]:
        """
        Реализуется в наследниках.
        Возвращает 'BUY' | 'SELL' | None.
        """
        raise NotImplementedError


class DummyModel(BaseModel):
    """Простейший пример: если последняя свеча зелёная -> BUY, красная -> SELL"""
    def predict(self, candles: List[dict]) -> Optional[str]:
        if len(candles) < 1:
            return None
        last = candles[-1]
        if last["C"] > last["O"]:
            return "BUY"
        elif last["C"] < last["O"]:
            return "SELL"
        return None


class SignalManager:
    def __init__(self, trade_executor=None):
        self.models: List[BaseModel] = []
        # История по символам/ТФ: {symbol: {tf: deque[candles]}}
        self.history: Dict[str, Dict[str, collections.deque]] = {}
        
        self.trade_executor = trade_executor

    def register_model(self, model: BaseModel):
        self.models.append(model)

    def on_candle(self, symbol: str, tf: str, candle: dict):
        # сохраняем историю
        if symbol not in self.history:
            self.history[symbol] = {}
        if tf not in self.history[symbol]:
            self.history[symbol][tf] = collections.deque(maxlen=500)
    
        self.history[symbol][tf].append(candle)
    
        # проверяем модели
        for model in self.models:
            if tf not in model.timeframes:
                continue
            if model.symbols != ["*"] and symbol not in model.symbols:
                continue
    
            ctx = list(self.history[symbol][tf])[-model.context_len:]
            signal = model.predict(ctx)
            if signal:
                signal["symbol"] = symbol       # обязательно, чтобы TradeExecutor понял, чем торговать
                signal["timeframe"] = tf        # можно добавить инфу для логов
    
                if self.trade_executor:
                    self.trade_executor.on_signal(signal)
                else:
                    print(f"[SIGNAL] {model.name} | {symbol.upper()} {tf} -> {signal}")
                
                """
                SignalManager может работать в двух режимах:
                1. С trade_executor — сразу торгует.
                2. Без trade_executor — просто пишет в лог (чтобы можно было тестить модели, не отправляя ордера).
                
                """