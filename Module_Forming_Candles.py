# -*- coding: utf-8 -*-
"""
Created on Tue Sep  9 23:45:28 2025

[Module_Forming_Candles.py]


"""



import datetime
from typing import List, Dict, Optional

UTC = datetime.timezone.utc

class CandleFormer:
    
    def __init__(self, timeframes: List[str] = ["1m"], start_time: Optional[datetime.datetime] = None, signal_manager=None):
        """
        timeframes: список строк, например ["1m","5m","1h","1d"]
        start_time: момент запуска (если None, берётся текущий UTC). Используется чтобы
                    не формировать "частичные" свечи, начавшие до старта.
        """
        self.timeframes = timeframes
        self.run_start = self._ensure_utc(start_time or datetime.datetime.utcnow().replace(tzinfo=UTC))
        # первый допустимый бакет для каждого ТФ (не будем формировать бакеты, начавшиеся раньше)
        self.first_allowed_bucket = {tf: self._get_first_allowed_bucket(self.run_start, tf) for tf in timeframes}
        # текущее состояние по символам: {symbol: {tf: candle_or_None}}
        self.current_candles: Dict[str, Dict[str, Optional[dict]]] = {}
        
        self.signal_manager = signal_manager

    # ----------------- вспомогательные функции -----------------
    def _ensure_utc(self, ts: datetime.datetime) -> datetime.datetime:
        if ts is None:
            return datetime.datetime.utcnow().replace(tzinfo=UTC)
        if ts.tzinfo is None:
            return ts.replace(tzinfo=UTC)
        return ts.astimezone(UTC)

    def _tf_timedelta(self, tf: str) -> datetime.timedelta:
        if tf.endswith("m"):
            return datetime.timedelta(minutes=int(tf[:-1]))
        if tf.endswith("h"):
            return datetime.timedelta(hours=int(tf[:-1]))
        if tf.endswith("d"):
            return datetime.timedelta(days=int(tf[:-1]))
        raise ValueError(f"Unknown timeframe {tf}")

    def _get_candle_start(self, ts: datetime.datetime, tf: str) -> datetime.datetime:
        """
        Возвращает начало бакета (timezone-aware UTC).
        """
        ts = self._ensure_utc(ts)
        if tf.endswith("m"):
            m = int(tf[:-1])
            minute_bucket = (ts.minute // m) * m
            return ts.replace(second=0, microsecond=0, minute=minute_bucket)
        if tf.endswith("h"):
            h = int(tf[:-1])
            hour_bucket = (ts.hour // h) * h
            return ts.replace(second=0, microsecond=0, minute=0, hour=hour_bucket)
        if tf.endswith("d"):
            return ts.replace(second=0, microsecond=0, minute=0, hour=0)
        raise ValueError(f"Unknown timeframe {tf}")

    def _get_candle_close(self, start: datetime.datetime, tf: str) -> datetime.datetime:
        return start + self._tf_timedelta(tf)

    def _get_first_allowed_bucket(self, start: datetime.datetime, tf: str) -> datetime.datetime:
        """
        Если старт был ровно на границе (например 00:40:00 для 5m), разрешаем этот бакет.
        Если старт в середине бакета (00:38:xx), то первый разрешённый = следующий бакет (00:40:00).
        """
        start = self._ensure_utc(start)
        bucket = self._get_candle_start(start, tf)
        if start == bucket:
            return bucket
        return bucket + self._tf_timedelta(tf)

    # ----------------- основной обработчик -----------------
    def process_trade(self, trade: dict, symbol: str):
        
        price = float(trade["price"])
        qty = float(trade["qty"])
    
        # фильтр: не учитываем сделки с нулевой ценой или количеством (иначе сделают Low = 0.0)
        if price <= 0.0 or qty <= 0.0:
            return
    
        ts = self._ensure_utc(trade["timestamp"])
    
        if symbol not in self.current_candles:
            self.current_candles[symbol] = {tf: None for tf in self.timeframes}
    
        for tf in self.timeframes:
            bucket = self._get_candle_start(ts, tf)
            if bucket < self.first_allowed_bucket[tf]:
                continue
    
            candle = self.current_candles[symbol].get(tf)
    
            # если свеча есть и истекла — закрываем
            if candle is not None and ts >= candle["close_time"]:
                self._emit_close(symbol, tf, candle)
                candle = None
                self.current_candles[symbol][tf] = None
    
            # если свеча отсутствует или новый бакет — создаём
            if candle is None or candle["open_time"] != bucket:
                close_time = self._get_candle_close(bucket, tf)
                candle = {
                    "open_time": bucket,
                    "close_time": close_time,
                    "O": trade["price"],
                    "H": trade["price"],
                    "L": trade["price"],
                    "C": trade["price"],
                    "Volume": trade["qty"],
                    "AskVolume": trade["qty"] if trade["side"] == "buy" else 0.0,
                    "BidVolume": trade["qty"] if trade["side"] == "sell" else 0.0,
                    "Delta": trade["qty"] if trade["side"] == "buy" else -trade["qty"],
                    "TradeCount": 1
                }
                self.current_candles[symbol][tf] = candle
                continue  # больше ничего не делаем, свеча только создана
    
            # обновляем существующую свечу
            price = trade["price"]
            candle["H"] = max(candle["H"], price)
            candle["L"] = min(candle["L"], price)
            
            candle["C"] = price
    
            candle["Volume"] += trade["qty"]
            if trade["side"] == "buy":
                candle["AskVolume"] += trade["qty"]
            else:
                candle["BidVolume"] += trade["qty"]
            candle["Delta"] = candle["AskVolume"] - candle["BidVolume"]
            candle["TradeCount"] += 1
            
            
            # print(f"L={candle['L']}")

    # ----------------- вывод / интеграция -----------------
    def _emit_close(self, symbol: str, tf: str, candle: dict):
        """
        В MVP просто печатаем — позже можно заменить коллбеком или записью в БД.
        Форматируем время ISO в UTC для удобства.
        """
        print(f"[{symbol.upper()}][{tf}] CLOSE @ {candle['open_time'].isoformat()} -> "
              f"O={candle['O']} H={candle['H']} L={candle['L']} C={candle['C']} "
              f"Ask={candle['AskVolume']}, Bid={candle['BidVolume']} "
              f"Vol={candle['Volume']} Δ={candle['Delta']} trades={candle['TradeCount']}")
        
        # хуйнюшка для передачи свечей в следующий модуль с Моделями
        if self.signal_manager:
            self.signal_manager.on_candle(symbol, tf, candle)
        else:
            print(f"[{symbol.upper()}][{tf}] CLOSE -> {candle}")