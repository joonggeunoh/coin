# multi_symbols_to_csv.py
# -*- coding: utf-8 -*-
"""
여러 심볼(예: ['XRPUSDT','ADAUSDT', ...])을 입력하면
바이낸스 스팟 kline을 받아 CSV로 저장합니다.
- 기본: 1시간봉, 2017-01-01 ~ 2025-12-31 (UTC 기준)
- 시간 컬럼은 Asia/Seoul로 변환 후 tz 제거 → CSV에 ISO 문자열로 기록
- 빈 데이터/미상장 심볼은 자동 건너뜀
"""

import time
import datetime as dt
from typing import List
import requests
import pandas as pd

BASE_URL = "https://api.binance.com/api/v3/klines"

DEFAULT_SYMBOLS = [
    # 2017~2025 사이 장기간 활발 (알트 위주)
    "XRPUSDT","LTCUSDT","BCHUSDT","ADAUSDT","XLMUSDT",
    "TRXUSDT","EOSUSDT","NEOUSDT","ETCUSDT","BNBUSDT",
    "LINKUSDT","DOGEUSDT","IOTAUSDT","QTUMUSDT","WAVESUSDT",
    # 필요 시 추가: "XEMUSDT","ZECUSDT","DASHUSDT" 등 (상태에 따라 빈 데이터 가능)
]

def get_binance_klines(symbol: str, interval: str, start: dt.datetime, end: dt.datetime,
                       limit: int = 1000, pause: float = 0.25, max_retries: int = 4) -> list:
    """start~end 구간의 모든 klines를 수집하여 raw list로 반환."""
    # datetime -> ms epoch (naive는 UTC로 가정)
    if start.tzinfo is None:
        start = start.replace(tzinfo=dt.timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=dt.timezone.utc)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    all_data: list = []
    cursor = start_ms
    last_progress = None

    while True:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
            "startTime": cursor,
            "endTime": end_ms
        }

        # 재시도 with 백오프
        chunk = None
        for attempt in range(max_retries):
            try:
                r = session.get(BASE_URL, params=params, timeout=20)
                # 429/418 등 레이트 제한 → 잠시 대기 후 재시도
                if r.status_code == 429:
                    time.sleep(1.0 + attempt)
                    continue
                r.raise_for_status()
                chunk = r.json()
                break
            except requests.RequestException:
                time.sleep(pause * (attempt + 1))
        if chunk is None:  # 끝내 실패
            print(f"[WARN] {symbol}: request failed at cursor={cursor}, skip this window.")
            break

        if not chunk:
            # 더 이상 받을 데이터 없음
            break

        all_data.extend(chunk)

        last_close = chunk[-1][6]  # closeTime(ms)
        if last_progress == last_close or last_close >= end_ms:
            break
        last_progress = last_close
        cursor = last_close + 1

        time.sleep(pause)

    return all_data


def save_symbol_csv(symbol: str, interval: str,
                    start: dt.datetime, end: dt.datetime) -> bool:
    """심볼 하나를 내려받아 CSV로 저장. 성공 여부 반환."""
    try:
        raw = get_binance_klines(symbol, interval, start, end)
    except Exception as e:
        print(f"[ERROR] {symbol}: fetch failed → {e}")
        return False

    if not raw:
        print(f"[INFO] {symbol}: no data in given range. skipped.")
        return False

    cols = ["openTime","open","high","low","close","volume",
            "closeTime","quoteAssetVolume","numberOfTrades",
            "takerBuyBase","takerBuyQuote","ignore"]
    df = pd.DataFrame(raw, columns=cols)

    # 시간 변환 (UTC → Asia/Seoul), tz 제거
    df["openTime"]  = pd.to_datetime(df["openTime"], unit="ms", utc=True)\
                        .dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    df["closeTime"] = pd.to_datetime(df["closeTime"], unit="ms", utc=True)\
                        .dt.tz_convert("Asia/Seoul").dt.tz_localize(None)

    # 숫자형 캐스팅
    num_cols = ["open","high","low","close","volume",
                "quoteAssetVolume","takerBuyBase","takerBuyQuote"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["numberOfTrades"] = pd.to_numeric(df["numberOfTrades"], errors="coerce").astype("Int64")

    # 중복 제거 & 시간 정렬
    df = df.drop_duplicates(subset=["openTime"]).sort_values("openTime").reset_index(drop=True)

    out_name = f"{symbol}_{interval}_{start.date()}_{end.date()}.csv"
    df.to_csv(out_name, index=False, encoding="utf-8-sig")
    print(f"[OK] saved {out_name} (rows={len(df)})")
    return True


if __name__ == "__main__":
    # 🔧 설정
    symbols: List[str] = DEFAULT_SYMBOLS[:]  # 필요 시 여기서 교체/추가
    interval = "1h"
    start = dt.datetime(2017, 1, 1)
    end   = dt.datetime(2024, 12, 31, 23, 59, 59)
    # start = dt.datetime(2025, 1, 1)
    # end   = dt.datetime(2025, 12, 31, 23, 59, 59)

    # BTC/ETH도 같이 받고 싶으면:
    # symbols = ["BTCUSDT","ETHUSDT"] + DEFAULT_SYMBOLS

    ok, fail = 0, 0
    for sym in symbols:
        if save_symbol_csv(sym, interval, start, end):
            ok += 1
        else:
            fail += 1
    print(f"\nDONE. success={ok}, skipped/failed={fail}, total={len(symbols)}")
