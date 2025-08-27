# btcusdt_to_csv.py
# -*- coding: utf-8 -*-
import time, datetime as dt, requests, pandas as pd

BASE_URL = "https://api.binance.com/api/v3/klines"

def get_binance_klines(symbol, interval, start, end, limit=1000, pause=0.25):
    if start.tzinfo is None:
        start = start.replace(tzinfo=dt.timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=dt.timezone.utc)
    start_ms = int(start.timestamp() * 1000)
    end_ms   = int(end.timestamp() * 1000)

    all_data = []
    cursor = start_ms
    while True:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
            "startTime": cursor,
            "endTime": end_ms
        }
        r = requests.get(BASE_URL, params=params, timeout=20)
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        all_data.extend(chunk)
        last_close = chunk[-1][6]
        if last_close >= end_ms:
            break
        cursor = last_close + 1
        time.sleep(pause)
    return all_data

if __name__ == "__main__":
    symbol   = "BTCUSDT"   # ✅ 비트코인/테더 마켓
    interval = "1h"        # 1시간 봉 (원하면 "1d", "5m" 등 변경 가능)
    start    = dt.datetime(2017, 1, 1)
    end      = dt.datetime(2024, 12, 31, 23, 59, 59)

    raw = get_binance_klines(symbol, interval, start, end)

    cols = ["openTime","open","high","low","close","volume",
            "closeTime","quoteAssetVolume","numberOfTrades",
            "takerBuyBase","takerBuyQuote","ignore"]
    df = pd.DataFrame(raw, columns=cols)

    # 시간 변환 (UTC → Asia/Seoul, tz 제거)
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

    # CSV 저장
    out_name = f"{symbol}_{interval}_2017_2024.csv"
    df.to_csv(out_name, index=False, encoding="utf-8-sig")
    print(f"저장 완료: {out_name} (rows={len(df)})")
