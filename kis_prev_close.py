# kis_prev_close.py
# 해외 ETF/주식의 전일종가(Previous Close) 조회 스크립트
# - .env의 KIS_SYMBOLS = "AMS:BITI,AMS:SBIT,AMS:SETH" 형식
# - 실전/모의 자동 분기
# - 응답 내 전일종가 키가 없으면 last/diff로 역산

import os
import json
import time
from typing import Dict, Any, List, Tuple, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

ENV        = os.getenv("KIS_ENV", "real").lower()
APPKEY     = os.getenv("KIS_APPKEY") or os.getenv("KIS_APP_KEY")
APPSECRET  = os.getenv("KIS_APPSECRET") or os.getenv("KIS_APP_SECRET")
SYMBOLS    = os.getenv("KIS_SYMBOLS", "AMS:BITI,AMS:SBIT,AMS:SETH")

if ENV == "real":
    BASE = "https://openapi.koreainvestment.com:9443"
    TOKEN_PATH = "/oauth2/token"
else:
    BASE = "https://openapivts.koreainvestment.com:29443"
    TOKEN_PATH = "/oauth2/tokenP"

PRICE_PATH = "/uapi/overseas-price/v1/quotations/price"
TR_ID_PRICE = "HHDFS00000300"  # 해외 현재가 조회 TR (REST)

def get_access_token(appkey: str, appsecret: str) -> str:
    url = BASE + TOKEN_PATH
    headers = {"Content-Type": "application/json; charset=UTF-8"}
    payload = {"grant_type": "client_credentials", "appkey": appkey, "appsecret": appsecret}
    r = requests.post(url, headers=headers, json=payload, timeout=10)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        print("[TOKEN ERR]", r.status_code, r.text)
        raise
    data = r.json()
    tok = data.get("access_token")
    if not tok:
        raise RuntimeError(f"Token error: {data}")
    return tok

def parse_symbols(raw: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for t in raw.split(","):
        t = t.strip()
        if not t:
            continue
        ex, sy = t.split(":")
        pairs.append((ex.strip().upper(), sy.strip().upper()))
    return pairs

def to_float(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    try:
        # 해외 응답은 소수점 포함 문자열일 수 있음
        return float(x.replace(",", ""))  # 혹시 천단위 콤마가 올 경우 대비
    except Exception:
        return None

def get_overseas_price(token: str, excd: str, symb: str) -> Dict[str, Any]:
    """
    해외 현재가 조회.
    반환은 KIS 응답 원본의 "output" dict (시장/상품별 키가 다를 수 있으므로 그대로 전달).
    """
    url = BASE + PRICE_PATH
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "authorization": f"Bearer {token}",
        "appkey": APPKEY,
        "appsecret": APPSECRET,
        "tr_id": TR_ID_PRICE,
    }
    params = {
        "AUTH": "",
        "EXCD": excd,    # NAS/NYS/AMS 등
        "SYMB": symb,    # 티커
    }
    r = requests.get(url, headers=headers, params=params, timeout=10)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        print(f"[PRICE ERR] {excd}:{symb}", r.status_code, r.text)
        raise
    return r.json().get("output", {}) or {}

def extract_prev_close(output: Dict[str, Any]) -> Dict[str, Any]:
    """
    전일종가 후보 키를 우선 탐색하고, 없으면 last/diff로 역산.
    또한 last/open/high/low/diff/rate 등 자주 쓰는 키도 같이 반환.
    """
    # 자주 보이는 키들(시장/버전에 따라 다를 수 있음) 후보
    key_candidates_prev = ["pclose", "prdy_clpr", "prev_close", "base", "yesterday_close"]
    key_candidates_last = ["last", "last_prc", "stck_prpr"]
    key_candidates_diff = ["diff", "prdy_vrss", "change"]
    key_candidates_rate = ["rate", "prdy_ctrt", "rate_pct"]
    key_candidates_open = ["open", "opnprc"]
    key_candidates_high = ["high", "hgprc"]
    key_candidates_low  = ["low",  "lwprc"]

    def pick(keys: List[str]) -> Optional[str]:
        for k in keys:
            if k in output and output[k] not in ("", None):
                return output[k]
        return None

    prev_raw = pick(key_candidates_prev)
    last_raw = pick(key_candidates_last)
    diff_raw = pick(key_candidates_diff)

    prev = to_float(prev_raw)
    last = to_float(last_raw)
    diff = to_float(diff_raw)

    # 역산: prev_close = last - diff
    if prev is None and (last is not None and diff is not None):
        prev = last - diff

    # 같이 담아둘 참고값들
    open_v = to_float(pick(key_candidates_open))
    high_v = to_float(pick(key_candidates_high))
    low_v  = to_float(pick(key_candidates_low))
    rate_v = to_float(pick(key_candidates_rate))

    # 현지 시각/일자 후보 (있으면 표시)
    tdate = output.get("tdate") or output.get("trd_dd")
    ttime = output.get("ttime") or output.get("trd_tm")

    return {
        "prev_close": prev,
        "last": last,
        "diff": diff,
        "rate": rate_v,
        "open": open_v,
        "high": high_v,
        "low": low_v,
        "tdate": tdate,#
        "ttime": ttime,
        "raw": output,  # 필요 시 디버깅용
    }

def main():
    assert APPKEY and APPSECRET, "KIS_APPKEY / KIS_APPSECRET 환경변수를 설정하세요 (.env)"
    pairs = parse_symbols(SYMBOLS)

    token = get_access_token(APPKEY, APPSECRET)

    rows = []
    for ex, sy in pairs:
        out = get_overseas_price(token, ex, sy)
        info = extract_prev_close(out)
        rows.append((ex, sy, info))

    # 출력
    print("-" * 80)
    print(f"ENV={ENV.upper()}  BASE={BASE}")
    for ex, sy, info in rows:
        pc = info["prev_close"]
        last = info["last"]
        diff = info["diff"]
        rate = info["rate"]
        tdate = info["tdate"]
        ttime = info["ttime"]
        print(f"{ex}:{sy:5s}  prev_close={pc}  last={last}  chg={diff} ({rate}%)  "
              f"OHLC={info['open']}/{info['high']}/{info['low']}  {tdate} {ttime}")

if __name__ == "__main__":
    main()
