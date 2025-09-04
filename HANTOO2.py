# HANTOO2.py
# - 실전(real) 환경 기준
# - .env 사용(KIS_APP_KEY, KIS_APP_SECRET, KIS_SYMBOLS 또는 KIS_EXCD/KIS_SYMBOL)
# - KIS WebSocket tryitout 채널(ws://) 구독 + PINGPONG 에코 + 자동 재접속
# - 구버전 websockets 호환: extra_headers / open_timeout 등 제거

import os
import json
import time
import asyncio
import requests
import websockets
from typing import List, Tuple, Optional
from dotenv import load_dotenv

load_dotenv()

APP_KEY     = os.getenv("KIS_APP_KEY")
APP_SECRET  = os.getenv("KIS_APP_SECRET")
ENV         = os.getenv("KIS_ENV", "real").lower()

# 다중 기호 우선, 없으면 단일
SYMBOLS_RAW = os.getenv("KIS_SYMBOLS")   # "AMS:BITI,AMS:SBIT,AMS:SETH"
EXCD3       = os.getenv("KIS_EXCD", "AMS")
SYMBOL      = os.getenv("KIS_SYMBOL", "BITI")

# REST (실전)
REST_BASE      = "https://openapi.koreainvestment.com:9443"
TOKEN_PATH     = "/oauth2/token"       # 모의는 /oauth2/tokenP
APPROVAL_PATH  = "/oauth2/Approval"

# WebSocket tryitout (샘플/지연 채널): 평문 WS 사용
WS_URL   = "ws://ops.koreainvestment.com:21000/tryitout/HDFSCNT0"
WS_TR_ID = "HDFSCNT0"  # 해외 체결가

# -------------------- 유틸 --------------------
def parse_symbols(raw: Optional[str]) -> List[Tuple[str, str]]:
    if raw:
        out: List[Tuple[str, str]] = []
        for t in raw.split(","):
            t = t.strip()
            if not t:
                continue
            ex, sy = t.split(":")
            out.append((ex.strip().upper(), sy.strip().upper()))
        return out
    return [(EXCD3.upper(), SYMBOL.upper())]

def build_tr_key(excd3: str, symbol: str) -> str:
    # 해외 체결가 구독 키: D + EXCD(3) + SYMBOL
    return f"D{excd3}{symbol}"

# -------------------- REST: 토큰/승인키 --------------------
def get_access_token() -> str:
    url = f"{REST_BASE}{TOKEN_PATH}"
    payload = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    r = requests.post(url, headers={"Content-Type": "application/json; charset=UTF-8"},
                      json=payload, timeout=10)
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

def get_approval_key() -> str:
    url = f"{REST_BASE}{APPROVAL_PATH}"
    payload = {"grant_type": "client_credentials", "appkey": APP_KEY, "secretkey": APP_SECRET}
    r = requests.post(url, headers={"Content-Type": "application/json; charset=UTF-8"},
                      json=payload, timeout=10)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        print("[APPROVAL ERR]", r.status_code, r.text)
        raise
    data = r.json()
    key = data.get("approval_key") or data.get("approvalkey")
    if not key:
        raise RuntimeError(f"Approval key error: {data}")
    return key

# -------------------- WS 처리 --------------------
def parse_tick_payload(payload: str):
    """
    payload: '^' 구분 필드 문자열.
    실제 필드맵은 KIS 문서 참고. 예시로 index 11을 현재가로 출력.
    """
    f = payload.split("^")
    last = f[11] if len(f) > 11 else None
    return last, payload

async def subscribe_one(ws, approval_key: str, tr_id: str, tr_key: str):
    header = {
        "approval_key": approval_key,
        "custtype": "P",     # 개인 P / 법인 B
        "tr_type": "1",      # 1:구독, 2:해지
        "content-type": "utf-8"
    }
    body = {"input": {"tr_id": tr_id, "tr_key": tr_key}}
    msg = json.dumps({"header": header, "body": body}, ensure_ascii=False)
    await ws.send(msg)
    print(f"[WS] Subscribed -> {tr_id} {tr_key}")

async def ws_loop(approval_key: str, pairs: List[Tuple[str, str]]):
    """
    단일 커넥션에 여러 종목 구독. 끊기면 자동 재접속.
    구버전 websockets 호환을 위해 extra_headers / open_timeout 제거.
    """
    retry = 3
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,   # 서버 PINGPONG 사용
                ping_timeout=None,
                max_size=None,        # 구버전 호환
            ) as ws:
                # 구독
                for ex, sy in pairs:
                    await subscribe_one(ws, approval_key, WS_TR_ID, build_tr_key(ex, sy))

                # 수신 루프
                while True:
                    msg = await ws.recv()
                    if isinstance(msg, bytes):
                        msg = msg.decode("utf-8", errors="ignore")
                    if not msg:
                        continue

                    if msg[0] == "0":
                        # 데이터 프레임: '0|...|...|payload(^-separated)'
                        parts = msg.split("|")
                        if len(parts) >= 4:
                            payload = parts[3]
                            last, raw = parse_tick_payload(payload)
                            ts = time.strftime("%Y-%m-%d %H:%M:%S")
                            print(f"[{ts}] last={last} raw={raw[:80]}...")
                        continue

                    # 제어 프레임(JSON): PINGPONG / SUBSCRIBE SUCCESS 등
                    try:
                        ctrl = json.loads(msg)
                        tr_id = ctrl.get("header", {}).get("tr_id")
                        if tr_id == "PINGPONG":
                            await ws.send(json.dumps({"header": {"tr_id": "PINGPONG"}}))
                        else:
                            print(f"[WS CTRL] {msg}")
                    except Exception:
                        # JSON 아니면 그대로 출력
                        print(f"[WS CTRL] {msg}")

        except (websockets.exceptions.ConnectionClosedError, asyncio.TimeoutError) as e:
            print(f"[WS] reconnect in {retry}s... ({e})")
            await asyncio.sleep(retry)
        except Exception as e:
            print(f"[WS] error: {e}; reconnect in {retry}s")
            await asyncio.sleep(retry)

async def main_async():
    # (선택) REST 토큰 필요 시 활성화
    # token = get_access_token()
    approval_key = get_approval_key()
    print("[LOGIN] approval_key OK")

    pairs = parse_symbols(SYMBOLS_RAW)
    print("[TARGETS]", ", ".join(f"{ex}:{sy}" for ex, sy in pairs))
    await ws_loop(approval_key, pairs)

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("bye")

if __name__ == "__main__":
    main()
