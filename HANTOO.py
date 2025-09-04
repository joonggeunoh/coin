import os, json, time, asyncio, signal
import requests
import websockets
from dotenv import load_dotenv

load_dotenv()

APP_KEY     = os.getenv("KIS_APP_KEY")
APP_SECRET  = os.getenv("KIS_APP_SECRET")
ENV         = os.getenv("KIS_ENV", "real").lower()
EXCD3       = os.getenv("KIS_EXCD", "AMS")     # EXCD 3자리 (NAS/NYS/AMS ...)
SYMBOL      = os.getenv("KIS_SYMBOL", "BITI")  # 티커

# REST / WS 엔드포인트
REST_BASE = "https://openapi.koreainvestment.com:9443"                     # 실전
WS_URL    = "ws://ops.koreainvestment.com:21000/tryitout/HDFSCNT0"         # 해외주식 실시간(지연) 체결가

# ---- 1) OAuth 접근토큰 (로그인) ----
def get_access_token() -> str:
    url = f"{REST_BASE}/oauth2/tokenP"
    payload = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }
    # 일부 문서는 헤더 생략 예시가 있으나 JSON 권장
    res = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(payload), timeout=10)
    res.raise_for_status()
    data = res.json()
    if "access_token" not in data:
        raise RuntimeError(f"Token error: {data}")
    return data["access_token"]

# ---- 2) 웹소켓 승인키 발급 ----
def get_approval_key() -> str:
    url = f"{REST_BASE}/oauth2/Approval"
    payload = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "secretkey": APP_SECRET
    }
    res = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(payload), timeout=10)
    res.raise_for_status()
    data = res.json()
    # 응답 키 필드명은 문서/예제에 따라 approval_key 또는 approval_key(동일) 사용
    key = data.get("approval_key") or data.get("approvalkey")
    if not key:
        raise RuntimeError(f"Approval key error: {data}")
    return key

# ---- 3) 해외 실시간 체결가(HDFSCNT0) 구독 ----
# tr_key 규칙: 'D' + EXCD(3자리) + SYMBOL   예: TSLA(나스닥)= 'DNASTSLA'
# Tistory 예시: 'DNASTQQQ' 로 구독 (현재가 index 11 파싱 예시)  → BITI(ARCA)는 'DAMS' + 'BITI' = 'DAMSBITI'
# 참고: 미국은 실시간 0분, 기타 시장 15~20분 지연이라는 예시가 있음(계정 실시간 사용 권한에 따름)
# (실시간 권한 미보유 시 자동으로 지연 데이터가 올 수 있음)
async def stream_overseas_trades(approval_key: str, tr_key: str):
    # KIS WebSocket 표준 메시지: header + body.input(tr_id, tr_key)
    header = {
        "approval_key": approval_key,
        "custtype": "P",          # 개인: 'P', 법인: 'B'
        "tr_type": "1",           # 1: 구독, 2: 해지
        "content-type": "utf-8"
    }
    body = {"input": {"tr_id": "HDFSCNT0", "tr_key": tr_key}}
    subscribe_msg = json.dumps({"header": header, "body": body}, ensure_ascii=False)

    async with websockets.connect(WS_URL, ping_interval=None, ping_timeout=None, close_timeout=5) as ws:
        await ws.send(subscribe_msg)
        print(f"[WS] Subscribed: tr_id=HDFSCNT0, tr_key={tr_key}")

        # 수신 루프
        while True:
            msg = await ws.recv()
            # KIS 실시간은 '0|'로 시작하는 데이터 프레임/그 외 제어 프레임이 섞여 옴
            # 형식: '0|<...>|<...>|<data_fields_caret_separated>'
            if isinstance(msg, bytes):
                msg = msg.decode("utf-8", errors="ignore")

            if not msg:
                continue

            if msg[0] == '0':
                parts = msg.split('|')
                if len(parts) >= 4:
                    payload = parts[3]
                    fields = payload.split('^')
                    # 커뮤니티 예시 기준 index 11이 현재가(호가단위/소수점 포함)로 알려짐
                    # (거래소/상품에 따라 필드 구성이 다를 수 있으므로 실제 필드맵은 KIS 문서 확인 권장)
                    last_price = None
                    try:
                        last_price = fields[11]
                    except Exception:
                        pass

                    ts = time.strftime('%Y-%m-%d %H:%M:%S')
                    if last_price is not None:
                        print(f"[{ts}] {SYMBOL} last={last_price} (raw={payload})")
                    else:
                        print(f"[{ts}] raw={payload}")
            else:
                # 심장박동/확인 프레임 등
                print(f"[WS CTRL] {msg}")

def build_tr_key(excd3: str, symbol: str) -> str:
    # KIS 해외 체결가용 키 포맷 (실사용 예시 기반)
    return f"D{excd3.upper()}{symbol.upper()}"

def main():
    # 1) OAuth 로그인 (토큰은 이 예제에선 보관만; 필요한 REST 호출 시 사용)
    # token = get_access_token()
    # print("[LOGIN] access_token OK")
    # print("token")
    # print(token)
    token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjNlM2YxMjc2LWI4M2UtNGJhYi05ZmViLWE3MDgzMjBmYTlhZCIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTc1NzA1NDE1MiwiaWF0IjoxNzU2OTY3NzUyLCJqdGkiOiJQU3NKTmJVR3RielhuYjRpWU5OZDlLZ0pxd3pSVjBPTmhWa3YifQ.dwyuvJN4_YHNVaCuqDxbwTS2E-k-d_zlWMppvFe1y5Ks8OC7yUxTyHR1uayNrMeznmlTrvEXEnfE13tn43mJ-Q"
    # 2) 웹소켓 승인키
    approval_key = get_approval_key()
    print("[LOGIN] approval_key OK")
    #
    # 3) tr_key 구성 (BITI @ NYSE Arca → AMS)
    tr_key = build_tr_key(EXCD3, SYMBOL)

    # 종료 핸들러
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, loop.stop)

    loop.run_until_complete(stream_overseas_trades(approval_key, tr_key))

if __name__ == "__main__":
    main()
