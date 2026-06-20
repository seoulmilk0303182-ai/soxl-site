"""
시발 SOXL 왜 올라요? — 백엔드 v7
- 토스증권 공식 Open API (OpenAPI 3.1 스펙 기준)
- GET /api/v1/prices?symbols=... 로 30개 종목 한 번에 조회 (최대 200개 지원)
- GET /api/v1/candles 로 전일 종가 조회 (등락률 계산용)
- 5분 주가 갱신

★★★ 중요: API 키 설정 방법 ★★★
절대 이 파일에 키를 직접 적지 마세요.

[Windows CMD]
set TOSS_CLIENT_ID=발급받은_ID
set TOSS_CLIENT_SECRET=발급받은_SECRET
python main.py

[Windows PowerShell]
$env:TOSS_CLIENT_ID="발급받은_ID"
$env:TOSS_CLIENT_SECRET="발급받은_SECRET"
python main.py

⚠️ 키를 어딘가에 노출한 적이 있다면 토스증권 앱에서 즉시 재발급하세요.
"""

import time
import threading
import os
from flask import Flask, jsonify, send_from_directory
import requests

TOSS_CLIENT_ID     = os.environ.get("TOSS_CLIENT_ID", "")
TOSS_CLIENT_SECRET = os.environ.get("TOSS_CLIENT_SECRET", "")
TOSS_BASE_URL       = "https://openapi.tossinvest.com"

if not TOSS_CLIENT_ID or not TOSS_CLIENT_SECRET:
    print("=" * 60)
    print("[WARN] TOSS_CLIENT_ID / TOSS_CLIENT_SECRET 환경변수가 비어있습니다.")
    print("=" * 60)

app = Flask(__name__, static_folder="static")

# ── SOXX 구성종목 30개 (비중은 추정값) ───────────────────────────
FALLBACK_HOLDINGS = [
    {"ticker": "MU",    "name": "마이크론",             "weight": 11.04},
    {"ticker": "AMD",   "name": "AMD",                  "weight": 9.51},
    {"ticker": "AVGO",  "name": "브로드컴",             "weight": 6.58},
    {"ticker": "INTC",  "name": "인텔",                 "weight": 6.53},
    {"ticker": "MRVL",  "name": "마벨테크",             "weight": 6.18},
    {"ticker": "NVDA",  "name": "엔비디아",             "weight": 5.96},
    {"ticker": "AMAT",  "name": "어플라이드머티리얼즈", "weight": 4.44},
    {"ticker": "QCOM",  "name": "퀄컴",                 "weight": 4.21},
    {"ticker": "TXN",   "name": "텍사스인스트루먼트",   "weight": 3.67},
    {"ticker": "NXPI",  "name": "NXP세미컨덕터",        "weight": 3.58},
    {"ticker": "MPWR",  "name": "모노리식파워",         "weight": 3.52},
    {"ticker": "LRCX",  "name": "램리서치",             "weight": 3.35},
    {"ticker": "KLAC",  "name": "KLA",                  "weight": 3.12},
    {"ticker": "TER",   "name": "테라다인",             "weight": 2.95},
    {"ticker": "ADI",   "name": "아날로그디바이스",     "weight": 2.90},
    {"ticker": "MCHP",  "name": "마이크로칩테크",       "weight": 2.60},
    {"ticker": "TSM",   "name": "TSMC",                 "weight": 2.56},
    {"ticker": "ASML",  "name": "ASML",                 "weight": 2.50},
    {"ticker": "ON",    "name": "온세미컨덕터",         "weight": 2.47},
    {"ticker": "ALAB",  "name": "Astera Labs",           "weight": 2.42},
    {"ticker": "CRDO",  "name": "Credo Technology",      "weight": 1.83},
    {"ticker": "MTSI",  "name": "MACOM Technology",      "weight": 1.34},
    {"ticker": "ENTG",  "name": "앤테그리스",           "weight": 1.06},
    {"ticker": "ASX",   "name": "ASE Technology",        "weight": 1.06},
    {"ticker": "UMC",   "name": "UMC",                  "weight": 0.83},
    {"ticker": "SWKS",  "name": "스카이웍스",           "weight": 0.70},
    {"ticker": "QRVO",  "name": "Qorvo",                "weight": 0.60},
    {"ticker": "STM",   "name": "ST마이크로",           "weight": 0.55},
    {"ticker": "WOLF",  "name": "울프스피드",           "weight": 0.40},
    {"ticker": "AMKR",  "name": "Amkor",                "weight": 0.35},
]

CACHE      = {"data": None}
CACHE_LOCK = threading.Lock()
CACHE_TTL  = 300

# ── OAuth 토큰 캐시 ───────────────────────────────────────────────
_token_cache = {"access_token": None, "expires_at": 0}
_token_lock  = threading.Lock()

def get_access_token():
    """
    POST /oauth2/token
    공식 스펙: body(form-urlencoded)에 grant_type, client_id, client_secret 전달.
    """
    with _token_lock:
        now = time.time()
        if _token_cache["access_token"] and now < _token_cache["expires_at"] - 300:
            return _token_cache["access_token"]

        if not TOSS_CLIENT_ID or not TOSS_CLIENT_SECRET:
            raise RuntimeError("TOSS_CLIENT_ID/SECRET 환경변수가 설정되지 않았습니다.")

        resp = requests.post(
            f"{TOSS_BASE_URL}/oauth2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type":    "client_credentials",
                "client_id":     TOSS_CLIENT_ID,
                "client_secret": TOSS_CLIENT_SECRET,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"토큰 발급 실패 ({resp.status_code}): {resp.text[:200]}")

        d = resp.json()
        _token_cache["access_token"] = d["access_token"]
        _token_cache["expires_at"]   = now + d.get("expires_in", 86400)
        print(f"[{time.strftime('%H:%M:%S')}] 토스증권 토큰 발급 완료 (유효 {d.get('expires_in',86400)}초)")
        return _token_cache["access_token"]


def auth_headers():
    return {"Authorization": f"Bearer {get_access_token()}"}


# ── 현재가 일괄 조회 (최대 200개, 콤마 구분) ─────────────────────
def fetch_all_prices(tickers):
    """
    GET /api/v1/prices?symbols=NVDA,AMD,...
    응답: { result: [ {symbol, timestamp, lastPrice, currency}, ... ] }
    30개 종목을 단 1번의 호출로 가져옵니다.
    """
    symbols_str = ",".join(tickers)
    resp = requests.get(
        f"{TOSS_BASE_URL}/api/v1/prices",
        params={"symbols": symbols_str},
        headers=auth_headers(),
        timeout=15,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"현재가 조회 실패 ({resp.status_code}): {resp.text[:200]}")

    d = resp.json()
    result = d.get("result", [])
    return {item["symbol"]: float(item["lastPrice"]) for item in result if item.get("lastPrice")}


# ── 전일 종가 조회 (일봉 캔들, 종목당 1콜) ───────────────────────
def fetch_prev_close(ticker):
    """
    GET /api/v1/candles?symbol=AAPL&interval=1d&count=2
    가장 최근 2개의 일봉 중 이전 봉의 종가를 전일 종가로 사용.
    """
    try:
        resp = requests.get(
            f"{TOSS_BASE_URL}/api/v1/candles",
            params={"symbol": ticker, "interval": "1d", "count": 2},
            headers=auth_headers(),
            timeout=10,
        )
        if resp.status_code != 200:
            return 0
        candles = resp.json().get("result", {}).get("candles", [])
        if len(candles) >= 2:
            return float(candles[1]["closePrice"])
        elif len(candles) == 1:
            return float(candles[0]["closePrice"])
        return 0
    except Exception as e:
        print(f"  [{ticker}] 전일종가 조회 오류: {e}")
        return 0


# ── 미국 장 운영 상태 조회 ────────────────────────────────────────
def fetch_us_market_status():
    """
    GET /api/v1/market-calendar/US
    현재 시각이 어느 세션(day/pre/regular/after/closed)인지 판단.
    휴장이면 4세션 모두 null.
    """
    try:
        resp = requests.get(
            f"{TOSS_BASE_URL}/api/v1/market-calendar/US",
            headers=auth_headers(),
            timeout=10,
        )
        if resp.status_code != 200:
            return "unknown"
        today = resp.json().get("result", {}).get("today", {})

        import datetime
        now_iso = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))

        def in_session(session):
            if not session:
                return False
            start = datetime.datetime.fromisoformat(session["startTime"])
            end   = datetime.datetime.fromisoformat(session["endTime"])
            return start <= now_iso <= end

        if in_session(today.get("regularMarket")):
            return "regular"
        if in_session(today.get("preMarket")):
            return "pre"
        if in_session(today.get("afterMarket")):
            return "after"
        if in_session(today.get("dayMarket")):
            return "day"
        return "closed"
    except Exception as e:
        print(f"[WARN] 장 운영 상태 조회 실패: {e}")
        return "unknown"


# ── 캐시 갱신 ────────────────────────────────────────────────────
def refresh_cache():
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] 주가 갱신 시작...")

    holdings    = FALLBACK_HOLDINGS
    all_tickers = ["SOXX", "SOXL"] + [h["ticker"] for h in holdings]

    market_status = fetch_us_market_status()

    try:
        prices = fetch_all_prices(all_tickers)
    except Exception as e:
        print(f"[ERROR] 현재가 일괄 조회 실패: {e}")
        prices = {}

    prev_closes = {}
    def _fetch_prev(t):
        prev_closes[t] = fetch_prev_close(t)

    threads = [threading.Thread(target=_fetch_prev, args=(t,)) for t in all_tickers]
    for th in threads: th.start()
    for th in threads: th.join(timeout=25)

    def quote(t):
        price = prices.get(t, 0)
        prev  = prev_closes.get(t, 0) or price
        # 장 마감(closed) 상태면 현재가도 전일 종가 기준으로 표시
        # (lastPrice 가 휴장 중 갱신되지 않는 거래소 특성을 명시적으로 보정)
        if market_status == "closed" and prev:
            price = prev
        chg = ((price - prev) / prev * 100) if prev else 0
        return {"price": round(price, 2), "prevClose": round(prev, 2), "changePercent": round(chg, 4)}

    def etf(t):
        q = quote(t)
        return {"price": q["price"], "changePercent": q["changePercent"]}

    holding_data = []
    for h in holdings:
        q   = quote(h["ticker"])
        chg = q["changePercent"]
        holding_data.append({
            **h,
            "price":         q["price"],
            "prevClose":     q["prevClose"],
            "changePercent": chg,
            "contribution":  round(chg * h["weight"] / 100, 5),
        })
    holding_data.sort(key=lambda x: abs(x["contribution"]), reverse=True)

    soxx = etf("SOXX")
    soxl = etf("SOXL")
    now  = int(time.time())

    data = {
        "soxx":              soxx,
        "soxl":              soxl,
        "holdings":          holding_data,
        "updated_at":        now,
        "next_refresh_at":   now + CACHE_TTL,
        "weight_updated_at": now,
        "market_status":     market_status,
    }
    with CACHE_LOCK:
        CACHE["data"] = data

    print(
        f"[{time.strftime('%H:%M:%S')}] 갱신 완료 ✓  "
        f"[{market_status}]  "
        f"SOXX {soxx['changePercent']:+.2f}%  "
        f"SOXL {soxl['changePercent']:+.2f}%  "
        f"({len(holding_data)}종목, {time.time()-t0:.1f}s)"
    )


def background_worker():
    """
    5분(CACHE_TTL)마다 갱신하되, time.sleep(300) 한 방에 의존하지 않고
    10초 간격으로 깨어나 '갱신할 때가 됐는지' 체크합니다.
    → 슬립/절전 등으로 인한 누락 없이 항상 정확한 시점에 갱신됩니다.
    """
    while True:
        time.sleep(10)
        with CACHE_LOCK:
            data = CACHE["data"]
        if data is None:
            continue
        if time.time() >= data.get("next_refresh_at", 0):
            try:
                refresh_cache()
            except Exception as e:
                print(f"[ERROR] {e}")

refresh_cache()
threading.Thread(target=background_worker, daemon=True).start()


@app.route("/api/data")
def api_data():
    with CACHE_LOCK:
        data = CACHE["data"]
    if data is None:
        return jsonify({"error": "데이터 준비 중. 잠시 후 새로고침하세요."}), 503
    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-cache"
    return resp

@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    try:
        refresh_cache()
        with CACHE_LOCK:
            data = CACHE["data"]
        resp = jsonify(data)
        resp.headers["Cache-Control"] = "no-cache"
        return resp
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
