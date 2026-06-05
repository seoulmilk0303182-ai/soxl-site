"""
시발 SOXL 왜 올라요? — 백엔드 v5
- 백그라운드 5분 자동갱신 (타이머 버그 수정)
- Finnhub 실시간 주가 (프리/애프터 포함)
- stockanalysis HTML 파싱으로 SOXX 비중 (실패 시 fallback)
"""

import time
import threading
import os
import re
from flask import Flask, jsonify, send_from_directory
import requests

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")

app = Flask(__name__, static_folder="static")

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

NAME_MAP = {h["ticker"]: h["name"] for h in FALLBACK_HOLDINGS}

CACHE      = {"data": None}
CACHE_LOCK = threading.Lock()
CACHE_TTL  = 300   # 주가 5분
WEIGHT_TTL = 86400 # 비중 하루

_weight_cache = {"holdings": None, "updated_at": 0}


# ── 비중 스크래핑 ────────────────────────────────────────────────
def fetch_weights_stockanalysis():
    url = "https://stockanalysis.com/etf/soxx/holdings/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    html = r.text

    tbody = re.search(r'<tbody[^>]*>(.*?)</tbody>', html, re.S)
    if not tbody:
        raise ValueError("tbody not found")

    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody.group(1), re.S)
    holdings = []
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)
        if len(cells) < 4:
            continue
        ticker = re.sub(r'<[^>]+>', '', cells[1]).strip().upper()
        if not ticker or ticker == 'SYMBOL':
            continue
        weight_raw = re.sub(r'<[^>]+>', '', cells[3]).strip().replace('%','').replace(',','')
        try:
            weight = float(weight_raw)
        except ValueError:
            continue
        if weight <= 0:
            continue
        holdings.append({
            "ticker": ticker,
            "name":   NAME_MAP.get(ticker, ticker),
            "weight": round(weight, 4),
        })

    if len(holdings) < 10:
        raise ValueError(f"종목 수 부족: {len(holdings)}")

    holdings.sort(key=lambda x: x["weight"], reverse=True)
    print(f"[{time.strftime('%H:%M:%S')}] 비중 {len(holdings)}개 갱신 (stockanalysis)")
    return holdings


def get_holdings():
    now = time.time()
    if _weight_cache["holdings"] is None or now - _weight_cache["updated_at"] > WEIGHT_TTL:
        try:
            _weight_cache["holdings"]   = fetch_weights_stockanalysis()
            _weight_cache["updated_at"] = now
        except Exception as e:
            print(f"[WARN] 비중 실패: {e} → fallback")
            if _weight_cache["holdings"] is None:
                _weight_cache["holdings"]   = FALLBACK_HOLDINGS
                _weight_cache["updated_at"] = now
    return _weight_cache["holdings"]


# ── 주가 조회 ────────────────────────────────────────────────────
def fetch_single(ticker):
    try:
        url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
        d   = requests.get(url, timeout=10).json()
        cur  = d.get("c", 0)
        prev = d.get("pc", cur) or cur
        if not cur:
            return {"price": 0, "prevClose": 0, "changePercent": 0}
        chg = ((cur - prev) / prev * 100) if prev else 0
        return {"price": round(cur, 2), "prevClose": round(prev, 2), "changePercent": round(chg, 4)}
    except Exception as e:
        print(f"  [{ticker}] 오류: {e}")
        return {"price": 0, "prevClose": 0, "changePercent": 0}


# ── 캐시 갱신 ────────────────────────────────────────────────────
def refresh_cache():
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] 주가 갱신 시작...")
    holdings    = get_holdings()
    all_tickers = ["SOXX", "SOXL"] + [h["ticker"] for h in holdings]

    results = {}
    def _fetch(t):
        results[t] = fetch_single(t)

    threads = [threading.Thread(target=_fetch, args=(t,)) for t in all_tickers]
    for th in threads: th.start()
    for th in threads: th.join(timeout=20)

    def etf(t):
        d = results.get(t, {})
        return {"price": d.get("price", 0), "changePercent": d.get("changePercent", 0)}

    holding_data = []
    for h in holdings:
        q   = results.get(h["ticker"], {})
        chg = q.get("changePercent", 0)
        holding_data.append({
            **h,
            "price":         q.get("price", 0),
            "prevClose":     q.get("prevClose", 0),
            "changePercent": chg,
            "contribution":  round(chg * h["weight"] / 100, 5),
        })
    holding_data.sort(key=lambda x: abs(x["contribution"]), reverse=True)

    soxx = etf("SOXX")
    soxl = etf("SOXL")

    # ★ next_refresh_at을 서버가 직접 계산해서 내려줌 → 프론트 타이머 오차 없음
    now = int(time.time())
    data = {
        "soxx":              soxx,
        "soxl":              soxl,
        "holdings":          holding_data,
        "updated_at":        now,
        "next_refresh_at":   now + CACHE_TTL,        # ← 핵심 추가
        "weight_updated_at": int(_weight_cache["updated_at"]),
    }
    with CACHE_LOCK:
        CACHE["data"] = data

    print(
        f"[{time.strftime('%H:%M:%S')}] 갱신 완료 ✓  "
        f"SOXX {soxx['changePercent']:+.2f}%  "
        f"SOXL {soxl['changePercent']:+.2f}%  "
        f"({len(holding_data)}종목, {time.time()-t0:.1f}s)"
    )


# ── 백그라운드 워커 ── 시작 직후 1회 갱신, 이후 5분 간격 ────────
def background_worker():
    while True:
        time.sleep(CACHE_TTL)   # ★ 먼저 대기 (최초 갱신은 메인에서 이미 함)
        try:
            refresh_cache()
        except Exception as e:
            print(f"[ERROR] {e}")

refresh_cache()  # 서버 시작 시 1회 즉시 실행
threading.Thread(target=background_worker, daemon=True).start()


# ── Flask 라우트 ─────────────────────────────────────────────────
@app.route("/api/data")
def api_data():
    with CACHE_LOCK:
        data = CACHE["data"]
    if data is None:
        return jsonify({"error": "데이터 준비 중. 잠시 후 새로고침하세요."}), 503
    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-cache"
    return resp

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
