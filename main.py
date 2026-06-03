print("THIS IS MY MAIN")

"""
시발 SOXL 왜 올라요? — 백엔드 v2
- 프리장/애프터장 포함
- iShares에서 실제 비중 자동 스크래핑 (실패 시 하드코딩 fallback)
- 5분 자동 갱신
"""

import time
import threading
import csv
import io
from flask import Flask, jsonify, send_from_directory
import yfinance as yf
import urllib.request

app = Flask(__name__, static_folder="static")

# ── 하드코딩 fallback 비중 (iShares 스크래핑 실패 시 사용) ──────
FALLBACK_HOLDINGS = [
    {"ticker": "NVDA",  "name": "엔비디아",            "weight": 8.50},
    {"ticker": "AVGO",  "name": "브로드컴",            "weight": 8.30},
    {"ticker": "AMD",   "name": "AMD",                  "weight": 5.80},
    {"ticker": "QCOM",  "name": "퀄컴",                 "weight": 5.70},
    {"ticker": "TXN",   "name": "텍사스인스트루먼트",   "weight": 5.50},
    {"ticker": "INTC",  "name": "인텔",                 "weight": 5.30},
    {"ticker": "ADI",   "name": "아날로그디바이스",     "weight": 5.10},
    {"ticker": "MRVL",  "name": "마벨테크",             "weight": 4.90},
    {"ticker": "MU",    "name": "마이크론",             "weight": 4.80},
    {"ticker": "KLAC",  "name": "KLA",                  "weight": 4.60},
    {"ticker": "LRCX",  "name": "램리서치",             "weight": 4.40},
    {"ticker": "AMAT",  "name": "어플라이드머티리얼즈", "weight": 4.30},
    {"ticker": "NXPI",  "name": "NXP세미컨덕터",        "weight": 3.80},
    {"ticker": "MCHP",  "name": "마이크로칩테크",       "weight": 3.50},
    {"ticker": "ON",    "name": "온세미컨덕터",         "weight": 3.20},
    {"ticker": "MPWR",  "name": "모노리식파워",         "weight": 3.00},
    {"ticker": "SWKS",  "name": "스카이웍스",           "weight": 2.80},
    {"ticker": "STM",   "name": "ST마이크로",           "weight": 2.50},
    {"ticker": "QRVO",  "name": "Qorvo",                "weight": 2.30},
    {"ticker": "ENTG",  "name": "앤테그리스",           "weight": 2.10},
    {"ticker": "OLED",  "name": "유니버설디스플레이",   "weight": 1.90},
    {"ticker": "WOLF",  "name": "울프스피드",           "weight": 1.70},
    {"ticker": "FORM",  "name": "FormFactor",            "weight": 1.50},
    {"ticker": "COHU",  "name": "Cohu",                 "weight": 1.30},
    {"ticker": "MKSI",  "name": "MKS인스트루먼트",      "weight": 1.20},
    {"ticker": "ACLS",  "name": "Axcelis",               "weight": 1.10},
    {"ticker": "POWI",  "name": "Power Integrations",    "weight": 1.00},
    {"ticker": "DIOD",  "name": "Diodes Inc",            "weight": 0.90},
    {"ticker": "AMKR",  "name": "Amkor",                 "weight": 0.80},
    {"ticker": "ICHR",  "name": "Ichor Holdings",        "weight": 0.70},
]

NAME_MAP = {h["ticker"]: h["name"] for h in FALLBACK_HOLDINGS}

CACHE = {"data": None}
CACHE_LOCK = threading.Lock()
CACHE_TTL  = 300   # 5분
WEIGHT_TTL = 86400 # 비중은 하루 1회 갱신

_weight_cache = {"holdings": None, "updated_at": 0}

# ── iShares 비중 스크래핑 ────────────────────────────────────────
def fetch_ishares_weights():
    """
    iShares SOXX CSV 다운로드로 실제 비중 가져오기.
    실패 시 fallback 반환.
    """
    url = (
        "https://www.ishares.com/us/products/239705/SOXX/"
        "1467271812596.ajax?fileType=csv&fileName=SOXX_holdings"
        "&dataType=fund"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
        ),
        "Referer": "https://www.ishares.com/",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode("utf-8", errors="ignore")

        # CSV 앞부분 메타 행 건너뛰기 (빈 줄 이후가 실제 데이터)
        lines = raw.splitlines()
        start = 0
        for i, line in enumerate(lines):
            if line.startswith("Ticker,") or line.startswith('"Ticker"'):
                start = i
                break

        reader = csv.DictReader(io.StringIO("\n".join(lines[start:])))
        holdings = []
        for row in reader:
            ticker = row.get("Ticker", "").strip().strip('"')
            weight_str = row.get("Weight (%)", row.get("Weightings", "0")).strip().strip('"')
            if not ticker or ticker == "-":
                continue
            try:
                weight = float(weight_str)
            except ValueError:
                continue
            if weight <= 0:
                continue
            holdings.append({
                "ticker": ticker,
                "name":   NAME_MAP.get(ticker, ticker),
                "weight": round(weight, 4),
            })

        if len(holdings) >= 10:
            print(f"[{time.strftime('%H:%M:%S')}] iShares 비중 {len(holdings)}개 갱신 완료")
            return sorted(holdings, key=lambda x: x["weight"], reverse=True)
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] iShares 스크래핑 실패 ({e}), fallback 사용")

    return FALLBACK_HOLDINGS

def get_holdings():
    now = time.time()
    if (
        _weight_cache["holdings"] is None
        or now - _weight_cache["updated_at"] > WEIGHT_TTL
    ):
        _weight_cache["holdings"]   = fetch_ishares_weights()
        _weight_cache["updated_at"] = now
    return _weight_cache["holdings"]

# ── 실시간 주가 (프리/애프터 포함) ──────────────────────────────
def fetch_single(ticker):
    """
    yfinance Ticker로 단건 조회.
    regularMarketPrice  → 정규장 or 가장 최근 종가
    preMarketPrice      → 프리장
    postMarketPrice     → 애프터장
    우선순위: 프리/애프터(있으면) > 정규장
    """
    try:
        t = yf.Ticker(ticker)
        info = t.fast_info          # 빠른 메타데이터
        price      = getattr(info, "last_price",           None) or 0
        prev_close = getattr(info, "previous_close",       None) or price

        # fast_info에 pre/post 없으면 .info 시도 (느림)
        try:
            full = t.info
            pre  = full.get("preMarketPrice")
            post = full.get("postMarketPrice")
            reg  = full.get("regularMarketPrice") or price
            prev_close = full.get("regularMarketPreviousClose") or prev_close

            # 현재 어느 세션인지에 따라 최신 가격 선택
            market_state = full.get("marketState", "REGULAR")
            if market_state == "PRE"  and pre:
                price = pre
            elif market_state in ("POST", "POSTPOST") and post:
                price = post
            else:
                price = reg
        except Exception:
            pass

        chg = ((price - prev_close) / prev_close * 100) if prev_close else 0
        return {
            "price":         round(float(price), 2),
            "prevClose":     round(float(prev_close), 2),
            "changePercent": round(float(chg), 4),
        }
    except Exception as e:
        print(f"  [{ticker}] 오류: {e}")
        return {"price": 0, "prevClose": 0, "changePercent": 0}

def refresh_cache():
    print(f"[{time.strftime('%H:%M:%S')}] 주가 갱신 시작...")
    holdings = get_holdings()
    all_tickers = ["SOXX", "SOXL"] + [h["ticker"] for h in holdings]

    # 병렬 다운로드 (yfinance download는 pre/post 미지원 → 단건 병렬)
    results = {}
    threads = []

    def _fetch(ticker):
        results[ticker] = fetch_single(ticker)

    for t in all_tickers:
        th = threading.Thread(target=_fetch, args=(t,))
        th.start()
        threads.append(th)
    for th in threads:
        th.join(timeout=20)

    def etf(t):
        d = results.get(t, {})
        return {
            "price":         d.get("price", 0),
            "changePercent": d.get("changePercent", 0),
        }

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

    data = {
        "soxx":       soxx,
        "soxl":       soxl,
        "holdings":   holding_data,
        "updated_at": int(time.time()),
        "weight_updated_at": int(_weight_cache["updated_at"]),
    }
    with CACHE_LOCK:
        CACHE["data"] = data

    print(
        f"[{time.strftime('%H:%M:%S')}] 갱신 완료 ✓  "
        f"SOXX {soxx['changePercent']:+.2f}%  "
        f"SOXL {soxl['changePercent']:+.2f}%"
    )

def background_worker():
    while True:
        try:
            refresh_cache()
        except Exception as e:
            print(f"[ERROR] 갱신 실패: {e}")
        time.sleep(CACHE_TTL)

# ── Flask 라우트 ─────────────────────────────────────────────────

@app.route("/api/data")
def api_data():
    with CACHE_LOCK:
        data = CACHE["data"]

    if data is None:
        return jsonify({
            "error": "데이터 준비 중입니다. 잠시 후 새로고침하세요."
        }), 503

    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# 최초 1회 데이터 로딩
refresh_cache()

# 백그라운드 갱신 스레드 시작
threading.Thread(
    target=background_worker,
    daemon=True
).start()

print(app.url_map)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)