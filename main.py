"""
시발 SOXL 왜 올라요? — 백엔드 최종판
- yfinance 로 SOXX/SOXL + 30개 구성종목 주가 조회 (프리/애프터 포함)
- /api/data  → 캐시된 데이터 즉시 반환 (자동갱신용)
- /api/refresh → 즉시 yfinance 재수집 (수동 새로고침 버튼용)
- 백그라운드 워커: 10초 단위 체크로 5분마다 자동갱신 (절전/탭비활성 누락 방지)
- 비중: stockanalysis.com 스크래핑 (실패 시 fallback)

의존성: pip install flask yfinance requests
"""

import time
import threading
import re
import requests
from flask import Flask, jsonify, send_from_directory
import yfinance as yf

app = Flask(__name__, static_folder="static")

# ── SOXX 구성종목 fallback 비중 (2026년 기준 추정) ──────────────
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

# ── 캐시 ─────────────────────────────────────────────────────────
CACHE      = {"data": None}
CACHE_LOCK = threading.Lock()
CACHE_TTL  = 300   # 5분

_weight_cache  = {"holdings": None, "updated_at": 0}
WEIGHT_TTL     = 86400  # 비중은 하루 1회


# ── 비중 스크래핑 ────────────────────────────────────────────────
def fetch_weights():
    try:
        r = requests.get(
            "https://stockanalysis.com/etf/soxx/holdings/",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20,
        )
        r.raise_for_status()
        tbody = re.search(r'<tbody[^>]*>(.*?)</tbody>', r.text, re.S)
        if not tbody:
            raise ValueError("tbody not found")
        holdings = []
        for row in re.findall(r'<tr[^>]*>(.*?)</tr>', tbody.group(1), re.S):
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)
            if len(cells) < 4:
                continue
            ticker = re.sub(r'<[^>]+>', '', cells[1]).strip().upper()
            if not ticker or ticker == 'SYMBOL':
                continue
            w_raw = re.sub(r'<[^>]+>', '', cells[3]).strip().replace('%','').replace(',','')
            try:
                weight = float(w_raw)
            except ValueError:
                continue
            if weight <= 0:
                continue
            holdings.append({"ticker": ticker, "name": NAME_MAP.get(ticker, ticker), "weight": round(weight, 4)})
        if len(holdings) < 10:
            raise ValueError(f"종목 수 부족: {len(holdings)}")
        holdings.sort(key=lambda x: x["weight"], reverse=True)
        print(f"[{time.strftime('%H:%M:%S')}] 비중 {len(holdings)}개 갱신 (stockanalysis)")
        return holdings
    except Exception as e:
        print(f"[WARN] 비중 스크래핑 실패: {e}")
        return None


def get_holdings():
    now = time.time()
    if _weight_cache["holdings"] is None or now - _weight_cache["updated_at"] > WEIGHT_TTL:
        result = fetch_weights()
        _weight_cache["holdings"]   = result or FALLBACK_HOLDINGS
        _weight_cache["updated_at"] = now
    return _weight_cache["holdings"]


# ── yfinance 단건 시세 조회 (프리/애프터 포함) ───────────────────
def fetch_single(ticker):
    try:
        t    = yf.Ticker(ticker)
        fi   = t.fast_info
        price      = float(getattr(fi, "last_price",     0) or 0)
        prev_close = float(getattr(fi, "previous_close", 0) or 0)
        market_state = "closed"

        try:
            info  = t.info
            state = info.get("marketState", "CLOSED").upper()
            reg   = float(info.get("regularMarketPrice",          0) or 0)
            pre   = float(info.get("preMarketPrice",              0) or 0)
            post  = float(info.get("postMarketPrice",             0) or 0)
            prev_close = float(info.get("regularMarketPreviousClose", 0) or prev_close or reg)

            if   state == "PRE"                    and pre:  price = pre;  market_state = "pre"
            elif state in ("POST", "POSTPOST")     and post: price = post; market_state = "after"
            elif reg:                                         price = reg;  market_state = "regular" if state == "REGULAR" else "closed"
        except Exception:
            pass

        if not price:
            return {"price": 0, "prevClose": 0, "changePercent": 0, "marketState": "closed"}

        prev = prev_close or price
        chg  = ((price - prev) / prev * 100) if prev else 0
        return {
            "price":         round(price, 2),
            "prevClose":     round(prev, 2),
            "changePercent": round(chg, 4),
            "marketState":   market_state,
        }
    except Exception as e:
        print(f"  [{ticker}] 오류: {e}")
        return {"price": 0, "prevClose": 0, "changePercent": 0, "marketState": "closed"}


# ── 전체 데이터 수집 (공통) ──────────────────────────────────────
def collect_data():
    """yfinance로 전 종목 병렬 수집 후 딕셔너리 반환."""
    t0       = time.time()
    holdings = get_holdings()
    all_tickers = ["SOXX", "SOXL"] + [h["ticker"] for h in holdings]

    results = {}
    def _fetch(tk):
        results[tk] = fetch_single(tk)

    threads = [threading.Thread(target=_fetch, args=(tk,)) for tk in all_tickers]
    for th in threads: th.start()
    for th in threads: th.join(timeout=30)

    market_status = results.get("SOXX", {}).get("marketState", "closed")

    def etf(tk):
        d = results.get(tk, {})
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
    now  = int(time.time())

    print(
        f"[{time.strftime('%H:%M:%S')}] 갱신 완료 ✓  "
        f"[{market_status}]  "
        f"SOXX {soxx['changePercent']:+.2f}%  "
        f"SOXL {soxl['changePercent']:+.2f}%  "
        f"({len(holding_data)}종목, {time.time()-t0:.1f}s)"
    )

    return {
        "soxx":              soxx,
        "soxl":              soxl,
        "holdings":          holding_data,
        "updated_at":        now,
        "next_refresh_at":   now + CACHE_TTL,
        "weight_updated_at": int(_weight_cache["updated_at"]),
        "market_status":     market_status,
    }


# ── 캐시 갱신 (백그라운드 자동갱신용) ────────────────────────────
def refresh_cache():
    print(f"[{time.strftime('%H:%M:%S')}] [자동] 주가 갱신 시작...")
    data = collect_data()
    with CACHE_LOCK:
        CACHE["data"] = data


# ── 즉시 수집 (수동 새로고침 버튼용) ─────────────────────────────
def force_refresh():
    print(f"[{time.strftime('%H:%M:%S')}] [수동] 주가 갱신 시작...")
    data = collect_data()
    with CACHE_LOCK:
        CACHE["data"] = data
    return data


# ── 백그라운드 워커 ───────────────────────────────────────────────
# 10초마다 깨어나 "갱신 시각이 됐는지" 체크 → 절전/탭비활성 누락 방지
def background_worker():
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
                print(f"[ERROR] 자동갱신 실패: {e}")


# 서버 시작 시 1회 즉시 수집
refresh_cache()
threading.Thread(target=background_worker, daemon=True).start()


# ── Flask 라우트 ─────────────────────────────────────────────────
@app.route("/api/data")
def api_data():
    """자동갱신용 — 캐시된 데이터 즉시 반환 (빠름)"""
    with CACHE_LOCK:
        data = CACHE["data"]
    if data is None:
        return jsonify({"error": "데이터 준비 중. 잠시 후 새로고침하세요."}), 503
    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """수동 새로고침 버튼용 — yfinance 즉시 재수집 후 반환 (5~15초 소요)"""
    try:
        data = force_refresh()
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