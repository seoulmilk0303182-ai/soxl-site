"""
시발 SOXL 왜 올라요? — 백엔드
- Yahoo Finance 배치 시세 API(v7/quote)로 SOXX/SOXL + 30개 구성종목을 "한 번에" 조회
  → 종목당 2회씩 총 60여 회 요청하던 기존 방식(약 7~8초) 대비 0.2~1.5초로 단축
- 프리장(PRE) / 정규장(REGULAR) / 애프터장(POST) 세션별 가격·등락률을 야후 값 그대로 사용
- /api/data    → 캐시된 데이터 즉시 반환 (자동갱신용)
- /api/refresh → 즉시 재수집 (수동 새로고침 버튼용) — 동시 요청은 하나로 합침
- 백그라운드 워커: 10초 단위 체크 (장중 60초 / 마감 5분 주기 자동갱신)
- 비중: stockanalysis.com 스크래핑 (백그라운드 갱신, 실패 시 fallback)

의존성: pip install flask yfinance requests
"""

import csv
import io
import time
import threading
import re
import requests
from flask import Flask, jsonify, send_from_directory
import yfinance as yf

app = Flask(__name__, static_folder="static")

# ── 티커 → 한글 종목명 ───────────────────────────────────────────
# iShares CSV 는 영문명만 주므로 여기서 한글로 바꾼다.
# 여기에 없는 티커(신규 편입)는 CSV 의 영문명을 그대로 쓴다.
NAME_MAP = {
    "NVDA": "엔비디아",       "MU":   "마이크론",         "AMD":  "AMD",
    "AVGO": "브로드컴",       "INTC": "인텔",             "TSM":  "TSMC",
    "MRVL": "마벨테크",       "AMAT": "어플라이드머티리얼즈",
    "LRCX": "램리서치",       "KLAC": "KLA",              "ADI":  "아날로그디바이스",
    "TXN":  "텍사스인스트루먼트", "MPWR": "모노리식파워",  "NXPI": "NXP세미컨덕터",
    "QCOM": "퀄컴",           "TER":  "테라다인",         "ASML": "ASML",
    "ALAB": "Astera Labs",    "MCHP": "마이크로칩테크",   "ON":   "온세미컨덕터",
    "CRDO": "Credo Technology", "ASX": "ASE Technology",  "ENTG": "앤테그리스",
    "MTSI": "MACOM Technology", "UMC": "UMC",             "ARM":  "ARM홀딩스",
    "STM":  "ST마이크로",     "NVMI": "노바",             "SWKS": "스카이웍스",
    "RMBS": "램버스",         "QRVO": "Qorvo",            "WOLF": "울프스피드",
    "AMKR": "Amkor",          "SKHY": "SK하이닉스",       "TSEM": "타워세미컨덕터",
    "CBRS": "세레브라스",
}

# ── 최후의 보루: 모든 외부 조회가 실패했을 때 쓰는 비중 스냅샷 ──
# (iShares 공식 보유내역 2026-09-03 기준)
FALLBACK_HOLDINGS = [
    {"ticker": "NVDA", "weight": 9.93}, {"ticker": "MU",   "weight": 9.05},
    {"ticker": "AMD",  "weight": 8.11}, {"ticker": "AVGO", "weight": 7.33},
    {"ticker": "INTC", "weight": 5.30}, {"ticker": "TSM",  "weight": 4.75},
    {"ticker": "MRVL", "weight": 4.67}, {"ticker": "AMAT", "weight": 4.44},
    {"ticker": "LRCX", "weight": 4.22}, {"ticker": "KLAC", "weight": 4.13},
    {"ticker": "ADI",  "weight": 3.95}, {"ticker": "TXN",  "weight": 3.81},
    {"ticker": "MPWR", "weight": 3.28}, {"ticker": "NXPI", "weight": 3.21},
    {"ticker": "QCOM", "weight": 3.08}, {"ticker": "TER",  "weight": 3.01},
    {"ticker": "ASML", "weight": 2.43}, {"ticker": "ALAB", "weight": 2.26},
    {"ticker": "MCHP", "weight": 2.22}, {"ticker": "ON",   "weight": 1.64},
    {"ticker": "CRDO", "weight": 1.55}, {"ticker": "ASX",  "weight": 1.29},
    {"ticker": "ENTG", "weight": 1.13}, {"ticker": "MTSI", "weight": 1.02},
    {"ticker": "UMC",  "weight": 0.95}, {"ticker": "ARM",  "weight": 0.69},
    {"ticker": "STM",  "weight": 0.69}, {"ticker": "NVMI", "weight": 0.64},
    {"ticker": "SWKS", "weight": 0.61}, {"ticker": "RMBS", "weight": 0.52},
]
for _h in FALLBACK_HOLDINGS:
    _h["name"] = NAME_MAP.get(_h["ticker"], _h["ticker"])

# ── 캐시 ─────────────────────────────────────────────────────────
CACHE      = {"data": None}
CACHE_LOCK = threading.Lock()

TTL_OPEN   = 60    # 프리/정규/애프터장: 1분마다 자동갱신
TTL_CLOSED = 300   # 장 마감: 5분마다
MIN_REFRESH_GAP = 3   # 수동 새로고침 최소 간격(초) — 이보다 빠르면 캐시 반환

_refresh_lock = threading.Lock()   # 동시 수동 새로고침을 하나로 합치기 위한 락

_weight_cache = {"holdings": None, "updated_at": 0, "fetching": False}
WEIGHT_TTL    = 86400  # 비중은 하루 1회


# ── 비중 조회 ────────────────────────────────────────────────────
# 1순위: iShares(운용사) 공식 보유내역 CSV — 30종목 전체, 비중 합 99.9%
# 2순위: stockanalysis.com 스크래핑 — 상위 25종목만 실림 (합 96.8%)
# 3순위: 위 FALLBACK_HOLDINGS 스냅샷
ISHARES_CSV = ("https://www.ishares.com/us/products/239705/"
               "ishares-phlx-semiconductor-etf/latest-holdings.csv")


def fetch_weights_ishares():
    """iShares 공식 CSV. 현금·선물·MMF 행은 제외하고 주식만 추린다."""
    try:
        r = requests.get(ISHARES_CSV, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        lines = r.content.decode("utf-8-sig", errors="replace").splitlines()

        hdr = next((i for i, l in enumerate(lines) if l.lstrip('"').startswith("Ticker")), None)
        if hdr is None:
            raise ValueError("Ticker 헤더 행 없음")

        holdings = []
        for row in csv.DictReader(io.StringIO("\n".join(lines[hdr:]))):
            ticker = (row.get("Ticker") or "").strip().upper()
            if not ticker or ticker == "-":
                continue
            if (row.get("Asset Class") or "").strip() != "Equity":
                continue          # 현금(USD) · 선물(IXTU6) · MMF(XTSLA) 제외
            try:
                weight = float((row.get("Weight (%)") or "0").replace(",", ""))
            except ValueError:
                continue
            if weight <= 0:
                continue
            eng = (row.get("Name") or "").strip().title()
            holdings.append({
                "ticker": ticker,
                "name":   NAME_MAP.get(ticker) or eng or ticker,
                "weight": round(weight, 4),
            })

        if len(holdings) < 20:
            raise ValueError(f"종목 수 부족: {len(holdings)}")

        holdings.sort(key=lambda x: x["weight"], reverse=True)
        total = sum(h["weight"] for h in holdings)
        asof  = next((l.split(",", 1)[1].strip(' "') for l in lines[:hdr]
                      if l.startswith("Fund Holdings as of")), "?")
        print(f"[{time.strftime('%H:%M:%S')}] 비중 {len(holdings)}개 갱신 "
              f"(iShares 공식, 합 {total:.2f}%, 기준일 {asof})")
        return holdings
    except Exception as e:
        print(f"[WARN] iShares CSV 실패: {e}")
        return None


def fetch_weights_stockanalysis():
    try:
        r = requests.get(
            "https://stockanalysis.com/etf/soxx/holdings/",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        r.raise_for_status()
        tbody = re.search(r"<tbody[^>]*>(.*?)</tbody>", r.text, re.S)
        if not tbody:
            raise ValueError("tbody not found")
        holdings = []
        for row in re.findall(r"<tr[^>]*>(.*?)</tr>", tbody.group(1), re.S):
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)
            if len(cells) < 4:
                continue
            ticker = re.sub(r"<[^>]+>", "", cells[1]).strip().upper()
            if not ticker or ticker == "SYMBOL":
                continue
            w_raw = re.sub(r"<[^>]+>", "", cells[3]).strip().replace("%", "").replace(",", "")
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
        print(f"[WARN] stockanalysis 스크래핑 실패: {e}")
        return None


def fetch_weights():
    return fetch_weights_ishares() or fetch_weights_stockanalysis()


def _refresh_weights():
    try:
        result = fetch_weights()
        if result:
            _weight_cache["holdings"]   = result
            _weight_cache["updated_at"] = time.time()
        else:
            # 실패해도 재시도 폭주를 막기 위해 30분간은 다시 시도하지 않음
            _weight_cache["updated_at"] = time.time() - WEIGHT_TTL + 1800
    finally:
        _weight_cache["fetching"] = False


def get_holdings():
    """첫 호출은 실제 비중을 기다려 받고(약 0.6초), 이후엔 하루 지났을 때만 백그라운드 갱신.

    첫 호출까지 백그라운드로 돌리면 서버 기동 직후 첫 수집이 옛 스냅샷으로 끝나,
    다음 갱신 주기(최대 5분)까지 SOXX 에 없는 종목이 보인다.
    """
    if _weight_cache["holdings"] is None:
        _refresh_weights()
        if _weight_cache["holdings"] is None:
            _weight_cache["holdings"] = FALLBACK_HOLDINGS
    elif (time.time() - _weight_cache["updated_at"] > WEIGHT_TTL
          and not _weight_cache["fetching"]):
        _weight_cache["fetching"] = True
        threading.Thread(target=_refresh_weights, daemon=True).start()

    return _weight_cache["holdings"]


# ── Yahoo 배치 시세 조회 ─────────────────────────────────────────
# yfinance 의 세션(YfData)을 빌려 쓰면 쿠키/crumb 인증을 알아서 처리해준다.
QUOTE_URL    = "https://query2.finance.yahoo.com/v7/finance/quote"
QUOTE_FIELDS = ",".join([
    "symbol", "marketState", "gmtOffSetMilliseconds",
    "regularMarketPrice",  "regularMarketChangePercent",
    "preMarketPrice",  "preMarketChangePercent",  "preMarketTime",
    "postMarketPrice", "postMarketChangePercent", "postMarketTime",
])
CHUNK_SIZE = 50   # 야후 quote API 심볼 개수 제한 여유분

# 구버전 yfinance 에는 YfData 가 없을 수 있다 — 그 경우 개별 조회로만 동작
try:
    from yfinance.data import YfData
    _yfdata = YfData()
except Exception as _e:
    print(f"[WARN] YfData 사용 불가 — 개별 조회로만 동작합니다: {_e}")
    _yfdata = None


def _f(v):
    """None / 문자열 / dict(raw) 무엇이 와도 float 로."""
    if isinstance(v, dict):
        v = v.get("raw")
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _is_today_at(ts, gmt_offset_sec):
    """거래소 현지 기준으로 ts 가 '오늘'인지.

    야후는 preMarketPrice / postMarketPrice 를 세션이 끝난 뒤에도 계속 들고 있다
    (정규장 15시에 조회해도 그날 09:29 프리장 가격이 그대로 남아있음).
    따라서 오늘 아직 체결이 없는 종목은 '어제 프리장 가격'이 내려올 수 있어,
    타임스탬프로 이번 세션 체결인지 확인해야 한다. 월요일 새벽이면 금요일 값.
    """
    if not ts:
        return False
    a = time.gmtime(ts + gmt_offset_sec)
    b = time.gmtime(time.time() + gmt_offset_sec)
    return (a.tm_year, a.tm_mon, a.tm_mday) == (b.tm_year, b.tm_mon, b.tm_mday)


def normalize_quote(q):
    """야후 원본 quote → 현재 세션에 맞는 가격/등락률로 정규화."""
    state = (q.get("marketState") or "CLOSED").upper()
    off   = _f(q.get("gmtOffSetMilliseconds")) / 1000.0

    reg,  reg_p  = _f(q.get("regularMarketPrice")),  _f(q.get("regularMarketChangePercent"))
    pre,  pre_p  = _f(q.get("preMarketPrice")),      _f(q.get("preMarketChangePercent"))
    post, post_p = _f(q.get("postMarketPrice")),     _f(q.get("postMarketChangePercent"))
    pre_t, post_t = q.get("preMarketTime"), q.get("postMarketTime")

    if state == "PRE":
        session = "pre"
        if pre and _is_today_at(pre_t, off):
            price, chg, quoted = pre, pre_p, True
        else:
            # 오늘 프리장 체결이 아직 없음 — 전일 종가 그대로, 변동 0
            price, chg, quoted = reg, 0.0, False
    elif state == "POST":
        session = "after"
        if post and _is_today_at(post_t, off):
            price, chg, quoted = post, post_p, True
        else:
            price, chg, quoted = reg, reg_p, True   # 애프터 체결 없으면 정규장 종가 기준
    elif state == "REGULAR":
        session = "regular"
        price, chg, quoted = reg, reg_p, True
    else:
        # CLOSED / PREPRE / POSTPOST — 직전 정규장 종가 기준
        session = "closed"
        price, chg, quoted = reg, reg_p, True

    if not price:
        return {"price": 0, "changePercent": 0, "marketState": session, "quoted": False}

    return {
        "price":         round(price, 2),
        "changePercent": round(chg, 4),
        "marketState":   session,
        "quoted":        quoted,
    }


def fetch_quotes(tickers):
    """전 종목을 배치로 한 번에 조회."""
    if _yfdata is None:
        raise RuntimeError("YfData 없음")
    out = {}
    for i in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[i:i + CHUNK_SIZE]
        r = _yfdata.get_raw_json(
            QUOTE_URL,
            params={"symbols": ",".join(chunk), "fields": QUOTE_FIELDS},
            timeout=15,
        )
        for q in r.get("quoteResponse", {}).get("result", []):
            sym = q.get("symbol")
            if sym:
                out[sym] = normalize_quote(q)
    return out


# ── 폴백: 종목별 개별 조회 (배치 실패 시에만) ────────────────────
def fetch_single(ticker):
    try:
        fi    = yf.Ticker(ticker).fast_info
        price = _f(getattr(fi, "last_price", 0))
        prev  = _f(getattr(fi, "previous_close", 0)) or price
        if not price:
            return {"price": 0, "changePercent": 0, "marketState": "closed", "quoted": False}
        chg = ((price - prev) / prev * 100) if prev else 0
        return {"price": round(price, 2), "changePercent": round(chg, 4),
                "marketState": "closed", "quoted": True}
    except Exception as e:
        print(f"  [{ticker}] 오류: {e}")
        return {"price": 0, "changePercent": 0, "marketState": "closed", "quoted": False}


def fetch_quotes_fallback(tickers):
    results = {}

    def _fetch(tk):
        results[tk] = fetch_single(tk)

    threads = [threading.Thread(target=_fetch, args=(tk,)) for tk in tickers]
    for th in threads: th.start()
    for th in threads: th.join(timeout=20)
    return results


# ── 전체 데이터 수집 ─────────────────────────────────────────────
def collect_data():
    t0          = time.time()
    holdings    = get_holdings()
    all_tickers = ["SOXX", "SOXL"] + [h["ticker"] for h in holdings]

    source = "batch"
    try:
        results = fetch_quotes(all_tickers)
        if len(results) < len(all_tickers) // 2:
            raise ValueError(f"응답 종목 부족: {len(results)}/{len(all_tickers)}")
    except Exception as e:
        print(f"[WARN] 배치 조회 실패 → 개별 조회로 폴백: {e}")
        results = fetch_quotes_fallback(all_tickers)
        source  = "fallback"

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
            "changePercent": chg,
            "quoted":        q.get("quoted", False),
            "contribution":  round(chg * h["weight"] / 100, 5),
        })
    holding_data.sort(key=lambda x: abs(x["contribution"]), reverse=True)

    soxx = etf("SOXX")
    soxl = etf("SOXL")
    now  = int(time.time())
    ttl  = TTL_CLOSED if market_status == "closed" else TTL_OPEN
    took = time.time() - t0

    print(
        f"[{time.strftime('%H:%M:%S')}] 갱신 완료 ✓  "
        f"[{market_status}/{source}]  "
        f"SOXX {soxx['changePercent']:+.2f}%  "
        f"SOXL {soxl['changePercent']:+.2f}%  "
        f"({len(holding_data)}종목, {took:.2f}s)"
    )

    return {
        "soxx":              soxx,
        "soxl":              soxl,
        "holdings":          holding_data,
        "updated_at":        now,
        "next_refresh_at":   now + ttl,
        "weight_updated_at": int(_weight_cache["updated_at"]),
        "market_status":     market_status,
    }


def refresh_cache(tag="자동"):
    """수집 후 캐시에 저장. 동시 호출은 락으로 묶어 실제 수집은 한 번만."""
    with _refresh_lock:
        with CACHE_LOCK:
            cached = CACHE["data"]
        # 방금 다른 요청이 갱신했으면 그 결과를 재사용
        if cached and time.time() - cached.get("updated_at", 0) < MIN_REFRESH_GAP:
            return cached
        print(f"[{time.strftime('%H:%M:%S')}] [{tag}] 주가 갱신 시작...")
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
            try:
                refresh_cache("초기")
            except Exception as e:
                print(f"[ERROR] 초기 수집 실패: {e}")
            continue
        if time.time() >= data.get("next_refresh_at", 0):
            try:
                refresh_cache("자동")
            except Exception as e:
                print(f"[ERROR] 자동갱신 실패: {e}")


# 서버 기동을 막지 않도록 첫 수집은 백그라운드에서
threading.Thread(target=lambda: refresh_cache("시작"), daemon=True).start()
threading.Thread(target=background_worker, daemon=True).start()


# ── Flask 라우트 ─────────────────────────────────────────────────
def _json(data):
    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/data")
def api_data():
    """자동갱신·첫 로딩용 — 유효하면 캐시를 즉시, 만료됐으면 그 자리에서 갱신해 반환.

    워커는 10초마다 깨어나므로 만료 직후 0~10초는 옛 데이터가 남아 있다.
    그걸 그대로 주면 클라이언트 카운트다운이 0에 머물러 1초마다 재요청한다.
    """
    with CACHE_LOCK:
        data = CACHE["data"]
    if data is None or time.time() >= data.get("next_refresh_at", 0):
        try:
            data = refresh_cache("요청")
        except Exception as e:
            if data is None:
                return jsonify({"error": f"데이터 준비 중입니다. 잠시 후 다시 시도해주세요. ({e})"}), 503
            print(f"[ERROR] 갱신 실패, 이전 데이터 반환: {e}")
    return _json(data)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """수동 새로고침 버튼용 — 즉시 재수집 후 반환"""
    try:
        return _json(refresh_cache("수동"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False, threaded=True)
