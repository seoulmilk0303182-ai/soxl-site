# 시발 SOXL 왜 올라요? 🚀💀

SOXX ETF 개별 종목 기여도 실시간 대시보드

## 빠른 시작

```bash
# 1. 의존성 설치
pip install flask yfinance

# 2. 서버 실행
python main.py

# 3. 브라우저 접속
http://localhost:8000
```

---

## 파일 구조

```
soxl-site/
├── main.py          # Flask 백엔드 + yfinance 스크래핑 + 5분 캐시
├── requirements.txt
├── static/
│   └── index.html   # 프론트엔드 (다크테마, 반응형)
└── README.md
```

## 기능

- SOXX / SOXL 현재가 & 등락률
- 30개 구성종목 기여도 바 차트 (기여도 = 등락률 × 비중)
- 자동 요약: "왜 오르는지 / 왜 내리는지" 한 줄 판정
- 5분마다 자동 갱신 + 카운트다운 타이머
- 전체 / 상승만 / 하락만 필터 + 기여도/등락률/비중 정렬

---

## 서버 배포

### systemd (Linux 서버)
```ini
# /etc/systemd/system/soxl.service
[Unit]
Description=SOXL Dashboard
After=network.target

[Service]
WorkingDirectory=/path/to/soxl-site
ExecStart=python /path/to/soxl-site/main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```
```bash
sudo systemctl enable soxl
sudo systemctl start soxl
```

### Docker
```bash
docker build -t soxl .
docker run -d -p 8000:8000 --name soxl soxl
```

### Dockerfile
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["python", "main.py"]
```

---

## 주의사항
- yfinance는 Yahoo Finance 비공식 클라이언트입니다.
- 장 외 시간에는 전일 종가 기준으로 표시됩니다.
- SOXL은 일별 3× 레버리지이므로 장기 수익률이 SOXX×3과 다를 수 있습니다.
- 비중은 근사치이며 실제 ETF와 소폭 차이가 있을 수 있습니다.
