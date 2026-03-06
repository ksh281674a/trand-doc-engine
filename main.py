import time
import random
import numpy as np
import warnings
import pytz
from datetime import datetime, timedelta
from threading import Thread
from flask import Flask
from pytrends.request import TrendReq
from apscheduler.schedulers.background import BackgroundScheduler
import firebase_admin
from firebase_admin import credentials, db

# --- [1] 기본 설정 ---
warnings.filterwarnings('ignore')
KST = pytz.timezone('Asia/Seoul')
app = Flask(__name__)

# Cloudtype 상태 확인용
@app.route('/')
def home():
    return f"TrendDoc Engine Active - {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}"

# Firebase 연결 (실제 본인의 URL과 JSON 키 파일 확인 필수)
try:
    cred = credentials.Certificate('service_account.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://trand-doc-default-rtdb.firebaseio.com/'
    })
except Exception as e:
    print(f"⚠️ Firebase 초기화 건너뜀 (로컬 실행용): {e}")

# 종목 리스트 (실제 검색어로 변경 가능)
STOCKS = [
    "Nvidia", "Apple", "Tesla", "Microsoft", "Google", 
    "Amazon", "Meta", "Netflix", "AMD", "Intel"
    # ... 총 34개를 채우시면 됩니다.
] + [f"Stock_{i}" for i in range(11, 35)]

stock_states = {}
for ticker in STOCKS:
    stock_states[ticker] = {
        "base_score": 60.0,    # 어제 종가 기준 (초기 60)
        "current_pct": 0.0,    # 당일 변동률
        "target_pct": 0.0,     # 구글 트렌드 목표 변동률
        "remaining_sec": 420,  # 7분 수렴 타이머
        "rev_left": 0,         # 남은 반전 횟수
        "direction": 1,        # 모멘텀 방향
        "volatility": 0.05     # 현재 변동성
    }

# --- [2] 4대 물리 엔진 (가우시안, 모멘텀, 클러스터링, 회귀) ---
def calculate_tick(ticker):
    s = stock_states[ticker]
    rem = s["remaining_sec"]
    
    if rem <= 0:
        # 7분 종료: 목표치 근처 +-0.15% 수렴
        s["current_pct"] = s["target_pct"] + random.uniform(-0.15, 0.15)
    else:
        # 1. 수렴 동력 (Drift)
        drift = (s["target_pct"] - s["current_pct"]) / (rem + 1)

        # 2. 가우시안 노이즈 (미세 떨림)
        noise = np.random.normal(0, s["volatility"])

        # 3. 모멘텀 시스템 (반전 포인트)
        if s["rev_left"] > 0 and random.random() < (s["rev_left"] / rem):
            s["direction"] *= -1
            s["rev_left"] -= 1
        momentum = s["direction"] * (abs(s["target_pct"]) * 0.005 + 0.005)

        # 4. 변동성 클러스터링 & 평균 회귀
        delta = drift + noise + momentum
        
        # 급등락 시 변동성 일시 증가 (클러스터링)
        if abs(delta) > 0.1: s["volatility"] *= 1.1
        else: s["volatility"] *= 0.99
        s["volatility"] = clip(s["volatility"], 0.01, 0.2)

        s["current_pct"] += delta
        s["remaining_sec"] -= 1

    final_score = s["base_score"] * (1 + s["current_pct"] / 100)
    return round(final_score, 2), round(s["current_pct"], 2)

def clip(n, smallest, largest):
    return max(smallest, min(n, largest))

# --- [3] 구글 트렌드 실시간 수집 (Pytrends) ---
def fetch_trends_batch(batch_num):
    start_idx = batch_num * 5
    end_idx = min(start_idx + 5, len(STOCKS))
    subset = STOCKS[start_idx:end_idx]
    
    pytrends = TrendReq(hl='ko-KR', tz=540)
    now_str = datetime.now(KST).strftime('%H:%M:%S')
    
    print(f"\n[{now_str}] 🔍 구글 수집 시작 (Batch {batch_num+1}/7): {', '.join(subset)}")
    
    try:
        pytrends.build_payload(subset, timeframe='now 1-H', geo='KR')
        df = pytrends.interest_over_time()
        
        for ticker in subset:
            # 트렌드 값(0~100) 가져오기, 실패 시 랜덤 보정
            val = int(df[ticker].iloc[-1]) if not df.empty and ticker in df.columns else random.randint(40, 80)
            
            # 1점 = 0.5% 변동률 목표 설정
            target_pct = (val - 60) * 0.5  # 60점 기준 위아래 변동
            
            stock_states[ticker].update({
                "target_pct": target_pct,
                "remaining_sec": 420,
                "rev_left": random.randint(0, 5),
                "direction": 1 if target_pct > stock_states[ticker]["current_pct"] else -1
            })
            
            print(f"  ✅ [수집완료] {ticker:10} | 트렌드: {val:2}점 | 목표수익률: {target_pct:>5.1f}%")
            
    except Exception as e:
        print(f"  ⚠️ [수집실패] {subset} 에러: {e}")

# --- [4] 자정 리셋 및 스케줄러 ---
def daily_reset():
    print(f"\n[{datetime.now(KST).date()}] 🕒 자정 리셋: 전날 종가 덮어쓰기")
    for ticker in STOCKS:
        final_score, _ = calculate_tick(ticker)
        stock_states[ticker]["base_score"] = final_score
        stock_states[ticker]["current_pct"] = 0.0
        stock_states[ticker]["target_pct"] = 0.0

scheduler = BackgroundScheduler(timezone=KST)
# 매일 20:10:30부터 1분 간격으로 7번 배치 실행
for i in range(7):
    scheduler.add_job(fetch_trends_batch, 'cron', hour=20, minute=10+i, second=30, args=[i])

# 매일 자정 리셋
scheduler.add_job(daily_reset, 'cron', hour=0, minute=0)
scheduler.start()

# --- [5] 실시간 데이터 전송 루프 ---
def physics_loop():
    print("🚀 물리 시뮬레이션 엔진 가동...")
    while True:
        updates = {}
        for ticker in STOCKS:
            score, pct = calculate_tick(ticker)
            updates[f"live/{ticker}"] = {
                "score": score,
                "pct": pct,
                "target": stock_states[ticker]["target_pct"]
            }
        
        # Firebase 일괄 업데이트 (네트워크 효율을 위해 1초마다)
        try:
            db.reference('realtime_stocks').update(updates)
        except:
            pass
        time.sleep(1)

if __name__ == "__main__":
    # 1. Flask 서버 (Cloudtype용 포트 8080)
    Thread(target=lambda: app.run(host='0.0.0.0', port=8080, use_reloader=False)).start()
    
    # 2. 물리 엔진 루프
    physics_loop()
