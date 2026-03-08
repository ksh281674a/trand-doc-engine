import time
import os
import json
import random
import numpy as np
from datetime import datetime, timedelta
import pytz
from pytrends_modern import TrendReq
import firebase_admin
from firebase_admin import credentials, db
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

app = Flask(__name__)
KST = pytz.timezone('Asia/Seoul')

# ---------------------------------------------------------
# 1. Firebase 인증
# ---------------------------------------------------------
cred = credentials.Certificate(json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"]))
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://trand-doc-default-rtdb.firebaseio.com/'
})

TICKERS = [
    "카카오", "인스타그램", "틱톡", "X (트위터)", "유튜브", "치지직", "SOOP", "쿠팡", 
    "알리", "무신사", "테무", "네이버", "구글", "다음", "MS (Bing)", "배달의민족", 
    "쿠팡이츠", "요기요", "유튜브 뮤직", "멜론", "애플뮤직", "라이엇", "스팀", "넥슨", 
    "넷플릭스", "티빙", "쿠팡플레이", "왓챠", "네이버웹툰", "카카오페이지", "하이브", "SM", 
    "YG", "JYP"
]

ohlc_buffer = {}

# ---------------------------------------------------------
# 2. 데이터 엔진 (동적 변화 및 경로 수리)
# ---------------------------------------------------------
def generate_ticks():
    try:
        # 1. 경로 확인: chart_data/trends 하위 데이터를 가져옴
        all_trends = db.reference('chart_data/trends').get()
        if not all_trends: return
        
        updates = {}
        # [수정] current_ts를 1분 고정이 아닌, 현재 '초' 단위까지 가져옴 (프론트 갱신 트리거)
        now_ts = int(time.time()) 
        
        for ticker, data in all_trends.items():
            try:
                target = data.get('target_yield', 0.0)
                current = data.get('current_yield', 0.0)
                
                # 2. 데이터 변화 여부: 노이즈 강도를 높여 round(5)에서도 무조건 변하게 함
                distance = target - current
                pull = distance * 0.1  # 추종 속도 상향
                noise = np.random.normal(0, 0.0015) # 노이즈 강도 상향
                
                # 개미털기 로직 (변동성 부여)
                if abs(distance) > 0.002 and random.random() < 0.25:
                    noise += -np.sign(distance) * random.uniform(0.001, 0.003)
                
                next_tick = round(current + pull + noise, 6) # 정밀도 한 자릿수 추가
                
                # 내부 수익률 업데이트
                updates[f'chart_data/trends/{ticker}/current_yield'] = next_tick
                
                # 3. OHLC 버퍼 업데이트
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': next_tick, 'high': next_tick, 'low': next_tick, 'close': next_tick}
                else:
                    ohlc_buffer[ticker]['high'] = max(ohlc_buffer[ticker]['high'], next_tick)
                    ohlc_buffer[ticker]['low'] = min(ohlc_buffer[ticker]['low'], next_tick)
                    ohlc_buffer[ticker]['close'] = next_tick
                
                # [수정] 실시간 데이터 경로: chart_data/live_data/{ticker} 하위에 객체 통째로 덮어쓰기
                # 이제 {ticker}: { 'time': ..., 'open': ... } 형태로 완벽하게 전송됩니다.
                updates[f'chart_data/live_data/{ticker}'] = {
                    'time': now_ts, 
                    'open': ohlc_buffer[ticker]['open'],
                    'high': ohlc_buffer[ticker]['high'],
                    'low': ohlc_buffer[ticker]['low'],
                    'close': ohlc_buffer[ticker]['close']
                }
                    
            except: continue
                
        if updates:
            db.reference('/').update(updates)

    except Exception as e:
        print(f"❌ generate_ticks 에러: {e}")

def record_minute_candle():
    try:
        now_utc = datetime.now(pytz.utc).replace(second=0, microsecond=0)
        ts = int(now_utc.timestamp())
        if not ohlc_buffer: return

        for ticker in TICKERS:
            candle = ohlc_buffer.get(ticker)
            if candle:
                db.reference(f'chart_data/chart_history/{ticker}/1m').push({
                    'time': ts,
                    'open': candle['open'],
                    'high': candle['high'],
                    'low': candle['low'],
                    'close': candle['close']
                })
                # 버퍼 초기화 (다음 분 시작)
                ohlc_buffer[ticker] = {
                    'open': candle['close'], 'high': candle['close'], 
                    'low': candle['close'], 'close': candle['close']
                }
        print(f"✅ [{datetime.now(KST).strftime('%H:%M:%S')}] 1분 봉 저장 완료")
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")

# ---------------------------------------------------------
# 3. 구글 트렌드 수집 및 초기화 (기존 로직 유지)
# ---------------------------------------------------------
def fetch_and_update():
    try:
        pt = TrendReq(hl='ko-KR', tz=540, retries=3)
        for ticker in TICKERS:
            ref = db.reference(f'chart_data/trends/{ticker}')
            data = ref.get() or {}
            baseline = data.get('baseline', 0.0)
            
            pt.build_payload([ticker], timeframe='now 1-H')
            df = pt.interest_over_time()
            current_score = float(df[ticker].iloc[-1]) if not df.empty else (baseline if baseline != 0.0 else 50.0)
            
            if baseline == 0.0: baseline = current_score
            
            # 1점당 0.5% 변동
            target_yield = round((current_score - baseline) * 0.005 + random.uniform(-0.001, 0.001), 5)
            
            ref.update({'baseline': baseline, 'last_score': current_score, 'target_yield': target_yield})
            print(f" ✅ {ticker}: {current_score}점 (목표 {target_yield*100:+.2f}%)")
            time.sleep(10) # 구글 차단 방지
    except Exception as e:
        print(f"❌ 수집 에러: {e}")

def initialize_app():
    for ticker in TICKERS:
        ref = db.reference(f'chart_data/trends/{ticker}')
        if not ref.get():
            ref.set({'baseline': 0.0, 'last_score': 0.0, 'target_yield': 0.0, 'current_yield': 0.0})

# ---------------------------------------------------------
# 4. 실행 및 스케줄러
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_generate_ticks_randomly():
    generate_ticks() 
    delay = random.uniform(0.8, 1.8) # 약 1초 내외로 더 역동적인 틱 생성
    next_run = datetime.now(KST) + timedelta(seconds=delay)
    scheduler.add_job(run_generate_ticks_randomly, 'date', run_date=next_run)

run_generate_ticks_randomly()
scheduler.add_job(record_minute_candle, 'interval', minutes=1, start_date=datetime.now(KST).replace(second=0, microsecond=0))
scheduler.add_job(fetch_and_update, 'interval', minutes=7)

if __name__ == "__main__":
    initialize_app()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
