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
try:
    cred_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    if not cred_json:
        raise ValueError("FIREBASE_SERVICE_ACCOUNT 환경 변수가 없습니다.")
    
    cred = credentials.Certificate(json.loads(cred_json))
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://trand-doc-default-rtdb.firebaseio.com/'
    })
except Exception as e:
    print(f"❌ Firebase 인증 실패: {e}")

# ---------------------------------------------------------
# 2. 34개 종목 데이터
# ---------------------------------------------------------
TICKERS_DATA = {
    "카카오": 60, "인스타그램": 60, "틱톡": 60, "X (트위터)": 60,
    "유튜브": 60, "치지직": 60, "SOOP": 60, "쿠팡": 60,
    "알리": 60, "무신사": 60, "테무": 60, "네이버": 60,
    "구글": 60, "다음": 60, "MS (Bing)": 60, "배달의민족": 60,
    "쿠팡이츠": 60, "요기요": 60, "유튜브 뮤직": 60, "멜론": 60,
    "애플뮤직": 60, "라이엇": 60, "스팀": 60, "넥슨": 60,
    "넷플릭스": 60, "티빙": 60, "쿠팡플레이": 60, "왓챠": 60,
    "네이버웹툰": 60, "카카오페이지": 60, "하이브": 60, "SM": 60,
    "YG": 60, "JYP": 60
}

TICKER_KEYS = list(TICKERS_DATA.keys())
ohlc_buffer = {}

# ---------------------------------------------------------
# 3. 데이터 엔진 (꼬리 생성 및 회귀 로직 강화)
# ---------------------------------------------------------
def generate_ticks():
    try:
        ref = db.reference('chart_data/trends')
        all_trends = ref.get()
        if not all_trends: return
        
        updates_trends = {}
        updates_live = {}
        now_ts = int(time.time())
        
        for ticker, data in all_trends.items():
            try:
                target = data.get('target_yield', 0.0)
                current = data.get('current_yield', 0.0)
                last_update_ts = data.get('last_update_ts', now_ts - 420)
                
                # [로직 1] 7분 선형 수렴 보폭
                elapsed_sec = now_ts - last_update_ts
                remaining_sec = max(10, 420 - elapsed_sec)
                distance = target - current
                ideal_step = distance / remaining_sec
                
                # [로직 2] 변동성 가중치
                pull = ideal_step * random.uniform(0.7, 1.3)
                noise = np.random.normal(0, 0.00035)
                if random.random() < 0.35:
                    noise -= pull * 1.2 

                # 🌟 [개선] 꼬리 만들기 로직 (Wick Pressure)
                # 시가(Open)로 돌아가려는 힘을 부여하여 꼬리를 형성
                current_candle_open = ohlc_buffer.get(ticker, {}).get('open', current)
                # 시가와의 괴리가 커질수록 시가 방향으로 당기는 탄성 계수 상향 (0.07)
                wick_pressure = -(current - current_candle_open) * 0.07
                
                next_tick = round(current + pull + noise + wick_pressure, 6)
                updates_trends[f'{ticker}/current_yield'] = next_tick
                
                # OHLC 버퍼 업데이트
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': next_tick, 'high': next_tick, 'low': next_tick, 'close': next_tick}
                else:
                    ohlc_buffer[ticker]['high'] = max(ohlc_buffer[ticker]['high'], next_tick)
                    ohlc_buffer[ticker]['low'] = min(ohlc_buffer[ticker]['low'], next_tick)
                    ohlc_buffer[ticker]['close'] = next_tick
                
                updates_live[ticker] = {
                    'time': now_ts,
                    'open': ohlc_buffer[ticker]['open'],
                    'high': ohlc_buffer[ticker]['high'],
                    'low': ohlc_buffer[ticker]['low'],
                    'close': ohlc_buffer[ticker]['close']
                }
            except: continue
                
        if updates_trends: db.reference('chart_data/trends').update(updates_trends)
        if updates_live: db.reference('chart_data/live_data').update(updates_live)
    except Exception as e:
        print(f"❌ generate_ticks 에러: {e}")

def record_minute_candle():
    """로그 출력 없이 조용히 1분 봉 저장"""
    try:
        now_utc = datetime.now(pytz.utc).replace(second=0, microsecond=0)
        ts = int(now_utc.timestamp())
        if not ohlc_buffer: return
        for ticker in TICKERS_DATA.keys():
            candle = ohlc_buffer.get(ticker)
            if candle:
                db.reference(f'chart_data/chart_history/{ticker}/1m').push({
                    'time': ts, 'open': candle['open'], 'high': candle['high'], 'low': candle['low'], 'close': candle['close']
                })
                # 다음 봉 시가를 이전 종가로 초기화
                ohlc_buffer[ticker] = {'open': candle['close'], 'high': candle['close'], 'low': candle['close'], 'close': candle['close']}
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")

# ---------------------------------------------------------
# 4. 수집 로직 (1분 내 그룹 완수 및 로그 % 표시)
# ---------------------------------------------------------
def fetch_and_update():
    now = datetime.now(KST)
    now_ts = int(time.time())
    
    group_idx = now.minute % 7
    items_per_group = 5 
    start_idx = group_idx * items_per_group
    end_idx = start_idx + items_per_group
    current_group_tickers = TICKER_KEYS[start_idx:end_idx]
    
    if not current_group_tickers: return

    print(f"\n📊 [그룹 {group_idx} 수집 시작] {now.strftime('%H:%M:%S')}")
    
    try:
        # 429 에러 방지를 위한 retries 및 backoff 설정 유지
        pt = TrendReq(hl='ko-KR', tz=540, retries=5, backoff_factor=2)
        pt.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36'
        
        for ticker in current_group_tickers:
            loop_start = time.time()
            try:
                ref = db.reference(f'chart_data/trends/{ticker}')
                data = ref.get() or {}
                baseline = data.get('baseline', TICKERS_DATA[ticker])
                
                pt.build_payload([ticker], timeframe='now 1-H')
                df = pt.interest_over_time()
                current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
                
                target_yield = round((current_score - baseline) * 0.005 + random.uniform(-0.0005, 0.0005), 5)
                
                ref.update({
                    'last_score': current_score, 
                    'target_yield': target_yield,
                    'last_update_ts': now_ts
                })
                # 모든 수집 라운드에서 % 결과를 로그에 명확히 출력
                print(f" ✅ {ticker}: {target_yield * 100:+.2f}% (점수: {current_score})")
                
            except Exception as e:
                print(f" ❌ {ticker} 수집 실패 (429 혹은 통신에러): {e}")
                continue
            finally:
                # 1분(60초) 내에 5개 종목을 끝내기 위해 대기 시간을 약 11초로 조정
                elapsed = time.time() - loop_start
                if elapsed < 11.0: time.sleep(11.0 - elapsed)
        
        print(f"🏁 그룹 {group_idx} 수집 완료")
    except Exception as e:
        print(f"❌ 수집 세션 에러: {e}")

def initialize_app():
    print("🚀 Firebase 초기화 및 동기화 중...")
    for ticker, avg in TICKERS_DATA.items():
        ref = db.reference(f'chart_data/trends/{ticker}')
        if not ref.get():
            ref.set({
                'baseline': avg, 
                'last_score': avg, 
                'target_yield': 0.0, 
                'current_yield': 0.0,
                'last_update_ts': int(time.time())
            })

# ---------------------------------------------------------
# 5. 스케줄러 설정 (정각 00초 동기화 핵심)
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks():
    generate_ticks()
    next_run = datetime.now(KST) + timedelta(seconds=random.uniform(0.8, 1.5))
    scheduler.add_job(run_ticks, 'date', run_date=next_run)

if __name__ == "__main__":
    initialize_app()
    
    # 서버 실행 시점으로부터 다음 '00초' 정각 계산
    now = datetime.now(KST)
    next_sync_time = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    
    # 로그 출력
    print(f"📡 시스템 대기 중... 첫 동기화 수집 시각: {next_sync_time.strftime('%H:%M:%S')}")

    # 1. 즉시 수집 대신 정각까지 기다렸다가 첫 수집 시작 (00초 동기화 핵심)
    scheduler.add_job(
        fetch_and_update, 
        'interval', 
        minutes=1, 
        start_date=next_sync_time, # 🌟 여기서 정각 시작이 결정됩니다.
        max_instances=1, 
        coalesce=True
    )
    
    # 2. 1분 봉 저장 (로그 출력 없음)
    scheduler.add_job(
        record_minute_candle, 
        'interval', 
        minutes=1, 
        start_date=next_sync_time
    )
    
    # 3. 틱 엔진은 즉시 구동하되 초기 current_yield 기반으로 시작
    run_ticks()
    
    scheduler.start()
    
    # Flask 서버 가동
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
