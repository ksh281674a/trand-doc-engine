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
# 3. 데이터 엔진 (추세 Conviction & 마이크로 진동 로직)
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
                
                # [로직 1] 시간 비례 수렴 보폭 계산
                elapsed_sec = now_ts - last_update_ts
                remaining_sec = max(5, 420 - elapsed_sec)
                distance = target - current
                ideal_step = distance / remaining_sec
                
                # 수익률이 클수록 정직하게 추종, 낮을수록 지그재그
                reverse_prob = 0.45 - min(0.35, abs(distance) * 10)
                
                if random.random() > reverse_prob:
                    # 정방향 추세 가속도
                    speed_boost = 1.8 + min(2.2, abs(distance) * 15)
                    move = ideal_step * random.uniform(speed_boost * 0.8, speed_boost * 1.2)
                else:
                    # 역방향/눌림목
                    move = -ideal_step * random.uniform(0.7, 1.5)
                
                # 마이크로 진동 (Anti-Quiet)
                shiver = np.random.normal(0, 0.0011) 
                
                # Wick Pressure (꼬리 형성)
                current_candle_open = ohlc_buffer.get(ticker, {}).get('open', current)
                wick_pressure = -(current - current_candle_open) * 0.1
                
                next_tick = round(current + move + shiver + wick_pressure, 6)
                updates_trends[f'{ticker}/current_yield'] = next_tick
                
                # OHLC 버퍼 업데이트
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': next_tick, 'high': next_tick, 'low': next_tick, 'close': next_tick}
                else:
                    ohlc_buffer[ticker]['high'] = max(ohlc_buffer[ticker]['high'], next_tick)
                    ohlc_buffer[ticker]['low'] = min(ohlc_buffer[ticker]['low'], next_tick)
                    ohlc_buffer[ticker]['close'] = next_tick
                
                updates_live[ticker] = {
                    'time': now_ts, 'open': ohlc_buffer[ticker]['open'],
                    'high': ohlc_buffer[ticker]['high'], 'low': ohlc_buffer[ticker]['low'],
                    'close': ohlc_buffer[ticker]['close']
                }
            except: continue
                
        if updates_trends: db.reference('chart_data/trends').update(updates_trends)
        if updates_live: db.reference('chart_data/live_data').update(updates_live)
    except Exception as e:
        print(f"❌ generate_ticks 에러: {e}")

def record_minute_candle():
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
                ohlc_buffer[ticker] = {'open': candle['close'], 'high': candle['close'], 'low': candle['close'], 'close': candle['close']}
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")

# ---------------------------------------------------------
# 4. 수집 및 자정 리셋 로직 (🌟 추가됨)
# ---------------------------------------------------------
def daily_reset():
    """매일 자정(00:00) 전날 최종 점수를 Baseline으로 설정하여 0% 리셋"""
    print(f"\n🕛 [자정 리셋 시작] {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        ref = db.reference('chart_data/trends')
        all_trends = ref.get()
        if not all_trends: return

        updates = {}
        for ticker in TICKER_KEYS:
            data = all_trends.get(ticker, {})
            # 직전 구글 트렌드 점수를 새로운 기준점으로 설정
            last_score = data.get('last_score', TICKERS_DATA.get(ticker, 60))
            
            updates[f'{ticker}/baseline'] = last_score
            updates[f'{ticker}/target_yield'] = 0.0
            updates[f'{ticker}/current_yield'] = 0.0
            updates[f'{ticker}/last_update_ts'] = int(time.time())

            # 로컬 OHLC 버퍼도 리셋 (새로운 0점 기준)
            ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}

        if updates:
            db.reference('chart_data/trends').update(updates)
            print("✅ 자정 리셋 완료: 모든 종목 수익률 0% 기준점 갱신")
    except Exception as e:
        print(f"❌ daily_reset 에러: {e}")

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
        pt = TrendReq(hl='ko-KR', tz=540, retries=5, backoff_factor=2)
        pt.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36'
        
        for ticker in current_group_tickers:
            loop_start = time.time()
            try:
                ref = db.reference(f'chart_data/trends/{ticker}')
                data = ref.get() or {}
                # 자정에 리셋된 baseline을 기준으로 수익률 계산
                baseline = data.get('baseline', TICKERS_DATA.get(ticker, 60))
                
                pt.build_payload([ticker], timeframe='now 1-H')
                df = pt.interest_over_time()
                current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
                
                target_yield = round((current_score - baseline) * 0.005 + random.uniform(-0.0005, 0.0005), 5)
                
                ref.update({
                    'last_score': current_score, 
                    'target_yield': target_yield,
                    'last_update_ts': now_ts
                })
                print(f" ✅ {ticker}: {target_yield * 100:+.2f}% (점수: {current_score})")
            except: continue
            finally:
                elapsed = time.time() - loop_start
                if elapsed < 11.0: time.sleep(11.0 - elapsed)
        print(f"🏁 그룹 {group_idx} 수집 완료")
    except Exception as e:
        print(f"❌ 수집 세션 에러: {e}")

def initialize_app():
    print("🚀 Firebase 초기화 중...")
    for ticker, avg in TICKERS_DATA.items():
        ref = db.reference(f'chart_data/trends/{ticker}')
        if not ref.get():
            ref.set({
                'baseline': avg, 'last_score': avg, 'target_yield': 0.0, 
                'current_yield': 0.0, 'last_update_ts': int(time.time())
            })

# ---------------------------------------------------------
# 5. 스케줄러 설정
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks():
    generate_ticks()
    next_run = datetime.now(KST) + timedelta(seconds=random.uniform(0.5, 1.1))
    scheduler.add_job(run_ticks, 'date', run_date=next_run)

if __name__ == "__main__":
    initialize_app()
    now = datetime.now(KST)
    next_sync_time = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    
    print(f"📡 시스템 대기 중... 첫 정각 동기화 시각: {next_sync_time.strftime('%H:%M:%S')}")

    # 1. 즉시 한 번 수집 실행 및 정각 주기 설정
    scheduler.add_job(fetch_and_update, 'date', run_date=now)
    scheduler.add_job(fetch_and_update, 'interval', minutes=1, start_date=next_sync_time, max_instances=1, coalesce=True)
    scheduler.add_job(record_minute_candle, 'interval', minutes=1, start_date=next_sync_time)
    
    # 🌟 2. 자정 리셋 스케줄러 추가 (매일 00:00:00 실행)
    scheduler.add_job(daily_reset, 'cron', hour=0, minute=0, second=0)
    
    # 3. 실시간 틱 가동
    run_ticks()
    
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
