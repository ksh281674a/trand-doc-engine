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

# ---------------------------------------------------------
# 2. 34개 종목 데이터 및 OHLC 버퍼
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

ohlc_buffer = {}

# ---------------------------------------------------------
# 3. 데이터 엔진 (보안 규칙 준수 및 봉 생성 보장)
# ---------------------------------------------------------
def generate_ticks():
    try:
        # [수정] 데이터 경로 유연성 확보 (둘 다 체크)
        root_ref = db.reference('chart_data/trends').get()
        legacy_ref = db.reference('trends').get()
        all_trends = root_ref if root_ref else legacy_ref
        
        if not all_trends:
            return
        
        updates = {}
        now_utc = datetime.now(pytz.utc).replace(second=0, microsecond=0)
        current_ts = int(now_utc.timestamp())
        
        for ticker, data in all_trends.items():
            try:
                target = data.get('target_yield', 0.0)
                current = data.get('current_yield', 0.0)
                
                distance = target - current
                pull = distance * 0.03 
                noise = np.random.normal(0, 0.001)
                
                if abs(distance) > 0.002 and random.random() < 0.20:
                    counter_move = -np.sign(distance) * abs(distance) * random.uniform(0.05, 0.15)
                    noise += counter_move
                
                max_step = max(0.002, abs(distance) * 0.1)
                noise = np.clip(noise, -max_step, max_step) 
                next_tick = round(current + pull + noise, 5)
                
                # 내부 연산용 및 보안 경로 업데이트
                updates[f'chart_data/trends/{ticker}/current_yield'] = next_tick
                
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': next_tick, 'high': next_tick, 'low': next_tick, 'close': next_tick}
                else:
                    ohlc_buffer[ticker]['high'] = max(ohlc_buffer[ticker]['high'], next_tick)
                    ohlc_buffer[ticker]['low'] = min(ohlc_buffer[ticker]['low'], next_tick)
                    ohlc_buffer[ticker]['close'] = next_tick
                
                # 실시간 OHLC 패키지 전송 (chart_data/live_data/{ticker})
                updates[f'chart_data/live_data/{ticker}'] = {
                    'time': current_ts,
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
    """매 1분마다 버퍼 데이터를 chart_history에 push하여 과거 데이터 축적"""
    try:
        now_utc = datetime.now(pytz.utc).replace(second=0, microsecond=0)
        ts = int(now_utc.timestamp())
        
        if not ohlc_buffer: return

        save_count = 0
        for ticker in TICKERS_DATA.keys():
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
                save_count += 1
        
        print(f"✅ [{datetime.now(KST).strftime('%H:%M:%S')}] 1분 봉 저장 완료 ({save_count}종목, TS:{ts})")
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")

def daily_midnight_reset():
    try:
        all_trends = db.reference('chart_data/trends').get()
        if not all_trends: return
        for ticker in TICKERS_DATA.keys():
            data = all_trends.get(ticker, {})
            last_score = data.get('last_score', TICKERS_DATA[ticker])
            db.reference(f'chart_data/trends/{ticker}').update({
                'baseline': last_score, 'target_yield': 0.0, 'current_yield': 0.0
            })
    except Exception as e:
        print(f"❌ 자정 리셋 실패: {e}")

# ---------------------------------------------------------
# 4. 트렌드 수집 로직
# ---------------------------------------------------------
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0 Safari/537.36'
]

def fetch_and_update():
    now = datetime.now(KST)
    print(f"\n📊 [수집 라운드 시작] {now.strftime('%H:%M:%S')}")

    try:
        pt = TrendReq(hl='ko-KR', tz=540, retries=3, backoff_factor=1)
        pt.headers['User-Agent'] = random.choice(USER_AGENTS)
    except Exception as e:
        print(f"❌ 구글 트렌드 세션 연결 실패: {e}")
        return

    for ticker in TICKERS_DATA.keys():
        loop_start_time = time.time()
        try:
            ref = db.reference(f'chart_data/trends/{ticker}')
            data = ref.get()
            baseline = data.get('baseline', TICKERS_DATA[ticker])
            
            pt.build_payload([ticker], timeframe='now 1-H')
            df = pt.interest_over_time()
            current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
            
            base_target = (current_score - baseline) * 0.007
            convergence_offset = random.uniform(-0.002, 0.002)
            target_yield = round(base_target + convergence_offset, 5)
            
            ref.update({'last_score': current_score, 'target_yield': target_yield})
            print(f" ✅ {ticker}: {target_yield * 100:+.2f}%")
        except: continue
        finally:
            sleep_time = 12.0 - (time.time() - loop_start_time)
            if sleep_time > 0: time.sleep(sleep_time)
            
    print(f"🏁 트렌드 업데이트 완료")

def initialize_app():
    print("🚀 Firebase 초기화 중...")
    try:
        for ticker, avg in TICKERS_DATA.items():
            ref = db.reference(f'chart_data/trends/{ticker}')
            if not ref.get():
                ref.set({'baseline': avg, 'last_score': avg, 'target_yield': 0.0, 'current_yield': 0.0})
        print("✅ 데이터 엔진 준비 완료!")
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")

# ---------------------------------------------------------
# 5. 스케줄러 설정 (기능 강화)
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_generate_ticks_randomly():
    generate_ticks() 
    delay = random.uniform(0.5, 1.5)
    next_run = datetime.now(KST) + timedelta(seconds=delay)
    scheduler.add_job(run_generate_ticks_randomly, 'date', run_date=next_run)

# 엔진 시동
run_generate_ticks_randomly()

# [수정] Interval 방식으로 매 분 봉 생성 보장
scheduler.add_job(record_minute_candle, 'interval', minutes=1, start_date=datetime.now(KST).replace(second=0, microsecond=0))

now_kst = datetime.now(KST)
next_minute = (now_kst + timedelta(minutes=1)).replace(second=0, microsecond=0)
scheduler.add_job(fetch_and_update, 'interval', minutes=7, next_run_time=next_minute, max_instances=1, coalesce=True)

scheduler.add_job(daily_midnight_reset, 'cron', hour=0, minute=0)

if __name__ == "__main__":
    initialize_app()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
