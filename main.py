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
# 2. 34개 종목 리스트 (60점 고정 삭제, 순수 리스트로 변경)
# ---------------------------------------------------------
TICKERS = [
    "카카오", "인스타그램", "틱톡", "X (트위터)", "유튜브", "치지직", "SOOP", "쿠팡", 
    "알리", "무신사", "테무", "네이버", "구글", "다음", "MS (Bing)", "배달의민족", 
    "쿠팡이츠", "요기요", "유튜브 뮤직", "멜론", "애플뮤직", "라이엇", "스팀", "넥슨", 
    "넷플릭스", "티빙", "쿠팡플레이", "왓챠", "네이버웹툰", "카카오페이지", "하이브", "SM", 
    "YG", "JYP"
]

ohlc_buffer = {}

# ---------------------------------------------------------
# 3. 데이터 엔진 (지그재그 및 프론트엔드용 OHLC 라이브 전송)
# ---------------------------------------------------------
def generate_ticks():
    try:
        all_trends = db.reference('chart_data/trends').get()
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
                
                # [개미털기] 20% 확률로 가던 방향의 반대로 살짝 꺾임 (음봉/양봉 교차)
                if abs(distance) > 0.002 and random.random() < 0.20:
                    counter_move = -np.sign(distance) * abs(distance) * random.uniform(0.05, 0.15)
                    noise += counter_move
                
                max_step = max(0.002, abs(distance) * 0.1)
                noise = np.clip(noise, -max_step, max_step) 
                next_tick = round(current + pull + noise, 5)
                
                # 내부 연산용 데이터 업데이트
                updates[f'chart_data/trends/{ticker}/current_yield'] = next_tick
                
                # 1분 봉 조각하기
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': next_tick, 'high': next_tick, 'low': next_tick, 'close': next_tick}
                else:
                    ohlc_buffer[ticker]['high'] = max(ohlc_buffer[ticker]['high'], next_tick)
                    ohlc_buffer[ticker]['low'] = min(ohlc_buffer[ticker]['low'], next_tick)
                    ohlc_buffer[ticker]['close'] = next_tick
                
                # 프론트엔드 실시간 차트용 1분봉 데이터 통째로 전송
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
    """매 분 00초에 버퍼의 데이터를 chart_history에 확정 저장"""
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
                # 다음 분을 위한 초기화
                ohlc_buffer[ticker] = {
                    'open': candle['close'], 'high': candle['close'], 
                    'low': candle['close'], 'close': candle['close']
                }
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")

def daily_midnight_reset():
    """자정 리셋: 그날의 마지막 점수를 다음날의 기준점수로 세팅"""
    try:
        all_trends = db.reference('chart_data/trends').get()
        if not all_trends: return
        for ticker in TICKERS:
            data = all_trends.get(ticker, {})
            last_score = data.get('last_score', 0.0)
            db.reference(f'chart_data/trends/{ticker}').update({
                'baseline': last_score, 'target_yield': 0.0, 'current_yield': 0.0
            })
    except Exception as e:
        print(f"❌ 자정 리셋 실패: {e}")

# ---------------------------------------------------------
# 4. 구글 트렌드 수집 로직 (동적 기준점수 & 1점당 0.5%)
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

    for ticker in TICKERS:
        loop_start_time = time.time()
        try:
            ref = db.reference(f'chart_data/trends/{ticker}')
            data = ref.get() or {}
            baseline = data.get('baseline', 0.0)
            
            pt.build_payload([ticker], timeframe='now 1-H')
            df = pt.interest_over_time()
            
            # 검색량이 없을 경우를 대비한 안전장치
            current_score = float(df[ticker].iloc[-1]) if not df.empty else (baseline if baseline != 0.0 else 50.0)
            
            # [핵심] 최초 실행 시(baseline이 0일 때) 실제 구글 점수를 기준점으로 고정!
            if baseline == 0.0:
                baseline = current_score
            
            # 1점당 0.5% (0.005) 변동폭으로 계산 및 +-0.2% 랜덤 안착
            base_target = (current_score - baseline) * 0.005
            convergence_offset = random.uniform(-0.002, 0.002)
            target_yield = round(base_target + convergence_offset, 5)
            
            # 업데이트
            ref.update({'baseline': baseline, 'last_score': current_score, 'target_yield': target_yield})
            print(f" ✅ {ticker}: {current_score}점 ➡️ 목표 {target_yield * 100:+.2f}%")
            
        except Exception as e:
            print(f" ❌ {ticker} 수집 실패: {e}")
        finally:
            sleep_time = 12.0 - (time.time() - loop_start_time)
            if sleep_time > 0: time.sleep(sleep_time)
            
    print(f"🏁 트렌드 업데이트 완료")

def initialize_app():
    print("🚀 Firebase 초기화 중...")
    try:
        for ticker in TICKERS:
            ref = db.reference(f'chart_data/trends/{ticker}')
            # 데이터가 아예 없을 때만 0으로 껍데기 생성
            if not ref.get():
                ref.set({'baseline': 0.0, 'last_score': 0.0, 'target_yield': 0.0, 'current_yield': 0.0})
        print("✅ 데이터 엔진 준비 완료!")
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")

# ---------------------------------------------------------
# 5. 스케줄러 설정
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_generate_ticks_randomly():
    generate_ticks() 
    delay = random.uniform(0.5, 1.5) # 0.5초 ~ 1.5초 사이 랜덤 실행
    next_run = datetime.now(KST) + timedelta(seconds=delay)
    scheduler.add_job(run_generate_ticks_randomly, 'date', run_date=next_run)

# 랜덤 틱 엔진 시동
run_generate_ticks_randomly()

# 1분 봉 저장 스케줄러
scheduler.add_job(record_minute_candle, 'interval', minutes=1, start_date=datetime.now(KST).replace(second=0, microsecond=0))

# 7분 구글 트렌드 수집 스케줄러
now_kst = datetime.now(KST)
next_minute = (now_kst + timedelta(minutes=1)).replace(second=0, microsecond=0)
scheduler.add_job(fetch_and_update, 'interval', minutes=7, next_run_time=next_minute, max_instances=1, coalesce=True)

# 자정 리셋 스케줄러
scheduler.add_job(daily_midnight_reset, 'cron', hour=0, minute=0)

if __name__ == "__main__":
    initialize_app()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
