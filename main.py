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
# 1. Firebase 인증 (기존 유지)
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

# [추가] 종목 리스트를 고정 순서로 변환
TICKER_KEYS = list(TICKERS_DATA.keys())
ohlc_buffer = {}

# ---------------------------------------------------------
# 3. 데이터 엔진 (기존 유지)
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
                
                distance = target - current
                pull = distance * 0.08 
                noise = np.random.normal(0, 0.0012)
                
                if abs(distance) > 0.002 and random.random() < 0.20:
                    noise -= np.sign(distance) * abs(distance) * 0.15
                
                next_tick = round(current + pull + noise, 6)
                updates_trends[f'{ticker}/current_yield'] = next_tick
                
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
        print(f"✅ [{datetime.now(KST).strftime('%H:%M:%S')}] 1분 봉 저장 완료")
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")

# ---------------------------------------------------------
# [수정] 분산 수집 로직 (중복 없이 그룹별 수집)
# ---------------------------------------------------------
def fetch_and_update():
    now = datetime.now(KST)
    
    # 총 34개 종목을 7분 주기로 나누기 (한 번에 4~5개씩)
    # 현재 분(minute)을 7로 나눈 나머지에 따라 그룹 결정
    group_idx = now.minute % 7
    
    # 수집할 종목 범위 계산
    items_per_group = 5 # 기본 5개씩
    start_idx = group_idx * items_per_group
    end_idx = start_idx + items_per_group
    
    # 마지막 그룹이 리스트 끝까지 가져오도록 처리
    current_group_tickers = TICKER_KEYS[start_idx:end_idx]
    
    if not current_group_tickers:
        print(f"⚠️ [{now.strftime('%H:%M:%S')}] 해당 그룹에 수집할 종목이 없습니다.")
        return

    print(f"\n📊 [그룹 {group_idx} 수집 시작] {now.strftime('%H:%M:%S')} - 대상: {current_group_tickers}")
    
    try:
        pt = TrendReq(hl='ko-KR', tz=540, retries=3, backoff_factor=1)
        pt.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'
        
        for ticker in current_group_tickers:
            loop_start = time.time()
            try:
                ref = db.reference(f'chart_data/trends/{ticker}')
                data = ref.get() or {}
                baseline = data.get('baseline', TICKERS_DATA[ticker])
                
                pt.build_payload([ticker], timeframe='now 1-H')
                df = pt.interest_over_time()
                current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
                
                target_yield = round((current_score - baseline) * 0.005 + random.uniform(-0.001, 0.001), 5)
                ref.update({'last_score': current_score, 'target_yield': target_yield})
                print(f" ✅ {ticker}: {target_yield * 100:+.2f}%")
            except Exception as e:
                print(f" ❌ {ticker} 수집 실패: {e}")
                continue
            finally:
                # 종목당 간격을 좀 더 짧게 조정 (어차피 5개만 하니까 5~8초면 충분)
                elapsed = time.time() - loop_start
                if elapsed < 8.0: time.sleep(8.0 - elapsed)
        print(f"🏁 그룹 {group_idx} 수집 완료")
    except Exception as e:
        print(f"❌ 수집 세션 실패: {e}")

def initialize_app():
    for ticker, avg in TICKERS_DATA.items():
        ref = db.reference(f'chart_data/trends/{ticker}')
        if not ref.get():
            ref.set({'baseline': avg, 'last_score': avg, 'target_yield': 0.0, 'current_yield': 0.0})

# ---------------------------------------------------------
# 4. 스케줄러 설정 (수정됨)
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks():
    generate_ticks()
    next_run = datetime.now(KST) + timedelta(seconds=random.uniform(0.8, 1.5))
    scheduler.add_job(run_ticks, 'date', run_date=next_run)

if __name__ == "__main__":
    initialize_app()
    
    now = datetime.now(KST)
    next_sync_time = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    
    # [변경] 7분 간격이 아니라 '매 1분마다' 실행하되, 함수 내부에서 그룹을 나눠 수집
    scheduler.add_job(
        fetch_and_update, 
        'interval', 
        minutes=1, 
        start_date=now, # 서버 켜자마자 첫 그룹 수집 시작
        max_instances=1, 
        coalesce=True
    )
    
    scheduler.add_job(
        record_minute_candle, 
        'interval', 
        minutes=1, 
        start_date=next_sync_time
    )
    
    run_ticks()
    
    scheduler.start()
    print(f"🚀 분산 수집 시스템 가동됨 (7분 주기로 전체 순환)")
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
