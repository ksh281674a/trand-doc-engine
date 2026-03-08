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
# 3. 데이터 엔진 (시간 비례 수렴 및 지그재그 로직)
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
                # 목표치가 언제 설정되었는지 확인 (없으면 현재로부터 7분 전으로 가정)
                last_update_ts = data.get('last_update_ts', now_ts - 420)
                
                # [개선 1] 남은 시간 비례 보폭 계산 (7분 = 420초)
                elapsed_sec = now_ts - last_update_ts
                remaining_sec = max(10, 420 - elapsed_sec) # 최소 10초 남은 것으로 간주하여 분모 0 방지
                
                # 가야 할 거리를 남은 시간으로 나누어 초당 적정 보폭(Step) 계산
                distance = target - current
                ideal_step = distance / remaining_sec
                
                # [개선 2] 지그재그 무빙 가중치
                # 보폭에 0.5배 ~ 1.8배 사이의 랜덤성을 주어 속도를 불규칙하게 만듦
                pull = ideal_step * random.uniform(0.5, 1.8)
                
                # [개선 3] 역방향 노이즈 (일직선 방지)
                # 35% 확률로 목표 방향과 반대로 튀는 힘을 가함
                noise = np.random.normal(0, 0.0004)
                if random.random() < 0.35:
                    noise -= pull * 1.3 # 가야 할 힘보다 조금 더 강하게 반대로 튕김
                
                next_tick = round(current + pull + noise, 6)
                updates_trends[f'{ticker}/current_yield'] = next_tick
                
                # OHLC 업데이트
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
# [수정] 수집 주기 동기화 및 시간 기록 추가
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
                
                # [중요] 목표치 설정 시간(last_update_ts)을 함께 기록하여 엔진이 보폭을 계산하게 함
                ref.update({
                    'last_score': current_score, 
                    'target_yield': target_yield,
                    'last_update_ts': now_ts
                })
                print(f" ✅ {ticker}: {target_yield * 100:+.2f}%")
            except: continue
            finally:
                elapsed = time.time() - loop_start
                if elapsed < 8.0: time.sleep(8.0 - elapsed)
        print(f"🏁 그룹 {group_idx} 수집 완료")
    except Exception as e:
        print(f"❌ 수집 세션 실패: {e}")

def initialize_app():
    print("🚀 Firebase 초기화 및 시간 동기화...")
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
# 4. 스케줄러 설정
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
    
    # 매 1분마다 그룹별 수집 (7분에 걸쳐 전체 순환)
    scheduler.add_job(
        fetch_and_update, 
        'interval', 
        minutes=1, 
        start_date=now,
        max_instances=1, 
        coalesce=True
    )
    
    # 1분 봉 기록
    scheduler.add_job(
        record_minute_candle, 
        'interval', 
        minutes=1, 
        start_date=next_sync_time
    )
    
    run_ticks()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
