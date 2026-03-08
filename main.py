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
# 3. 데이터 엔진 (꼬리 생성 로직 포함)
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
                
                # [로직 1] 7분 수렴 보폭 계산
                elapsed_sec = now_ts - last_update_ts
                remaining_sec = max(10, 420 - elapsed_sec)
                distance = target - current
                ideal_step = distance / remaining_sec
                
                # [로직 2] 지그재그 랜덤 가중치
                pull = ideal_step * random.uniform(0.6, 1.4)
                
                # [로직 3] 역방향 노이즈 (출렁임)
                noise = np.random.normal(0, 0.0003)
                if random.random() < 0.30:
                    noise -= pull * 1.1 

                # 🌟 [신규] 꼬리 만들기 로직 (Wick Pressure)
                # 시가(Open)로부터 멀어질수록 반대 방향으로 되돌아가려는 힘을 줍니다.
                current_candle_open = ohlc_buffer.get(ticker, {}).get('open', current)
                # 시가와의 괴리율에 비례한 저항력 (0.05 계수)
                wick_pressure = -(current - current_candle_open) * 0.05
                
                next_tick = round(current + pull + noise + wick_pressure, 6)
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
                ohlc_buffer[ticker] = {'open': candle['close'], 'high': candle['close'], 'low': candle['close'], 'close': candle['close']}
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")

# ---------------------------------------------------------
# 4. 수집 로직 (로그 % 표시 수정 및 1분 내 완료 보장)
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
        pt = TrendReq(hl='ko-KR', tz=540, retries=3, backoff_factor=1)
        pt.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'
        
        for ticker in current_group_tickers:
            loop_start = time.time()
            try:
                ref = db.reference(f'chart_data/trends/{ticker}')
                # 기존 데이터가 없으면 기본값으로 초기화
                data = ref.get() or {}
                baseline = data.get('baseline', TICKERS_DATA[ticker])
                
                pt.build_payload([ticker], timeframe='now 1-H')
                df = pt.interest_over_time()
                current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
                
                # 목표 수익률 계산 (1점당 0.5%)
                target_yield = round((current_score - baseline) * 0.005 + random.uniform(-0.001, 0.001), 5)
                
                ref.update({
                    'last_score': current_score, 
                    'target_yield': target_yield,
                    'last_update_ts': now_ts
                })
                # 로그에 퍼센트가 항상 찍히도록 print 위치 조정
                print(f" ✅ {ticker}: {target_yield * 100:+.2f}% (점수: {current_score})")
            except Exception as e:
                print(f" ❌ {ticker} 수집 실패: {e}")
                continue
            finally:
                # 1분 안에 5개를 끝내기 위해 종목당 대기 시간을 9초로 고정 (9*5 = 45초)
                elapsed = time.time() - loop_start
                if elapsed < 9.0: time.sleep(9.0 - elapsed)
        print(f"🏁 그룹 {group_idx} 수집 완료")
    except Exception as e:
        print(f"❌ 수집 세션 실패: {e}")

def initialize_app():
    print("🚀 Firebase 초기화 및 동기화...")
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
# 5. 스케줄러 설정
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
    
    # 1. 즉시 한 번 수집 실행 (서버 시작 시 데이터 확보)
    scheduler.add_job(fetch_and_update, 'date', run_date=now)
    
    # 2. 매 1분마다 그룹별 수집 (7분에 걸쳐 전체 순환)
    scheduler.add_job(
        fetch_and_update, 
        'interval', 
        minutes=1, 
        start_date=next_sync_time,
        max_instances=1, 
        coalesce=True
    )
    
    # 3. 1분 봉 저장
    scheduler.add_job(
        record_minute_candle, 
        'interval', 
        minutes=1, 
        start_date=next_sync_time
    )
    
    # 4. 틱 생성 엔진 구동
    run_ticks()
    
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
