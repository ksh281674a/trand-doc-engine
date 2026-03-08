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
    "카카오": 42, "인스타그램": 55, "틱톡": 48, "X (트위터)": 50,
    "유튜브": 89, "치지직": 68, "SOOP": 52, "쿠팡": 78,
    "알리": 74, "무신사": 65, "테무": 72, "네이버": 85,
    "구글": 92, "다음": 35, "MS (Bing)": 28, "배달의민족": 62,
    "쿠팡이츠": 45, "요기요": 30, "유튜브 뮤직": 58, "멜론": 52,
    "애플뮤직": 35, "라이엇": 45, "스팀": 42, "넥슨": 48,
    "넷플릭스": 70, "티빙": 58, "쿠팡플레이": 45, "왓챠": 25,
    "네이버웹툰": 55, "카카오페이지": 40, "하이브": 48, "SM": 38,
    "YG": 35, "JYP": 32
}

ohlc_buffer = {}

# ---------------------------------------------------------
# 💡 [신규 로직] 0.5초 ~ 2.0초 랜덤 스케줄러
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def schedule_next_tick():
    """다음 틱 생성 시간을 0.5초 ~ 2초 사이로 랜덤하게 예약합니다."""
    delay = random.uniform(0.5, 2.0)
    run_time = datetime.now(KST) + timedelta(seconds=delay)
    scheduler.add_job(generate_ticks, 'date', run_date=run_time)

# ---------------------------------------------------------
# 3. 데이터 엔진 (꼬리 확률 감소 + 수렴 강화)
# ---------------------------------------------------------
def generate_ticks():
    try:
        all_trends = db.reference('chart_data/trends').get()
        if not all_trends:
            return
        
        updates_trends = {}
        updates_live = {}
        
        for ticker, data in all_trends.items():
            try:
                target = data.get('target_yield', 0.0)
                current = data.get('current_yield', 0.0)
                
                # 1. 수렴 (목표치로 당기는 힘을 15%로 상향하여 빠르게 붙게 만듦)
                pull = (target - current) * 0.15
                
                # 2. 랜덤 노이즈 (표준편차를 0.0005로 확 줄여서 꼬리가 길어질 확률 극감)
                noise = np.random.normal(0, 0.0005)
                
                # 3. 극단적 변동 억제 (어떤 상황에서도 노이즈가 +- 0.0025를 넘지 못하게 가위로 자름)
                noise = np.clip(noise, -0.0025, 0.0025)
                
                # 최종 가격 = 현재가 + 목표로 가는 힘 + 제한된 노이즈
                next_tick = round(current + pull + noise, 5)
                
                updates_trends[f'{ticker}/current_yield'] = next_tick
                updates_live[f'{ticker}/current_price'] = next_tick
                
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': next_tick, 'high': next_tick, 'low': next_tick, 'close': next_tick}
                else:
                    ohlc_buffer[ticker]['high'] = max(ohlc_buffer[ticker]['high'], next_tick)
                    ohlc_buffer[ticker]['low'] = min(ohlc_buffer[ticker]['low'], next_tick)
                    ohlc_buffer[ticker]['close'] = next_tick
                    
            except: continue
                
        if updates_trends:
            db.reference('chart_data/trends').update(updates_trends)
        if updates_live:
            db.reference('chart_data/live_data').update(updates_live)

    except Exception as e:
        print(f"❌ generate_ticks 실패: {e}")
    finally:
        # 이번 틱 연산이 끝나면, 다음 틱을 랜덤한 시간 뒤에 실행하도록 스스로 예약
        schedule_next_tick()

def record_minute_candle():
    try:
        now_utc = datetime.now(pytz.utc).replace(second=0, microsecond=0)
        ts = int(now_utc.timestamp())
        
        if not ohlc_buffer: return

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
                ohlc_buffer[ticker] = {
                    'open': candle['close'], 'high': candle['close'], 
                    'low': candle['close'], 'close': candle['close']
                }
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
            target_yield = round((current_score - baseline) * 0.001, 5)
            
            ref.update({'last_score': current_score, 'target_yield': target_yield})
            print(f" ✅ {ticker}: {target_yield:+.2f}%")
        except Exception as e:
            print(f" ❌ {ticker} 수집 실패: {e}")
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
# 5. 스케줄러 실행 세팅
# ---------------------------------------------------------
# 기존 interval(2초) 틱 생성 스케줄러는 제거하고 랜덤 틱 호출을 시작합니다.
schedule_next_tick()

scheduler.add_job(record_minute_candle, 'cron', second=0)

now_kst = datetime.now(KST)
next_minute = (now_kst + timedelta(minutes=1)).replace(second=0, microsecond=0)
scheduler.add_job(fetch_and_update, 'interval', minutes=7, next_run_time=next_minute, max_instances=1, coalesce=True)

scheduler.add_job(daily_midnight_reset, 'cron', hour=0, minute=0)

if __name__ == "__main__":
    initialize_app()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
