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
# 환경 변수에 저장된 서비스 계정 키를 사용합니다.
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

# 1분 동안의 틱을 모아 OHLC를 만들기 위한 임시 저장소
ohlc_buffer = {}

# ---------------------------------------------------------
# 3. 핵심 로직: 틱 생성 및 OHLC 갱신
# ---------------------------------------------------------
def generate_ticks():
    """2초마다 실행: 실시간 수익률 계산 및 OHLC 최고/최저가 갱신"""
    all_trends = db.reference('trends').get()
    if not all_trends: return
    
    updates_trends = {}
    updates_live = {}
    
    for ticker, data in all_trends.items():
        try:
            target = data.get('target_yield', 0.0)
            current = data.get('current_yield', 0.0)
            
            # 변동성 부여 (수익률 단위: 0.0000)
            noise = np.random.normal(0, 0.008) 
            pull = (target - current) * 0.06
            next_tick = round(current + noise + pull, 5)
            
            # 1. trends 노드 업데이트 준비 (내부 연산용)
            updates_trends[f'{ticker}/current_yield'] = next_tick
            
            # 2. live_data 노드 업데이트 준비 (프론트엔드 실시간 선 긋기용)
            updates_live[f'{ticker}/current_price'] = next_tick
            
            # 3. OHLC 버퍼 업데이트 (봉 생성용)
            if ticker not in ohlc_buffer:
                ohlc_buffer[ticker] = {'open': next_tick, 'high': next_tick, 'low': next_tick, 'close': next_tick}
            else:
                ohlc_buffer[ticker]['high'] = max(ohlc_buffer[ticker]['high'], next_tick)
                ohlc_buffer[ticker]['low'] = min(ohlc_buffer[ticker]['low'], next_tick)
                ohlc_buffer[ticker]['close'] = next_tick
                
        except Exception as e:
            continue
            
    if updates_trends:
        db.reference('trends').update(updates_trends)
    if updates_live:
        db.reference('live_data').update(updates_live)

def record_minute_candle():
    """매 분 00초 실행: 1분간의 데이터를 chart_history에 확정 저장"""
    # UTC 유닉스 타임스탬프 (초 단위 정수)
    now_utc = datetime.now(pytz.utc).replace(second=0, microsecond=0)
    ts = int(now_utc.timestamp())
    
    for ticker in TICKERS_DATA.keys():
        candle = ohlc_buffer.get(ticker)
        if candle:
            # 규격 가이드: chart_history/{ticker}/1m 경로에 push
            db.reference(f'chart_history/{ticker}/1m').push({
                'time': ts,
                'open': candle['open'],
                'high': candle['high'],
                'low': candle['low'],
                'close': candle['close']
            })
            
            # 다음 분을 위한 버퍼 초기화 (현재 종가가 다음 시가가 됨)
            ohlc_buffer[ticker] = {
                'open': candle['close'], 
                'high': candle['close'], 
                'low': candle['close'], 
                'close': candle['close']
            }
    print(f"📦 [봉 확정] {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC - 1분봉 저장 완료")

# ---------------------------------------------------------
# 4. 외부 데이터 수집 (구글 트렌드)
# ---------------------------------------------------------
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0 Safari/537.36'
]

def fetch_and_update():
    now = datetime.now(KST)
    print(f"\n📊 [트렌드 수집] {now.strftime('%H:%M:%S')}")

    try:
        pt = TrendReq(hl='ko-KR', tz=540, retries=3, backoff_factor=1)
        pt.headers['User-Agent'] = random.choice(USER_AGENTS)
    except Exception as e:
        print(f"❌ 세션 연결 실패: {e}")
        return

    for ticker in TICKERS_DATA.keys():
        start_time = time.time()
        try:
            ref = db.reference(f'trends/{ticker}')
            data = ref.get()
            baseline = data.get('baseline', TICKERS_DATA[ticker])
            
            pt.build_payload([ticker], timeframe='now 1-H')
            df = pt.interest_over_time()
            current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
            
            # 목표 수익률 계산 (트렌드 점수 차이 기반)
            target_yield = round((current_score - baseline) * 0.005, 5) 
            
            ref.update({'last_score': current_score, 'target_yield': target_yield})
            print(f" ✅ {ticker}: {target_yield:+.4f}%")
        except:
            print(f" ❌ {ticker} 수집 건너뜀")
        finally:
            time.sleep(max(0, 12.0 - (time.time() - start_time)))

def daily_midnight_reset():
    """자정 리셋: 오늘의 종가를 내일의 기준가(Baseline)로 설정"""
    all_trends = db.reference('trends').get()
    if not all_trends: return
    for ticker in TICKERS_DATA.keys():
        data = all_trends.get(ticker, {})
        last_score = data.get('last_score', TICKERS_DATA[ticker])
        db.reference(f'trends/{ticker}').update({
            'baseline': last_score, 'target_yield': 0.0, 'current_yield': 0.0
        })

def initialize_app():
    print("🚀 Firebase 초기화 및 경로 점검...")
    for ticker, avg in TICKERS_DATA.items():
        ref = db.reference(f'trends/{ticker}')
        if not ref.get():
            ref.set({'baseline': avg, 'last_score': avg, 'target_yield': 0.0, 'current_yield': 0.0})
    print("✅ 데이터 엔진 준비 완료!")

# ---------------------------------------------------------
# 5. 스케줄러 설정
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

# 1. 2초마다 틱 생성 (차트 움직임)
scheduler.add_job(generate_ticks, 'interval', seconds=2)

# 2. 1분마다 OHLC 봉 저장 (프론트엔드 연동 핵심)
scheduler.add_job(record_minute_candle, 'cron', second=0)

# 3. 7분마다 트렌드 수집
scheduler.add_job(fetch_and_update, 'interval', minutes=7, next_run_time=datetime.now(KST) + timedelta(seconds=10))

# 4. 매일 자정 리셋
scheduler.add_job(daily_midnight_reset, 'cron', hour=0, minute=0)

if __name__ == "__main__":
    initialize_app()
    scheduler.start()
    # Flask 서버 실행 (Render 등 배포 환경용)
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
