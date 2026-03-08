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

ohlc_buffer = {}

# ---------------------------------------------------------
# 3. 데이터 엔진 (실시간 틱 생성 및 1분봉 구성)
# ---------------------------------------------------------
def generate_ticks():
    try:
        ref = db.reference('chart_data/trends')
        all_trends = ref.get()
        
        if not all_trends:
            initialize_app()
            return
        
        updates_trends = {}
        updates_live = {}
        
        # [수정] current_ts를 1분 고정이 아닌 '초' 단위까지 가져옴 (프론트 갱신용)
        now_ts = int(time.time())
        
        for ticker, data in all_trends.items():
            try:
                target = data.get('target_yield', 0.0)
                current = data.get('current_yield', 0.0)
                
                # [수정] 데이터 변화량 및 견인력 강화 (더 역동적인 무빙)
                distance = target - current
                pull = distance * 0.08  # 견인력 강화
                noise = np.random.normal(0, 0.0012) # 노이즈 강화
                
                # 지그재그 무빙 (개미털기) 확률 및 강도 조정
                if abs(distance) > 0.002 and random.random() < 0.20:
                    noise -= np.sign(distance) * abs(distance) * 0.15
                
                # 정밀도를 6자리로 늘려 미세한 변화도 프론트가 감지하게 함
                next_tick = round(current + pull + noise, 6)
                updates_trends[f'{ticker}/current_yield'] = next_tick
                
                # OHLC 버퍼 관리
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': next_tick, 'high': next_tick, 'low': next_tick, 'close': next_tick}
                else:
                    ohlc_buffer[ticker]['high'] = max(ohlc_buffer[ticker]['high'], next_tick)
                    ohlc_buffer[ticker]['low'] = min(ohlc_buffer[ticker]['low'], next_tick)
                    ohlc_buffer[ticker]['close'] = next_tick
                
                # [중요] 실시간 데이터 패키징 (경로: chart_data/live_data/{ticker})
                # 프론트엔드가 요구하는 {time, open, high, low, close} 객체를 통째로 갱신
                updates_live[ticker] = {
                    'time': now_ts,
                    'open': ohlc_buffer[ticker]['open'],
                    'high': ohlc_buffer[ticker]['high'],
                    'low': ohlc_buffer[ticker]['low'],
                    'close': ohlc_buffer[ticker]['close']
                }
            except: continue
                
        if updates_trends:
            db.reference('chart_data/trends').update(updates_trends)
        if updates_live:
            # chart_data/live_data 하위에 각 종목 객체들이 동시에 업데이트됨
            db.reference('chart_data/live_data').update(updates_live)

    except Exception as e:
        print(f"❌ generate_ticks 에러: {e}")

def record_minute_candle():
    """1분마다 확정된 봉을 히스토리에 저장"""
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
                # 다음 분을 위해 버퍼 초기화 (이전 종가로 시작)
                ohlc_buffer[ticker] = {'open': candle['close'], 'high': candle['close'], 'low': candle['close'], 'close': candle['close']}
        print(f"✅ [{datetime.now(KST).strftime('%H:%M:%S')}] 1분 봉 저장 완료 (TS: {ts})")
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")

def fetch_and_update():
    """7분마다 구글 트렌드 점수 수집 및 목표 수익률 설정"""
    now = datetime.now(KST)
    print(f"\n📊 [수집 시작] {now.strftime('%H:%M:%S')}")
    try:
        pt = TrendReq(hl='ko-KR', tz=540, retries=3, backoff_factor=1)
        pt.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'
    except Exception as e:
        print(f"❌ 세션 실패: {e}")
        return

    for ticker in TICKERS_DATA.keys():
        loop_start = time.time()
        try:
            ref = db.reference(f'chart_data/trends/{ticker}')
            data = ref.get()
            baseline = data.get('baseline', TICKERS_DATA[ticker])
            pt.build_payload([ticker], timeframe='now 1-H')
            df = pt.interest_over_time()
            current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
            
            # 1점당 0.5% 변동으로 현실감 있게 조정 (0.007 -> 0.005)
            target_yield = round((current_score - baseline) * 0.005 + random.uniform(-0.001, 0.001), 5)
            
            ref.update({'last_score': current_score, 'target_yield': target_yield})
            # 로그 출력 시 % 단위로 보기 편하게 변경
            print(f" ✅ {ticker}: {target_yield * 100:+.2f}%")
        except: continue
        finally:
            elapsed = time.time() - loop_start
            if elapsed < 12.0: time.sleep(12.0 - elapsed)
    print(f"🏁 수집 완료")

def initialize_app():
    print("🚀 Firebase 초기화...")
    try:
        for ticker, avg in TICKERS_DATA.items():
            ref = db.reference(f'chart_data/trends/{ticker}')
            if not ref.get():
                ref.set({'baseline': avg, 'last_score': avg, 'target_yield': 0.0, 'current_yield': 0.0})
        print("✅ 초기화 완료")
    except Exception as e:
        print(f"❌ 초기화 실패: {e}")

# ---------------------------------------------------------
# 4. 스케줄러 설정
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks():
    generate_ticks()
    # 0.8초 ~ 1.5초 사이 랜덤한 주기로 틱 생성
    next_run = datetime.now(KST) + timedelta(seconds=random.uniform(0.8, 1.5))
    scheduler.add_job(run_ticks, 'date', run_date=next_run)

if __name__ == "__main__":
    initialize_app()
    # 시작 시 트렌드 수집
    fetch_and_update()
    
    # 틱 생성 엔진 구동
    run_ticks()
    
    # 1분 봉 기록 스케줄러
    scheduler.add_job(record_minute_candle, 'interval', minutes=1, start_date=datetime.now(KST).replace(second=0, microsecond=0))
    # 7분 트렌드 갱신 스케줄러
    scheduler.add_job(fetch_and_update, 'interval', minutes=7, max_instances=1, coalesce=True)
    
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
