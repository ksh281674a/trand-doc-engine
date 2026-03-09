import time
import os
import json
import random
import numpy as np
from datetime import datetime, timedelta
import pytz
import urllib.parse
import requests
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
# 2. 34개 종목 매핑
# ---------------------------------------------------------
SEARCH_MAPPING = {
    "카카오": "카카오", "인스타그램": "인스타그램", "틱톡": "틱톡", "X (트위터)": "트위터",
    "유튜브": "유튜브", "치지직": "치지직", "SOOP": "SOOP", "쿠팡": "쿠팡",
    "알리": "알리익스프레스", "무신사": "무신사", "테무": "테무", "네이버": "네이버",
    "구글": "구글", "다음": "다음 포털", "MS (Bing)": "마이크로소프트 빙", "배달의민족": "배달의민족",
    "쿠팡이츠": "쿠팡이츠", "요기요": "요기요", "유튜브 뮤직": "유튜브 뮤직", "멜론": "멜론 노래",
    "애플뮤직": "애플뮤직", "라이엇": "라이엇게임즈", "스팀": "스팀 게임", "넥슨": "넥슨",
    "넷플릭스": "넷플릭스", "티빙": "티빙", "쿠팡플레이": "쿠팡플레이", "왓챠": "왓챠",
    "네이버웹툰": "네이버웹툰", "카카오페이지": "카카오페이지", "하이브": "하이브", "SM": "SM엔터테인먼트",
    "YG": "YG엔터테인먼트", "JYP": "JYP엔터테인먼트"
}

TICKER_KEYS = list(SEARCH_MAPPING.keys())
ohlc_buffer = {}

# ---------------------------------------------------------
# 3. 데이터 엔진 (꼬리 억제 및 부드러운 수렴)
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
                
                elapsed_sec = now_ts - last_update_ts
                remaining_sec = max(20, 420 - elapsed_sec) 
                distance = target - current
                
                # 수렴 속도 상향 (ideal_step)
                ideal_step = distance / remaining_sec
                
                # 🌟 [꼬리 억제] 진동폭과 노이즈 대폭 감소
                volatility = 0.00005 + abs(distance) * 0.01 
                
                if random.random() < 0.60: # 정방향 확률 살짝 상향
                    move = ideal_step * random.uniform(1.2, 4.0) + np.random.normal(0, volatility)
                else:
                    move = -ideal_step * random.uniform(0.5, 1.5) + np.random.normal(0, volatility * 0.5)
                
                # 급격한 찢어짐 방지 (캡핑)
                move = np.clip(move, -0.004, 0.004)
                
                # 🌟 [꼬리 억제] 미세 진동 최소화
                shiver = np.random.normal(0, 0.00005) 
                
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': current, 'high': current, 'low': current, 'close': current}
                
                current_candle_open = ohlc_buffer[ticker]['open']
                # 🌟 [꼬리 억제] 꼬리 압력 완화
                wick_pressure = -(current - current_candle_open) * 0.08
                
                next_tick = round(current + move + shiver + wick_pressure, 6)
                updates_trends[f'{ticker}/current_yield'] = next_tick
                
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
        for ticker in TICKER_KEYS:
            candle = ohlc_buffer.get(ticker)
            if candle:
                db.reference(f'chart_data/chart_history/{ticker}/1m').push({
                    'time': ts, 'open': candle['open'], 'high': candle['high'], 'low': candle['low'], 'close': candle['close']
                })
                ohlc_buffer[ticker] = {'open': candle['close'], 'high': candle['close'], 'low': candle['close'], 'close': candle['close']}
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")

# ---------------------------------------------------------
# 4. 네이버 실시간 검색 API (🌟 평균 증가량 대비 하락 로직 적용)
# ---------------------------------------------------------
def fetch_and_update():
    now = datetime.now(KST)
    now_ts = int(time.time())
    
    group_idx = now.minute % 7
    start_idx = group_idx * 5
    end_idx = min(start_idx + 5, 34)
    current_group_tickers = TICKER_KEYS[start_idx:end_idx]
    
    if not current_group_tickers: return

    print(f"\n──────────────── 그룹 {group_idx} 네이버 수집 시작 ────────────────")
    
    headers = {"X-Naver-Client-Id": "0G9LeMqi2n9OQTmH0ueC", "X-Naver-Client-Secret": "6tgdSvlfjA"}
    
    for ticker in current_group_tickers:
        try:
            url = f"https://openapi.naver.com/v1/search/blog.json?query={urllib.parse.quote(SEARCH_MAPPING[ticker])}&display=1"
            response = requests.get(url, headers=headers, timeout=5)
            
            if response.status_code == 200:
                current_score = float(response.json().get('total', 0))
                ref = db.reference(f'chart_data/trends/{ticker}')
                data = ref.get() or {}
                
                baseline = data.get('baseline', current_score)
                avg_diff = data.get('avg_diff', 0.0) # 🌟 평균 증가량 학습 데이터
                
                diff = current_score - baseline
                
                # 🌟 [요구사항] 평균 증가량 대비 하락 로직
                # 이동 평균 업데이트 (최근 데이터 25% 반영)
                if avg_diff == 0:
                    new_avg = float(diff)
                else:
                    new_avg = (avg_diff * 0.75) + (diff * 0.25)
                
                if diff > new_avg:
                    # 평균보다 많이 올라오면 상승 (증가폭에 따른 보너스 강화)
                    target_yield = 0.005 + (diff * 0.0006) 
                elif diff < new_avg and diff > 0:
                    # 올라오긴 했는데 평균보다 못하면 하락 (조정)
                    target_yield = -0.012 + (diff * 0.0002)
                elif diff == 0:
                    # 증가가 아예 없으면 관심 급락으로 하락
                    target_yield = -random.uniform(0.015, 0.025)
                else:
                    # 데이터 이상치 등 마이너스 시 하락
                    target_yield = -0.020
                
                target_yield = float(np.clip(target_yield, -0.30, 0.30)) # 최대 30% 제한
                
                ref.update({
                    'baseline': current_score,
                    'last_score': current_score,
                    'avg_diff': new_avg, # 평균값 저장
                    'target_yield': target_yield,
                    'last_update_ts': now_ts
                })
                print(f" ✅ {ticker.ljust(10)}: {target_yield * 100:>+6.2f}% (찐증가:{int(diff):+} | 평균:{int(new_avg)})")
                time.sleep(0.1)
                
        except Exception as e:
            print(f" ❌ {ticker.ljust(10)} 에러: {e}")

def daily_reset():
    print(f"\n🕛 [자정 리셋]")
    try:
        ref = db.reference('chart_data/trends')
        all_trends = ref.get()
        if not all_trends: return
        updates = {}
        for ticker in TICKER_KEYS:
            data = all_trends.get(ticker, {})
            last_score = data.get('last_score', 0)
            updates[f'{ticker}/baseline'] = last_score
            updates[f'{ticker}/avg_diff'] = 0.0 # 평균 초기화
            updates[f'{ticker}/target_yield'] = 0.0
            updates[f'{ticker}/current_yield'] = 0.0
            updates[f'{ticker}/last_update_ts'] = int(time.time())
            ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
        db.reference('chart_data/trends').update(updates)
    except Exception as e: print(f"❌ 리셋 에러: {e}")

def initialize_app():
    print("🚀 엔진 가동 (꼬리 억제 및 평균 학습 모드)")
    updates = {}
    now_ts = int(time.time())
    for ticker in TICKER_KEYS:
        updates[ticker] = {
            'baseline': 0, 'last_score': 0, 'avg_diff': 0.0, 
            'target_yield': 0.0, 'current_yield': 0.0, 'last_update_ts': now_ts
        }
        ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
    db.reference('chart_data/trends').set(updates)

# ---------------------------------------------------------
# 5. 스케줄러 실행
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks_wrapper():
    generate_ticks()
    # 🌟 틱 생성 주기 랜덤화 (0.6초 ~ 1.3초)
    next_delay = random.uniform(0.6, 1.3)
    scheduler.add_job(run_ticks_wrapper, 'date', run_date=datetime.now(KST) + timedelta(seconds=next_delay))

if __name__ == "__main__":
    initialize_app()
    scheduler.add_job(fetch_and_update, 'interval', minutes=1)
    scheduler.add_job(record_minute_candle, 'interval', minutes=1)
    scheduler.add_job(daily_reset, 'cron', hour=0, minute=0)
    run_ticks_wrapper()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
