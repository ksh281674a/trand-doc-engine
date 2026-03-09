import time
import os
import json
import random
import numpy as np
from datetime import datetime, timedelta
import pytz
import urllib.parse
import requests  # 🌟 네이버 통신을 위한 필수 라이브러리
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
# 2. 34개 종목 및 네이버 검색어 매핑
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
# 3. 데이터 엔진 (🌟 양봉/음봉 지그재그 수렴 로직 적용)
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
                remaining_sec = max(30, 420 - elapsed_sec) 
                distance = target - current
                ideal_step = distance / remaining_sec
                
                # 🌟 [핵심] 목표치까지 남은 거리에 비례하여 변동성(캔들 크기) 결정
                # 5% 폭등일 땐 캔들이 길쭉길쭉하게, 0.1%일 땐 잔잔하게 움직임
                volatility = 0.0002 + abs(distance) * 0.02
                
                # 🌟 [핵심] 55% 확률로 목표 방향 전진(양봉/음봉 생성), 45% 확률로 조정(반대 색상 캔들)
                # 이렇게 해야 일직선 수직 상승이 아니라 지그재그 파동을 그리며 수렴함
                if random.random() < 0.55:
                    move = ideal_step * random.uniform(1.0, 3.5) + np.random.normal(0, volatility)
                else:
                    move = -ideal_step * random.uniform(0.5, 2.0) + np.random.normal(0, volatility * 0.8)
                
                # 수직 찢어짐 방지: 거리가 멀면 보폭을 살짝 열어주되, 최대 0.3% 이상 점프 불가
                max_step = min(0.003, 0.0005 + abs(distance) * 0.01)
                move = np.clip(move, -max_step, max_step)
                
                shiver = np.random.normal(0, 0.0001) 
                
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': current, 'high': current, 'low': current, 'close': current}
                
                # 캔들 꼬리 생성 압력 (너무 한 방향으로만 쏠리지 않게 당겨줌)
                current_candle_open = ohlc_buffer[ticker]['open']
                wick_pressure = -(current - current_candle_open) * 0.15
                
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
# 4. 네이버 실시간 검색 API (퍼센트 계산 및 레버리지 로직)
# ---------------------------------------------------------
def fetch_and_update():
    now = datetime.now(KST)
    now_ts = int(time.time())
    
    group_idx = now.minute % 7
    start_idx = group_idx * 5
    end_idx = min(start_idx + 5, 34)
    current_group_tickers = TICKER_KEYS[start_idx:end_idx]
    
    if not current_group_tickers: return

    print(f"\n──────────────── 그룹 {group_idx} 네이버 실시간 수집 시작 ────────────────")
    
    naver_client_id = "0G9LeMqi2n9OQTmH0ueC"
    naver_client_secret = "6tgdSvlfjA"
    
    headers = {
        "X-Naver-Client-Id": naver_client_id,
        "X-Naver-Client-Secret": naver_client_secret
    }
    
    for ticker in current_group_tickers:
        try:
            search_query = SEARCH_MAPPING[ticker]
            url = f"https://openapi.naver.com/v1/search/blog.json?query={urllib.parse.quote(search_query)}&display=1"
            
            response = requests.get(url, headers=headers, timeout=5)
            
            if response.status_code == 200:
                json_data = response.json()
                current_score = float(json_data.get('total', 0))
            else:
                print(f" ❌ {ticker.ljust(10)} 네이버 API 에러: {response.status_code}")
                continue
            
            ref = db.reference(f'chart_data/trends/{ticker}')
            data = ref.get() or {}
            
            baseline = data.get('baseline', 0)
            if baseline == 0:
                baseline = current_score
            
            if baseline > 0:
                real_change_rate = (current_score - baseline) / baseline
                leverage = 150 
                target_yield = round((real_change_rate * leverage) + random.uniform(-0.001, 0.001), 5)
            else:
                target_yield = random.uniform(-0.001, 0.001)
            
            target_yield = float(np.clip(target_yield, -0.15, 0.15))
            
            ref.update({
                'baseline': baseline,
                'last_score': current_score, 
                'target_yield': target_yield,
                'last_update_ts': now_ts
            })
            print(f" ✅ {ticker.ljust(10)}: {target_yield * 100:+.2f}% (실시간 버즈량: {int(current_score):,}건)")
            time.sleep(0.1) 
            
        except Exception as e:
            print(f" ❌ {ticker.ljust(10)} 네트워크/통신 실패: {e}")
            continue
            
    print(f"──────────────── 그룹 {group_idx} 수집 완료 ────────────────")

def daily_reset():
    print(f"\n🕛 [자정 리셋] {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        ref = db.reference('chart_data/trends')
        all_trends = ref.get()
        if not all_trends: return
        updates = {}
        for ticker in TICKER_KEYS:
            data = all_trends.get(ticker, {})
            last_score = data.get('last_score', 0)
            updates[f'{ticker}/baseline'] = last_score
            updates[f'{ticker}/target_yield'] = 0.0
            updates[f'{ticker}/current_yield'] = 0.0
            updates[f'{ticker}/last_update_ts'] = int(time.time())
            ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
        if updates: db.reference('chart_data/trends').update(updates)
    except Exception as e: print(f"❌ 리셋 에러: {e}")

def initialize_app():
    print("🚀 Firebase 초기화 중... (기존 과거 데이터 강제 초기화 및 0점 세팅)")
    updates = {}
    now_ts = int(time.time())
    for ticker in TICKER_KEYS:
        updates[ticker] = {
            'baseline': 0, 'last_score': 0, 'target_yield': 0.0, 
            'current_yield': 0.0, 'last_update_ts': now_ts
        }
        ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
    db.reference('chart_data/trends').set(updates)

# ---------------------------------------------------------
# 5. 스케줄러 설정
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks():
    generate_ticks()
    next_run_delay = random.uniform(0.5, 1.5)
    next_run = datetime.now(KST) + timedelta(seconds=next_run_delay)
    scheduler.add_job(run_ticks, 'date', run_date=next_run)

if __name__ == "__main__":
    initialize_app()
    now = datetime.now(KST)
    next_sync_time = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    
    print(f"📡 대기 중... 첫 정각(00초) 수집 시작 시각: {next_sync_time.strftime('%H:%M:%S')}")

    scheduler.add_job(fetch_and_update, 'interval', minutes=1, start_date=next_sync_time, max_instances=1, coalesce=True)
    scheduler.add_job(record_minute_candle, 'interval', minutes=1, start_date=next_sync_time)
    scheduler.add_job(daily_reset, 'cron', hour=0, minute=0, second=0)
    
    run_ticks()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
