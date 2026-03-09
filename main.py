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
# 3. 데이터 엔진 (수렴도 강화 및 꼬리 확률 제어)
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
                # 7분(420초) 주기에 맞춘 남은 시간 계산
                remaining_sec = max(10, 420 - elapsed_sec) 
                distance = target - current
                
                # 🌟 [수렴 강화] 목표치까지 남은 시간에 따른 보폭 계산
                # 시간이 얼마 안 남았을수록(remaining_sec이 작을수록) 보폭을 키워 수렴 보장
                ideal_step = distance / remaining_sec
                
                # 🌟 [꼬리 제어] 모든 봉에 꼬리가 생기지 않도록 변동성 확률 제어
                # 30% 확률로만 노이즈(꼬리 원인) 발생
                if random.random() < 0.3:
                    volatility = 0.0001 + abs(distance) * 0.01
                    shiver = np.random.normal(0, 0.00005)
                else:
                    volatility = 0.0
                    shiver = 0.0

                # 거리가 멀면 더 공격적으로 이동, 가까우면(±0.25% 이내) 미세 조정
                if abs(distance) > 0.0025:
                    move = ideal_step * random.uniform(1.5, 3.0)
                else:
                    move = ideal_step * random.uniform(0.8, 1.2)
                
                # 급격한 수직 이동 방지
                move = np.clip(move, -0.005, 0.005)
                
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': current, 'high': current, 'low': current, 'close': current}
                
                # 🌟 [꼬리 제어] 꼬리 압력을 20% 확률로만 적용
                if random.random() < 0.2:
                    current_candle_open = ohlc_buffer[ticker]['open']
                    wick_pressure = -(current - current_candle_open) * 0.1
                else:
                    wick_pressure = 0
                
                next_tick = round(current + move + shiver + wick_pressure, 6)
                
                # 다음 사이클을 위해 current_yield 업데이트 준비
                updates_trends[f'{ticker}/current_yield'] = next_tick
                
                # OHLC 갱신
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
                # 다음 봉 시작가 설정
                ohlc_buffer[ticker] = {'open': candle['close'], 'high': candle['close'], 'low': candle['close'], 'close': candle['close']}
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")

# ---------------------------------------------------------
# 4. 네이버 실시간 검색 API (🌟 밸런싱된 등락 로직)
# ---------------------------------------------------------
def fetch_and_update():
    now = datetime.now(KST)
    now_ts = int(time.time())
    
    group_idx = now.minute % 7
    start_idx = group_idx * 5
    end_idx = min(start_idx + 5, 34)
    current_group_tickers = TICKER_KEYS[start_idx:end_idx]
    
    if not current_group_tickers: return

    print(f"\n──────────────── 그룹 {group_idx} 네이버 데이터 연동 ────────────────")
    
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
                avg_diff = data.get('avg_diff', 0.0) 
                
                diff = current_score - baseline
                
                # 이동 평균(EMA) 학습 (지수 가중 이동 평균 방식)
                if avg_diff == 0:
                    new_avg = float(diff)
                else:
                    new_avg = (avg_diff * 0.8) + (diff * 0.2)
                
                # 🌟 [요구사항 반영] 등락 밸런스 조정
                if diff > new_avg and diff > 0:
                    # 평균보다 높으면 확실한 상승 (카카오 5% 등 목표치 달성 용이하게 설정)
                    target_yield = 0.015 + (diff * 0.005) 
                elif diff > 0:
                    # 증가하긴 했으나 평균 이하라면 약보합/조정
                    target_yield = random.uniform(-0.005, 0.008)
                else:
                    # 증가량 0이면 서서히 하락 (기존 폭락 방지 위해 하락폭 축소)
                    target_yield = -random.uniform(0.002, 0.008)
                
                # 최대 수익률 30% 제한
                target_yield = float(np.clip(target_yield, -0.30, 0.30)) 
                
                ref.update({
                    'baseline': current_score,
                    'last_score': current_score,
                    'avg_diff': new_avg,
                    'target_yield': target_yield,
                    'last_update_ts': now_ts
                })
                print(f" ✅ {ticker.ljust(10)}: {target_yield * 100:>+6.2f}% (증가:{int(diff):+} | 평균:{int(new_avg)})")
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
            updates[f'{ticker}/avg_diff'] = 0.0
            updates[f'{ticker}/target_yield'] = 0.0
            updates[f'{ticker}/current_yield'] = 0.0
            updates[f'{ticker}/last_update_ts'] = int(time.time())
            ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
        db.reference('chart_data/trends').update(updates)
    except Exception as e: print(f"❌ 리셋 에러: {e}")

def initialize_app():
    print("🚀 트렌드 엔진 초기화 완료")
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
    # 틱 간격 0.7초 ~ 1.2초 사이 랜덤
    next_delay = random.uniform(0.7, 1.2)
    scheduler.add_job(run_ticks_wrapper, 'date', run_date=datetime.now(KST) + timedelta(seconds=next_delay))

if __name__ == "__main__":
    initialize_app()
    scheduler.add_job(fetch_and_update, 'interval', minutes=1)
    scheduler.add_job(record_minute_candle, 'interval', minutes=1)
    scheduler.add_job(daily_reset, 'cron', hour=0, minute=0)
    run_ticks_wrapper()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
