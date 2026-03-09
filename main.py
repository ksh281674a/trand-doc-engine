import time
import os
import json
import random
import numpy as np
from datetime import datetime, timedelta
import pytz
import urllib.parse
import requests  # 🌟 말썽쟁이 라이브러리 대신 안정적인 직접 통신 사용
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
# 2. 34개 종목 및 위키피디아 매핑
# ---------------------------------------------------------
WIKI_MAPPING = {
    "카카오": "카카오_(기업)", "인스타그램": "인스타그램", "틱톡": "틱톡", "X (트위터)": "X_(소셜_네트워크)",
    "유튜브": "YouTube", "치지직": "치지직", "SOOP": "SOOP", "쿠팡": "쿠팡",
    "알리": "알리익스프레스", "무신사": "무신사", "테무": "테무", "네이버": "네이버",
    "구글": "구글", "다음": "다음_(포털_사이트)", "MS (Bing)": "빙_(검색_엔진)", "배달의민족": "배달의민족",
    "쿠팡이츠": "쿠팡이츠", "요기요": "요기요", "유튜브 뮤직": "유튜브_뮤직", "멜론": "멜론_(서비스)",
    "애플뮤직": "애플_뮤직", "라이엇": "라이엇_게임즈", "스팀": "스팀_(서비스)", "넥슨": "넥슨",
    "넷플릭스": "넷플릭스", "티빙": "티빙", "쿠팡플레이": "쿠팡플레이", "왓챠": "왓챠",
    "네이버웹툰": "네이버_웹툰", "카카오페이지": "카카오페이지", "하이브": "하이브_(기업)", "SM": "SM_엔터테인먼트",
    "YG": "YG_엔터테인먼트", "JYP": "JYP_엔터테인먼트"
}

TICKER_KEYS = list(WIKI_MAPPING.keys())
ohlc_buffer = {}

# ---------------------------------------------------------
# 3. 데이터 엔진 (수직 점프 방지 및 0.5~1.5s 랜덤 진동)
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
                remaining_sec = max(60, 420 - elapsed_sec) 
                distance = target - current
                ideal_step = distance / remaining_sec
                
                reverse_prob = 0.45 - min(0.35, abs(distance) * 10)
                
                if random.random() > reverse_prob:
                    speed_boost = 1.8 + min(2.2, abs(distance) * 15)
                    move = ideal_step * random.uniform(speed_boost * 0.8, speed_boost * 1.2)
                else:
                    move = -ideal_step * random.uniform(0.7, 1.5)
                
                # 🌟 [안정 장치 유지] 캔들 찢어짐 방지 보폭 제한
                move = np.clip(move, -0.0012, 0.0012)
                
                shiver = np.random.normal(0, 0.0011) 
                
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {'open': current, 'high': current, 'low': current, 'close': current}
                
                current_candle_open = ohlc_buffer[ticker]['open']
                wick_pressure = -(current - current_candle_open) * 0.1
                
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
# 4. 위키피디아 직접 통신 수집 로직 (에러 원천 차단)
# ---------------------------------------------------------
def fetch_and_update():
    now = datetime.now(KST)
    now_ts = int(time.time())
    
    group_idx = now.minute % 7
    start_idx = group_idx * 5
    end_idx = min(start_idx + 5, 34)
    current_group_tickers = TICKER_KEYS[start_idx:end_idx]
    
    if not current_group_tickers: return

    print(f"\n──────────────── 그룹 {group_idx} 위키 수집 시작 ────────────────")
    # 위키피디아 API 날짜 포맷 (YYYYMMDD00)
    target_date = (now - timedelta(days=1)).strftime('%Y%m%d00')
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    for ticker in current_group_tickers:
        try:
            wiki_title = WIKI_MAPPING[ticker]
            # 한국어 위키백과 URL에 맞게 한글 인코딩
            encoded_title = urllib.parse.quote(wiki_title, safe='')
            
            # 🌟 외부 라이브러리 없이 위키피디아 API와 직접 통신
            url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/ko.wikipedia/all-access/all-agents/{encoded_title}/daily/{target_date}/{target_date}"
            
            response = requests.get(url, headers=headers, timeout=5)
            
            if response.status_code == 200:
                json_data = response.json()
                if 'items' in json_data and len(json_data['items']) > 0:
                    current_score = float(json_data['items'][0]['views'])
                else:
                    current_score = 0.0
            else:
                # 404 에러 등 문서가 없거나 실패 시
                print(f" ❌ {ticker.ljust(10)} 위키 API 응답 에러: {response.status_code}")
                continue
            
            ref = db.reference(f'chart_data/trends/{ticker}')
            data = ref.get() or {}
            
            # [동적 점수 반영] baseline이 0이면 첫 수집값을 기준으로 설정
            baseline = data.get('baseline', 0)
            if baseline == 0:
                baseline = current_score
            
            target_yield = round((current_score - baseline) * 0.005 + random.uniform(-0.0005, 0.0005), 5)
            
            ref.update({
                'baseline': baseline,
                'last_score': current_score, 
                'target_yield': target_yield,
                'last_update_ts': now_ts
            })
            print(f" ✅ {ticker.ljust(10)}: {target_yield * 100:+.2f}% (조회수: {int(current_score)})")
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
    print("🚀 Firebase 초기화 중... (기준점 0으로 시작)")
    for ticker in TICKER_KEYS:
        ref = db.reference(f'chart_data/trends/{ticker}')
        if not ref.get():
            ref.set({
                'baseline': 0, 'last_score': 0, 'target_yield': 0.0, 
                'current_yield': 0.0, 'last_update_ts': int(time.time())
            })

# ---------------------------------------------------------
# 5. 스케줄러 설정
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks():
    generate_ticks()
    # 🌟 랜덤 들썩임 주기 (0.5초 ~ 1.5초) 유지
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
