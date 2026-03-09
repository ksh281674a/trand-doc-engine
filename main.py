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
# 3. 틱 엔진
# ---------------------------------------------------------
def generate_ticks():
    try:
        ref = db.reference('chart_data/trends')
        all_trends = ref.get()
        if not all_trends:
            return

        updates_trends = {}
        updates_live = {}
        now_ts = int(time.time())

        for ticker, data in all_trends.items():
            try:
                target  = data.get('target_yield', 0.0)
                current = data.get('current_yield', 0.0)
                last_update_ts = data.get('last_update_ts', now_ts - 600)

                elapsed_sec   = now_ts - last_update_ts
                remaining_sec = max(5, 600 - elapsed_sec)   # 10분(600초) 수렴
                distance      = target - current

                convergence_ratio    = (600 - remaining_sec) / 600
                convergence_strength = 1.0 + convergence_ratio * 2.0
                ideal_step = (distance / remaining_sec) * convergence_strength

                volatility = 0.00006 + abs(distance) * 0.002

                rand = random.random()
                if abs(distance) < 0.0005:
                    # 목표 근처: 작은 노이즈만
                    move = np.random.normal(0, volatility * 0.3)
                elif rand < 0.65:
                    # 65% 목표 방향
                    move = ideal_step * random.uniform(0.9, 1.8) + np.random.normal(0, volatility)
                elif rand < 0.85:
                    # 20% 약한 반대 방향 (자연스러운 되돌림)
                    move = -ideal_step * random.uniform(0.2, 0.6) + np.random.normal(0, volatility)
                else:
                    # 15% 횡보 노이즈
                    move = np.random.normal(0, volatility * 0.6)

                max_step = max(0.0002, abs(distance) * 0.25)
                move = float(np.clip(move, -max_step, max_step))

                # target 초과 방지
                projected = current + move
                if distance > 0 and projected > target + 0.0002:
                    move = (target - current) * 0.98
                elif distance < 0 and projected < target - 0.0002:
                    move = (target - current) * 0.98

                shiver    = np.random.normal(0, 0.00004)
                next_tick = round(current + move + shiver, 6)

                updates_trends[f'{ticker}/current_yield'] = next_tick

                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {
                        'open': current, 'high': current,
                        'low':  current, 'close': current
                    }

                buf = ohlc_buffer[ticker]
                candle_open = buf['open']
                max_wick    = abs(candle_open) * 0.002 + 0.0004
                buf['high']  = max(buf['high'],  min(next_tick, candle_open + max_wick))
                buf['low']   = min(buf['low'],   max(next_tick, candle_open - max_wick))
                buf['close'] = next_tick

                updates_live[ticker] = {
                    'time':  now_ts,
                    'open':  buf['open'],  'high': buf['high'],
                    'low':   buf['low'],   'close': buf['close']
                }

            except Exception:
                continue

        if updates_trends:
            db.reference('chart_data/trends').update(updates_trends)
        if updates_live:
            db.reference('chart_data/live_data').update(updates_live)

    except Exception as e:
        print(f"❌ generate_ticks 에러: {e}")


def record_minute_candle():
    """매 1분: 분봉 저장 후 다음 open = 직전 close (갭 없음)"""
    try:
        now_utc = datetime.now(pytz.utc).replace(second=0, microsecond=0)
        ts = int(now_utc.timestamp())
        if not ohlc_buffer:
            return
        for ticker in TICKER_KEYS:
            candle = ohlc_buffer.get(ticker)
            if candle:
                db.reference(f'chart_data/chart_history/{ticker}/1m').push({
                    'time': ts,
                    'open':  candle['open'],  'high': candle['high'],
                    'low':   candle['low'],   'close': candle['close']
                })
                close_price = candle['close']
                ohlc_buffer[ticker] = {
                    'open':  close_price, 'high': close_price,
                    'low':   close_price, 'close': close_price
                }
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")


# ---------------------------------------------------------
# 4. 네이버 수집: 매 1분마다 34개 전체
# ---------------------------------------------------------
def fetch_and_update():
    now_ts  = int(time.time())
    now_kst = datetime.now(KST)

    print(f"\n{'─'*52}")
    print(f"📡 [{now_kst.strftime('%H:%M:%S')}] 34개 전체 수집 시작")
    print(f"{'─'*52}")

    headers = {
        "X-Naver-Client-Id":     "0G9LeMqi2n9OQTmH0ueC",
        "X-Naver-Client-Secret": "6tgdSvlfjA"
    }

    success, fail = 0, 0

    for ticker in TICKER_KEYS:
        try:
            url = (
                f"https://openapi.naver.com/v1/search/blog.json"
                f"?query={urllib.parse.quote(SEARCH_MAPPING[ticker])}&display=1"
            )
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code != 200:
                print(f" ❌ {ticker.ljust(10)} API 에러: {response.status_code}")
                fail += 1
                continue

            current_score = float(response.json().get('total', 0))

            ref  = db.reference(f'chart_data/trends/{ticker}')
            data = ref.get() or {}
            baseline = data.get('baseline', 0)

            if baseline == 0:
                ref.update({
                    'baseline':       current_score,
                    'last_score':     current_score,
                    'target_yield':   0.0,
                    'last_update_ts': now_ts
                })
                print(f" 🔄 {ticker.ljust(10)}: 최초 세팅 ({int(current_score):,}건)")
                time.sleep(0.08)
                success += 1
                continue

            diff = current_score - baseline

            if diff == 0:
                target_yield = random.choice([1, -1]) * random.uniform(0.004, 0.009)
            elif diff > 0:
                target_yield =  random.uniform(0.004, 0.007) + (diff * 0.0005)
            else:
                target_yield = -random.uniform(0.004, 0.007) + (diff * 0.0005)

            target_yield = float(np.clip(target_yield, -0.25, 0.25))

            ref.update({
                'baseline':       current_score,
                'last_score':     current_score,
                'target_yield':   target_yield,
                'last_update_ts': now_ts          # 10분 수렴 타이머 리셋
            })

            print(
                f" ✅ {ticker.ljust(10)}: {target_yield * 100:>+6.2f}%"
                f"  (전:{int(baseline):,} → 현:{int(current_score):,} | Δ{int(diff):+}건)"
            )
            time.sleep(0.08)   # 34건 × 0.08s ≈ 2.7초
            success += 1

        except Exception as e:
            print(f" ❌ {ticker.ljust(10)} 실패: {e}")
            fail += 1

    print(f"{'─'*52}")
    print(f"✔ 완료: 성공 {success} / 실패 {fail}")
    print(f"{'─'*52}\n")


# ---------------------------------------------------------
# 5. 자정 리셋
# ---------------------------------------------------------
def daily_reset():
    print(f"\n🕛 [자정 리셋] {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        ref = db.reference('chart_data/trends')
        all_trends = ref.get()
        if not all_trends:
            return
        updates = {}
        now_ts = int(time.time())
        for ticker in TICKER_KEYS:
            data = all_trends.get(ticker, {})
            updates[f'{ticker}/baseline']       = data.get('last_score', 0)
            updates[f'{ticker}/target_yield']   = 0.0
            updates[f'{ticker}/current_yield']  = 0.0
            updates[f'{ticker}/last_update_ts'] = now_ts
            ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
        db.reference('chart_data/trends').update(updates)
    except Exception as e:
        print(f"❌ 리셋 에러: {e}")


# ---------------------------------------------------------
# 6. 초기화
# ---------------------------------------------------------
def initialize_app():
    print("🚀 초기화 중...")
    updates = {}
    now_ts  = int(time.time())
    for ticker in TICKER_KEYS:
        updates[ticker] = {
            'baseline': 0, 'last_score': 0,
            'target_yield': 0.0, 'current_yield': 0.0,
            'last_update_ts': now_ts
        }
        ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
    db.reference('chart_data/trends').set(updates)


# ---------------------------------------------------------
# 7. 스케줄러
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks():
    generate_ticks()
    delay    = random.uniform(0.5, 1.5)
    next_run = datetime.now(KST) + timedelta(seconds=delay)
    scheduler.add_job(run_ticks, 'date', run_date=next_run)


if __name__ == "__main__":
    initialize_app()

    # 시작 즉시 첫 수집 실행
    print("📡 첫 수집 즉시 시작...")
    fetch_and_update()

    # 이후 다음 정각부터 1분 간격으로 반복
    now            = datetime.now(KST)
    next_sync_time = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    print(f"📡 다음 정각 수집 예정: {next_sync_time.strftime('%H:%M:%S')}")

    scheduler.add_job(
        fetch_and_update, 'interval', minutes=1,
        start_date=next_sync_time, max_instances=1, coalesce=True
    )
    scheduler.add_job(
        record_minute_candle, 'interval', minutes=1,
        start_date=next_sync_time
    )
    scheduler.add_job(daily_reset, 'cron', hour=0, minute=0, second=0)

    run_ticks()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
