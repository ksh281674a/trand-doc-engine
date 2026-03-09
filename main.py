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
from pytrends.request import TrendReq

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

# 각 ticker의 현재 분봉 내 틱 방향 상태 (오르락내리락용)
tick_state = {}

def generate_ticks():
    try:
        ref = db.reference('chart_data/trends')
        all_trends = ref.get()
        if not all_trends:
            return

        updates_trends = {}
        updates_live   = {}
        now_ts = int(time.time())

        for ticker, data in all_trends.items():
            try:
                target  = data.get('target_yield', 0.0)
                current = data.get('current_yield', 0.0)
                last_update_ts = data.get('last_update_ts', now_ts - 600)

                elapsed_sec   = now_ts - last_update_ts
                remaining_sec = max(5, 600 - elapsed_sec)
                distance      = target - current

                # 수렴 강도
                convergence_ratio    = (600 - remaining_sec) / 600
                convergence_strength = 1.0 + convergence_ratio * 2.0
                ideal_step = (distance / remaining_sec) * convergence_strength

                # ── 틱 방향 상태 (오르락내리락 자연스럽게) ──────────────
                state = tick_state.get(ticker, {'counter': 0, 'dir': 1})
                counter = state['counter']
                cur_dir = state['dir']

                # 1~3틱마다 방향 전환 (매우 자주, 격렬하게)
                if counter <= 0:
                    rand = random.random()
                    if distance > 0:
                        cur_dir = 1 if rand < 0.62 else -1
                    elif distance < 0:
                        cur_dir = -1 if rand < 0.62 else 1
                    else:
                        cur_dir = 1 if rand < 0.50 else -1
                    # 주 방향: 1~3틱, 반대 방향: 1~2틱
                    if (cur_dir > 0 and distance > 0) or (cur_dir < 0 and distance < 0):
                        counter = random.randint(1, 3)
                    else:
                        counter = random.randint(1, 2)
                    tick_state[ticker] = {'counter': counter, 'dir': cur_dir}
                else:
                    tick_state[ticker]['counter'] = counter - 1

                # ── 이동량 계산 ──────────────────────────────────────────
                volatility = 0.00035 + abs(distance) * 0.008

                if abs(distance) < 0.0003:
                    move = np.random.normal(0, volatility * 1.2)
                else:
                    base  = abs(ideal_step) * random.uniform(1.5, 3.5)
                    noise = np.random.normal(0, volatility)
                    move  = cur_dir * base + noise

                # 한 틱 최대 이동 제한
                max_step = max(0.0008, abs(distance) * 0.40)
                move = float(np.clip(move, -max_step, max_step))

                # target 초과 방지
                projected = current + move
                if distance > 0 and projected > target + 0.001:
                    move = (target - current) * 0.95
                elif distance < 0 and projected < target - 0.001:
                    move = (target - current) * 0.95

                next_tick = round(current + move, 6)
                updates_trends[f'{ticker}/current_yield'] = next_tick

                # ── OHLC 버퍼 업데이트 ───────────────────────────────────
                if ticker not in ohlc_buffer:
                    ohlc_buffer[ticker] = {
                        'open': current, 'high': current,
                        'low':  current, 'close': current
                    }

                buf = ohlc_buffer[ticker]
                # high/low: 꼬리 제한 없이 실제 틱 그대로 반영
                buf['high']  = max(buf['high'],  next_tick)
                buf['low']   = min(buf['low'],   next_tick)
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
                # 다음 분봉 open = 직전 close (점프 없음)
                close_price = candle['close']
                ohlc_buffer[ticker] = {
                    'open':  close_price, 'high': close_price,
                    'low':   close_price, 'close': close_price
                }
                # 틱 방향 상태 초기화 (새 분봉 시작)
                tick_state[ticker] = {'counter': 0, 'dir': 1}
    except Exception as e:
        print(f"❌ record_minute_candle 에러: {e}")




# ---------------------------------------------------------
# 4. 구글 트렌드 수집
# ---------------------------------------------------------
def fetch_google_trends(keywords):
    """
    keywords: list of str (한국어 검색어)
    반환: {keyword: score(0~100)}
    pytrends는 5개씩 묶어서 요청
    """
    result = {}
    try:
        pytrends = TrendReq(hl='ko', tz=540, timeout=(10, 25))
        for i in range(0, len(keywords), 5):
            chunk = keywords[i:i+5]
            try:
                pytrends.build_payload(chunk, timeframe='now 1-H', geo='KR')
                df = pytrends.interest_over_time()
                if df is not None and not df.empty:
                    for kw in chunk:
                        result[kw] = float(df[kw].iloc[-1]) if kw in df.columns else 0.0
                else:
                    for kw in chunk:
                        result[kw] = 0.0
                time.sleep(random.uniform(1.5, 3.0))
            except Exception as e:
                print(f"  ⚠ 구글트렌드 청크 실패 {chunk}: {e}")
                for kw in chunk:
                    result[kw] = 0.0
    except Exception as e:
        print(f"  ⚠ 구글트렌드 전체 실패: {e}")
    return result


# ---------------------------------------------------------
# 5. 네이버+구글 통합 수집
# ---------------------------------------------------------
def fetch_and_update():
    now_ts  = int(time.time())
    now_kst = datetime.now(KST)

    print(f"\n{'─'*52}")
    print(f"📡 [{now_kst.strftime('%H:%M:%S')}] 34개 수집 시작 (네이버+구글)")
    print(f"{'─'*52}")

    naver_headers = {
        "X-Naver-Client-Id":     "0G9LeMqi2n9OQTmH0ueC",
        "X-Naver-Client-Secret": "6tgdSvlfjA"
    }

    all_trends = db.reference('chart_data/trends').get() or {}
    updates_db = {}

    # 구글 트렌드 34개 한번에 수집
    print(f"  🔍 구글 트렌드 수집 중...")
    google_keywords  = list(SEARCH_MAPPING.values())
    google_scores    = fetch_google_trends(google_keywords)
    google_baseline  = db.reference('chart_data/google_baseline').get() or {}
    google_updates   = {}

    success, fail = 0, 0

    for ticker in TICKER_KEYS:
        try:
            search_query = SEARCH_MAPPING[ticker]
            data         = all_trends.get(ticker, {})
            baseline     = data.get('baseline', 0)

            # 네이버 블로그 수집
            query = urllib.parse.quote(search_query)
            url   = f"https://openapi.naver.com/v1/search/blog.json?query={query}&display=1&sort=date"
            resp  = requests.get(url, headers=naver_headers, timeout=5)

            if resp.status_code != 200:
                print(f" ❌ {ticker.ljust(10)} 네이버 에러: {resp.status_code}")
                fail += 1
                continue

            naver_score = float(resp.json().get('total', 0))
            g_current   = google_scores.get(search_query, 0.0)
            google_updates[search_query] = g_current

            # 최초 세팅
            if baseline == 0:
                updates_db[ticker] = {
                    'baseline':       naver_score,
                    'last_score':     naver_score,
                    'target_yield':   0.0,
                    'current_yield':  data.get('current_yield', 0.0),
                    'last_update_ts': now_ts
                }
                print(f" 🔄 {ticker.ljust(10)}: 최초 세팅 (네이버:{int(naver_score):,} / 구글:{g_current:.0f})")
                success += 1
                continue

            # 네이버 변화량 → % (선형: 1건=0.2%, 10건=2%, 100건=20%)
            naver_diff = naver_score - baseline
            if naver_diff == 0:
                naver_yield = random.choice([1, -1]) * random.uniform(0.001, 0.0015)
            else:
                base        = random.uniform(0.001, 0.002)
                naver_yield = (naver_diff * 0.002) + (base * (1 if naver_diff > 0 else -1))

            # 구글 변화량 → % (선형: 1pt=0.25%, 10pt=2.5%, 40pt=10%)
            g_baseline_val = google_baseline.get(search_query, g_current)
            g_diff         = g_current - g_baseline_val
            if g_diff == 0:
                google_yield = random.choice([1, -1]) * random.uniform(0.001, 0.0015)
            else:
                base         = random.uniform(0.001, 0.002)
                google_yield = (g_diff * 0.0025) + (base * (1 if g_diff > 0 else -1))

            # 평균, 최대 +-30%
            target_yield = (naver_yield + google_yield) / 2.0
            target_yield = float(np.clip(target_yield, -0.30, 0.30))

            updates_db[ticker] = {
                'baseline':       naver_score,
                'last_score':     naver_score,
                'target_yield':   target_yield,
                'current_yield':  data.get('current_yield', 0.0),
                'last_update_ts': now_ts
            }

            print(
                f" ✅ {ticker.ljust(10)}: {target_yield * 100:>+6.2f}%"
                f"  (N:{int(naver_diff):+}건={naver_yield*100:+.2f}%"
                f" / G:{g_diff:+.0f}pt={google_yield*100:+.2f}%)"
            )
            success += 1

        except Exception as e:
            print(f" ❌ {ticker.ljust(10)} 실패: {e}")
            fail += 1

    if updates_db:
        db.reference('chart_data/trends').update(updates_db)
    if google_updates:
        db.reference('chart_data/google_baseline').update(google_updates)

    print(f"{'─'*52}")
    print(f"✔ 완료: 성공 {success} / 실패 {fail}  ({int(time.time()-now_ts)}초 소요)")
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

    now = datetime.now(KST)

    def next_minute_mark(dt):
        return (dt + timedelta(minutes=1)).replace(second=0, microsecond=0)

    def next_10min_mark(dt):
        next_m = ((dt.minute // 10) + 1) * 10
        if next_m >= 60:
            return (dt + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        return dt.replace(minute=next_m, second=0, microsecond=0)

    # ① 다음 분 00초에 첫 수집 (예: 16:37:24 시작 → 16:38:00 첫수집)
    first_sync  = next_minute_mark(now)
    # ② 그 다음 분 00초에 두번째 수집 (예: 16:39:00)
    second_sync = next_minute_mark(first_sync)

    print(f"📡 [1] 첫 수집 예정:   {first_sync.strftime('%H:%M:%S')}")
    print(f"📡 [2] 두번째 수집 예정: {second_sync.strftime('%H:%M:%S')}")

    # 두번째 수집 후 → second_sync 기준 +10분마다 반복
    def second_fetch_then_schedule():
        fetch_and_update()
        # second_sync 시각 기준으로 +10분, +20분 ... 계산
        third_sync = second_sync + timedelta(minutes=10)
        print(f"📡 [3~] 이후 10분 간격 수집: {third_sync.strftime('%H:%M:%S')} 부터")
        scheduler.add_job(
            fetch_and_update, 'interval', minutes=10,
            start_date=third_sync,
            max_instances=1, coalesce=True,
            id='fetch_10min'
        )

    scheduler.add_job(
        fetch_and_update, 'date',
        run_date=first_sync, max_instances=1
    )
    scheduler.add_job(
        second_fetch_then_schedule, 'date',
        run_date=second_sync, max_instances=1
    )

    # 분봉 기록: 매 1분 00초
    scheduler.add_job(
        record_minute_candle, 'interval', minutes=1,
        start_date=first_sync
    )

    scheduler.add_job(daily_reset, 'cron', hour=0, minute=0, second=0)

    run_ticks()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
