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
    print(f"Firebase 인증 실패: {e}")

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
tick_state  = {}

# ---------------------------------------------------------
# 3. 틱 엔진
# target/last_update_ts 로컬 캐시 (Firebase 읽기 지연으로 인한 점프 방지)
local_target_cache = {}  # {ticker: {'target': float, 'last_update_ts': int}}

# ---------------------------------------------------------
def generate_ticks():
    try:
        updates_trends = {}
        updates_live   = {}
        now_ts = int(time.time())

        # 로컬 캐시가 비어있을 때만 Firebase에서 읽기
        if not local_target_cache:
            all_trends = db.reference('chart_data/trends').get()
            if not all_trends:
                return
            for ticker, data in all_trends.items():
                local_target_cache[ticker] = {
                    'target':         data.get('target_yield', 0.0),
                    'last_update_ts': data.get('last_update_ts', now_ts - 600)
                }

        for ticker in TICKER_KEYS:
            try:
                cache  = local_target_cache.get(ticker, {'target': 0.0, 'last_update_ts': now_ts - 600})
                target = cache['target']
                last_update_ts = cache['last_update_ts']

                if ticker in ohlc_buffer:
                    current = ohlc_buffer[ticker]['close']
                else:
                    ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
                    current = 0.0

                elapsed_sec   = now_ts - last_update_ts
                remaining_sec = max(5, 600 - elapsed_sec)
                distance      = target - current

                # 이번 분봉 방향 (candle_dir): 분봉 시작 시 결정, 유지됨
                state      = tick_state.get(ticker, {})
                candle_dir = state.get('candle_dir', None)
                counter    = state.get('counter', 0)
                cur_dir    = state.get('cur_dir', 1)

                if candle_dir is None:
                    main_dir = 1 if distance >= 0 else -1
                    if abs(distance) > 0.002 and random.random() < 0.25:
                        candle_dir = -main_dir
                    else:
                        candle_dir = main_dir
                    cur_dir = candle_dir
                    counter = random.randint(1, 2)
                    tick_state[ticker] = {'candle_dir': candle_dir, 'cur_dir': cur_dir, 'counter': counter}

                if counter <= 0:
                    if random.random() < 0.55:
                        cur_dir = candle_dir
                        counter = random.randint(1, 2)
                    else:
                        cur_dir = -candle_dir
                        counter = 1
                    tick_state[ticker] = {'candle_dir': candle_dir, 'cur_dir': cur_dir, 'counter': counter}
                else:
                    tick_state[ticker]['counter'] = counter - 1

                convergence_strength = 1.0 + ((600 - remaining_sec) / 600) * 2.0
                ideal_step = (distance / remaining_sec) * convergence_strength

                # 단일 틱 최대 이동량: 전체 distance의 3% 또는 0.0015 중 작은 값
                # → 긴 봉 방지
                tick_cap = min(0.0015, abs(distance) * 0.03)
                tick_cap = max(tick_cap, 0.0002)  # 최소 이동 보장

                if abs(distance) < 0.0003:
                    move = np.random.normal(0, 0.0002)
                else:
                    move = cur_dir * tick_cap * random.uniform(0.5, 1.0)

                # 반대 방향은 절반으로 더 제한
                if cur_dir != (1 if distance >= 0 else -1):
                    move = move * 0.5

                move = float(np.clip(move, -tick_cap, tick_cap))

                # target 절대 초과 금지
                projected = current + move
                if distance > 0 and projected > target:
                    move = target - current
                elif distance < 0 and projected < target:
                    move = target - current

                next_tick = round(current + move, 6)
                updates_trends[f'{ticker}/current_yield'] = next_tick

                buf = ohlc_buffer[ticker]
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
        print(f"generate_ticks 에러: {e}")


def record_minute_candle():
    try:
        now_utc = datetime.now(pytz.utc).replace(second=0, microsecond=0)
        ts = int(now_utc.timestamp())
        if not ohlc_buffer:
            return

        history_updates = {}
        trend_updates   = {}
        live_updates    = {}

        for ticker in TICKER_KEYS:
            candle = ohlc_buffer.get(ticker)
            if candle:
                close_price = candle['close']

                # 분봉 히스토리 저장
                db.reference(f'chart_data/chart_history/{ticker}/1m').push({
                    'time': ts,
                    'open':  candle['open'],  'high': candle['high'],
                    'low':   candle['low'],   'close': close_price
                })

                # ohlc_buffer 초기화 (다음 분봉 open = 직전 close, 갭 없음)
                ohlc_buffer[ticker] = {
                    'open':  close_price, 'high': close_price,
                    'low':   close_price, 'close': close_price
                }
                tick_state[ticker] = {'candle_dir': None, 'cur_dir': 1, 'counter': 0}

                # trends, live_data 동시에 업데이트 (점프 방지)
                trend_updates[f'{ticker}/current_yield'] = close_price
                live_updates[ticker] = {
                    'time':  ts,
                    'open':  close_price, 'high': close_price,
                    'low':   close_price, 'close': close_price
                }

        # 한 번에 모두 업데이트
        if trend_updates:
            db.reference('chart_data/trends').update(trend_updates)
        if live_updates:
            db.reference('chart_data/live_data').update(live_updates)

    except Exception as e:
        print(f"record_minute_candle 에러: {e}")


# ---------------------------------------------------------
# 4. 네이버 수집 (구글 제거)
# ---------------------------------------------------------
def fetch_and_update():
    now_ts  = int(time.time())
    now_kst = datetime.now(KST)

    print(f"\n{'─'*52}")
    print(f"[{now_kst.strftime('%H:%M:%S')}] 34개 수집 시작")
    print(f"{'─'*52}")

    headers = {
        "X-Naver-Client-Id":     "0G9LeMqi2n9OQTmH0ueC",
        "X-Naver-Client-Secret": "6tgdSvlfjA"
    }

    all_trends = db.reference('chart_data/trends').get() or {}
    updates_db = {}
    success, fail = 0, 0

    for ticker in TICKER_KEYS:
        try:
            query = urllib.parse.quote(SEARCH_MAPPING[ticker])
            url   = f"https://openapi.naver.com/v1/search/blog.json?query={query}&display=1&sort=date"
            resp  = requests.get(url, headers=headers, timeout=5)

            if resp.status_code != 200:
                print(f" {ticker.ljust(10)} 네이버 에러: {resp.status_code}")
                fail += 1
                continue

            naver_score = float(resp.json().get('total', 0))
            data        = all_trends.get(ticker, {})
            baseline    = data.get('baseline', 0)

            # 최초 세팅
            if baseline == 0:
                updates_db[ticker] = {
                    'baseline':       naver_score,
                    'last_score':     naver_score,
                    'target_yield':   0.0,
                    'last_update_ts': now_ts
                    # current_yield 건드리지 않음
                }
                print(f" {ticker.ljust(10)}: 최초 세팅 ({int(naver_score):,}건)")
                success += 1
                continue

            diff = naver_score - baseline

            # 1건 = 0.2% 선형, 0건이면 +-0.1~0.15% 랜덤
            if diff == 0:
                target_yield = random.choice([1, -1]) * random.uniform(0.001, 0.0015)
            else:
                sign         = 1 if diff > 0 else -1
                base         = random.uniform(0.001, 0.002)
                log_val      = np.log1p(abs(diff)) * 0.018  # 1건≈1%, 5건≈3%, 10건≈5%, 40건≈8%
                target_yield = sign * (base + log_val)

            target_yield = float(np.clip(target_yield, -0.30, 0.30))

            # current_yield 건드리지 않고 target/baseline만 업데이트
            updates_db[f'{ticker}/baseline']       = naver_score
            updates_db[f'{ticker}/last_score']     = naver_score
            updates_db[f'{ticker}/target_yield']   = target_yield
            updates_db[f'{ticker}/last_update_ts'] = now_ts

            # 로컬 캐시도 즉시 업데이트 (틱 엔진이 바로 새 target 사용)
            local_target_cache[ticker] = {
                'target':         target_yield,
                'last_update_ts': now_ts
            }

            print(
                f" {ticker.ljust(10)}: {target_yield * 100:>+6.2f}%"
                f"  (전:{int(baseline):,} -> 현:{int(naver_score):,} | 증감:{int(diff):+}건)"
            )
            success += 1

        except Exception as e:
            print(f" {ticker.ljust(10)} 실패: {e}")
            fail += 1

    if updates_db:
        db.reference('chart_data/trends').update(updates_db)

    print(f"{'─'*52}")
    print(f"완료: 성공 {success} / 실패 {fail}  ({int(time.time()-now_ts)}초 소요)")
    print(f"{'─'*52}\n")


# ---------------------------------------------------------
# 5. 자정 리셋
# ---------------------------------------------------------
def daily_reset():
    print(f"\n[자정 리셋] {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")
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
        print(f"리셋 에러: {e}")


# ---------------------------------------------------------
# 6. 초기화
# ---------------------------------------------------------
def initialize_app():
    print("초기화 중...")
    now_ts = int(time.time())

    # 기존 Firebase 데이터 읽기 (재시작 시 보존)
    existing = db.reference('chart_data/trends').get() or {}

    updates = {}
    for ticker in TICKER_KEYS:
        data = existing.get(ticker, {})
        baseline = data.get('baseline', 0)

        # ohlc_buffer: 기존 current_yield 값으로 복구
        current = data.get('current_yield', 0.0)
        ohlc_buffer[ticker] = {
            'open': current, 'high': current,
            'low':  current, 'close': current
        }
        tick_state[ticker] = {'candle_dir': None, 'cur_dir': 1, 'counter': 0}

        # Firebase는 기존 데이터가 있으면 건드리지 않음
        if baseline == 0:
            updates[ticker] = {
                'baseline': 0, 'last_score': 0,
                'target_yield': 0.0, 'current_yield': 0.0,
                'last_update_ts': now_ts
            }
            print(f"  {ticker}: 신규 초기화")
        else:
            print(f"  {ticker}: 기존 데이터 유지 (baseline={int(baseline):,})")

    if updates:
        db.reference('chart_data/trends').update(updates)


# ---------------------------------------------------------
# 7. 스케줄러
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks():
    generate_ticks()
    delay    = random.uniform(0.3, 0.8)
    next_run = datetime.now(KST) + timedelta(seconds=delay)
    scheduler.add_job(run_ticks, 'date', run_date=next_run)


if __name__ == "__main__":
    initialize_app()

    now = datetime.now(KST)

    def next_minute_mark(dt):
        return (dt + timedelta(minutes=1)).replace(second=0, microsecond=0)

    def next_10min_boundary(dt):
        """다음 10분 정각: :00, :10, :20, :30, :40, :50"""
        next_10 = ((dt.minute // 10) + 1) * 10
        if next_10 >= 60:
            return (dt + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        return dt.replace(minute=next_10, second=0, microsecond=0)

    first_sync  = next_minute_mark(now)
    second_sync = next_minute_mark(first_sync)

    print(f"[1] 첫 수집 예정:    {first_sync.strftime('%H:%M:%S')}")
    print(f"[2] 두번째 수집 예정: {second_sync.strftime('%H:%M:%S')}")

    def schedule_next_fetch():
        """다음 10분 정각에 fetch 예약"""
        def fetch_and_reschedule():
            fetch_and_update()
            schedule_next_fetch()
        next_run = next_10min_boundary(datetime.now(KST))
        print(f"[다음 수집 예정] {next_run.strftime('%H:%M:%S')}")
        scheduler.add_job(
            fetch_and_reschedule, 'date',
            run_date=next_run,
            max_instances=1,
            id=f'fetch_{int(next_run.timestamp())}',
            misfire_grace_time=60
        )

    def second_fetch_then_schedule():
        fetch_and_update()
        schedule_next_fetch()

    scheduler.add_job(fetch_and_update,           'date', run_date=first_sync,  max_instances=1)
    scheduler.add_job(second_fetch_then_schedule, 'date', run_date=second_sync, max_instances=1)
    scheduler.add_job(record_minute_candle, 'interval', minutes=1, start_date=first_sync)
    scheduler.add_job(daily_reset, 'cron', hour=0, minute=0, second=0)

    run_ticks()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
