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

TICKER_KEYS     = list(SEARCH_MAPPING.keys())
ohlc_buffer     = {}
tick_state      = {}
candle_snapshot = {}   # :57초 봉 마감 스냅샷
candle_mode     = {}   # 분봉 방향 모드: 'normal' or 'reverse' (음봉 확정)
fetch_count     = 0    # 수집 완료 횟수

TICK_INTERVAL = 0.75   # 평균 틱 간격(초)
CANDLE_PERIOD = 600    # 수렴 주기(초) = 10분

# ---------------------------------------------------------
# 3. 틱 엔진  ★ 수정: 600초 균등 수렴
# ---------------------------------------------------------
def generate_ticks():
    try:
        ref        = db.reference('chart_data/trends')
        all_trends = ref.get()
        if not all_trends:
            return

        updates_trends = {}
        updates_live   = {}
        now_ts         = int(time.time())

        for ticker, data in all_trends.items():
            try:
                target = data.get('target_yield', 0.0)

                if ticker in ohlc_buffer:
                    current = ohlc_buffer[ticker]['close']
                else:
                    current = data.get('current_yield', 0.0)
                    ohlc_buffer[ticker] = {
                        'open': current, 'high': current,
                        'low':  current, 'close': current
                    }

                distance = target - current
                abs_dist = abs(distance)

                # ★ 핵심: 남은 시간 기준으로 틱당 이동량 계산 (600초 균등 수렴)
                last_update_ts = data.get('last_update_ts', now_ts - CANDLE_PERIOD)
                elapsed        = now_ts - last_update_ts
                remaining      = max(1, CANDLE_PERIOD - elapsed)

                ideal_move = (distance / remaining) * TICK_INTERVAL
                noise      = np.random.normal(0, abs(ideal_move) * 0.2 + 0.00005)

                # 방향 결정 (candle_mode 반영)
                state   = tick_state.get(ticker, {'counter': 0, 'dir': 1})
                counter = state['counter']
                cur_dir = state['dir']
                mode    = candle_mode.get(ticker, 'normal')

                if counter <= 0:
                    rand = random.random()

                    if abs_dist < 0.0005:
                        cur_dir = 1 if rand < 0.50 else -1
                        counter = random.randint(1, 2)
                    elif mode == 'reverse':
                        rev_dir = -1 if distance > 0 else 1
                        cur_dir = rev_dir if rand < 0.70 else -rev_dir
                        same_rev = (cur_dir == rev_dir)
                        counter = random.randint(2, 4) if same_rev else random.randint(1, 2)
                    else:
                        if distance > 0:
                            cur_dir = 1 if rand < 0.65 else -1
                        elif distance < 0:
                            cur_dir = -1 if rand < 0.65 else 1
                        else:
                            cur_dir = 1 if rand < 0.50 else -1
                        same_dir = (cur_dir > 0 and distance > 0) or (cur_dir < 0 and distance < 0)
                        counter  = random.randint(2, 4) if same_dir else random.randint(1, 2)

                    tick_state[ticker] = {'counter': counter, 'dir': cur_dir}
                else:
                    tick_state[ticker]['counter'] = counter - 1

                # cur_dir이 ideal_move 방향과 같으면 그대로, 반대면 감속
                ideal_dir = 1 if distance >= 0 else -1
                if cur_dir == ideal_dir:
                    move = ideal_move + noise
                else:
                    move = ideal_move * 0.3 + noise  # 역방향일 땐 30%만 이동

                # target 초과 방지
                projected = current + move
                if distance > 0 and projected > target:
                    move = target - current
                elif distance < 0 and projected < target:
                    move = target - current

                next_tick = round(current + move, 6)
                updates_trends[f'{ticker}/current_yield'] = next_tick

                buf          = ohlc_buffer[ticker]
                buf['high']  = max(buf['high'],  next_tick)
                buf['low']   = min(buf['low'],   next_tick)
                buf['close'] = next_tick

                updates_live[ticker] = {
                    'time':  now_ts,
                    'open':  buf['open'], 'high': buf['high'],
                    'low':   buf['low'],  'close': buf['close']
                }

            except Exception:
                continue

        if updates_trends:
            db.reference('chart_data/trends').update(updates_trends)
        if updates_live:
            db.reference('chart_data/live_data').update(updates_live)

    except Exception as e:
        print(f"generate_ticks 에러: {e}")


# ---------------------------------------------------------
# 4. 봉 마감: :57초 스냅샷 → :00초 분봉 저장
# ---------------------------------------------------------
def take_candle_snapshot():
    """매 분 :57초 — 현재 OHLC 스냅샷 저장"""
    for ticker in TICKER_KEYS:
        buf = ohlc_buffer.get(ticker)
        if buf:
            candle_snapshot[ticker] = {
                'open': buf['open'], 'high': buf['high'],
                'low':  buf['low'],  'close': buf['close']
            }


def record_minute_candle():
    """매 분 :00초 — 스냅샷으로 분봉 저장, 새 봉은 스냅샷 close 에서 정확히 시작"""
    try:
        now_utc   = datetime.now(pytz.utc).replace(second=0, microsecond=0)
        candle_ts = int(now_utc.timestamp()) - 60
        current_updates = {}

        for ticker in TICKER_KEYS:
            candle = candle_snapshot.get(ticker) or ohlc_buffer.get(ticker)
            if not candle:
                continue

            close_price = candle['close']

            db.reference(f'chart_data/chart_history/{ticker}/1m').push({
                'time':  candle_ts,
                'open':  candle['open'], 'high': candle['high'],
                'low':   candle['low'],  'close': close_price
            })

            # 새 봉은 스냅샷 close 에서 정확히 시작
            ohlc_buffer[ticker] = {
                'open':  close_price, 'high': close_price,
                'low':   close_price, 'close': close_price
            }
            tick_state[ticker] = {'counter': 0, 'dir': 1}

            # ★ 다음 분봉 모드 결정: 30% 확률로 음봉 확정
            candle_mode[ticker] = 'reverse' if random.random() < 0.30 else 'normal'

            current_updates[f'{ticker}/current_yield'] = close_price

        candle_snapshot.clear()

        if current_updates:
            db.reference('chart_data/trends').update(current_updates)

    except Exception as e:
        print(f"record_minute_candle 에러: {e}")


# ---------------------------------------------------------
# 5. 네이버 수집  ★ 오버슈트 클램핑 추가
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

    global fetch_count
    fetch_count += 1

    all_trends = db.reference('chart_data/trends').get() or {}
    updates_db = {}
    success, fail = 0, 0

    for ticker in TICKER_KEYS:
        try:
            query = urllib.parse.quote(SEARCH_MAPPING[ticker])
            url   = f"https://openapi.naver.com/v1/search/blog.json?query={query}&display=1&sort=date"
            resp  = requests.get(url, headers=headers, timeout=5)

            if resp.status_code != 200:
                print(f"  {ticker.ljust(12)} 네이버 에러: {resp.status_code}")
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
                    'current_yield':  0.0,
                    'last_update_ts': now_ts
                }
                print(f"  {ticker.ljust(12)}: 최초 세팅 ({int(naver_score):,}건)")
                success += 1
                continue

            diff = naver_score - baseline

            if diff == 0:
                target_yield = random.choice([1, -1]) * random.uniform(0.001, 0.0015)
            else:
                sign    = 1 if diff > 0 else -1
                base    = random.uniform(0.001, 0.002)
                log_val = np.log1p(abs(diff)) * 0.018
                target_yield = sign * (base + log_val)

            target_yield = float(np.clip(target_yield, -0.30, 0.30))

            # ★ 현재 위치가 새 target을 이미 넘었으면 target으로 클램핑
            current_now = ohlc_buffer.get(ticker, {}).get('close', data.get('current_yield', 0.0))
            if (target_yield >= 0 and current_now > target_yield) or \
               (target_yield < 0 and current_now < target_yield):
                current_now = target_yield
                if ticker in ohlc_buffer:
                    for k in ('open', 'high', 'low', 'close'):
                        ohlc_buffer[ticker][k] = current_now

            updates_db[f'{ticker}/baseline']       = naver_score
            updates_db[f'{ticker}/last_score']     = naver_score
            updates_db[f'{ticker}/target_yield']   = target_yield
            updates_db[f'{ticker}/current_yield']  = current_now
            updates_db[f'{ticker}/last_update_ts'] = now_ts

            print(
                f"  {ticker.ljust(12)}: {target_yield * 100:>+6.2f}%"
                f"  (전:{int(baseline):,} → 현:{int(naver_score):,} | 증감:{int(diff):+}건)"
            )
            success += 1

        except Exception as e:
            print(f"  {ticker.ljust(12)} 실패: {e}")
            fail += 1

    if updates_db:
        db.reference('chart_data/trends').update(updates_db)

    print(f"{'─'*52}")
    print(f"완료: 성공 {success} / 실패 {fail}  ({int(time.time()-now_ts)}초 소요)")
    print(f"{'─'*52}\n")

    # ★ interval job 없을 때만 다음 수집 예약
    if scheduler.get_job('fetch_10min') is None:
        _schedule_next_fetch()


def _schedule_next_fetch():
    """1차 완료→2차 예약, 2차 완료→10분 간격 등록 (fetch_count 기반)"""
    now_kst   = datetime.now(KST)
    next_mark = (now_kst + timedelta(minutes=1)).replace(second=0, microsecond=0)

    if fetch_count == 1:
        # 1차 완료 → 2차 예약
        print(f"[→] 2차 수집 예정: {next_mark.strftime('%H:%M:%S')}")
        scheduler.add_job(
            fetch_and_update, 'date',
            run_date=next_mark,
            max_instances=1,
            id='fetch_2nd'
        )
    elif fetch_count == 2:
        # 2차 완료 → 10분 간격 등록
        interval_start = next_mark + timedelta(minutes=10)
        print(f"[→] 10분 간격 시작: {interval_start.strftime('%H:%M:%S')}")
        scheduler.add_job(
            fetch_and_update, 'interval', minutes=10,
            start_date=interval_start,
            max_instances=1, coalesce=True,
            id='fetch_10min'
        )


# ---------------------------------------------------------
# 6. 자정 리셋
# ---------------------------------------------------------
def daily_reset():
    print(f"\n[자정 리셋] {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        ref        = db.reference('chart_data/trends')
        all_trends = ref.get()
        if not all_trends:
            return
        updates = {}
        now_ts  = int(time.time())
        for ticker in TICKER_KEYS:
            data = all_trends.get(ticker, {})
            updates[f'{ticker}/baseline']       = data.get('last_score', 0)
            updates[f'{ticker}/target_yield']   = 0.0
            updates[f'{ticker}/current_yield']  = 0.0
            updates[f'{ticker}/last_update_ts'] = now_ts
            ohlc_buffer[ticker]  = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
            tick_state[ticker]   = {'counter': 0, 'dir': 1}
        db.reference('chart_data/trends').update(updates)
        candle_snapshot.clear()
    except Exception as e:
        print(f"리셋 에러: {e}")


# ---------------------------------------------------------
# 7. 초기화 (chart_history 유지, 수치만 리셋)
# ---------------------------------------------------------
def initialize_app():
    print("초기화 중... (chart_history 유지)")
    now_ts  = int(time.time())
    updates = {}
    for ticker in TICKER_KEYS:
        updates[ticker] = {
            'baseline': 0, 'last_score': 0,
            'target_yield': 0.0, 'current_yield': 0.0,
            'last_update_ts': now_ts
        }
        ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
        tick_state[ticker]   = {'counter': 0, 'dir': 1}
        candle_mode[ticker]  = 'normal'
    db.reference('chart_data/trends').set(updates)
    db.reference('chart_data/live_data').set({})
    candle_snapshot.clear()
    print("초기화 완료\n")


# ---------------------------------------------------------
# 8. 스케줄러  ★ 수정: 09:00~12:00 시간 체크 추가
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks():
    now_kst = datetime.now(KST)
    if 9 <= now_kst.hour < 12:
        generate_ticks()
        delay = random.uniform(0.5, 1.0)
    else:
        delay = 30.0  # 운영시간 외: 30초마다 대기

    next_run = datetime.now(KST) + timedelta(seconds=delay)
    scheduler.add_job(run_ticks, 'date', run_date=next_run)


if __name__ == "__main__":
    initialize_app()

    now        = datetime.now(KST)
    first_sync = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)

    print(f"현재: {now.strftime('%H:%M:%S')}  |  1차 수집: {first_sync.strftime('%H:%M:%S')}")
    print(f"2차는 1차 완료 직후 다음 분 정각, 이후 10분 간격\n")

    # 1차 수집
    scheduler.add_job(fetch_and_update, 'date', run_date=first_sync,
                      max_instances=1, id='fetch_1st')

    # 매 분 :57초 — 봉 스냅샷
    scheduler.add_job(take_candle_snapshot, 'cron', second=57, max_instances=1)

    # 매 분 :00초 — 분봉 저장
    scheduler.add_job(record_minute_candle, 'cron', second=0, max_instances=1)

    # 자정 리셋
    scheduler.add_job(daily_reset, 'cron', hour=0, minute=0, second=0)

    run_ticks()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
