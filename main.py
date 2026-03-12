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
tick_paused     = False  # :57~:00 구간 틱 일시정지
market_open     = False  # 장 운영 여부

# ---------------------------------------------------------
# 3. 틱 엔진  ★ 수정: 수렴 속도 / 오르락내리락 / 일방통행 방지
# ---------------------------------------------------------
def generate_ticks():
    if not market_open:
        return
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

                distance  = target - current
                abs_dist  = abs(distance)

                last_update_ts = data.get('last_update_ts', now_ts - 600)
                elapsed_sec    = now_ts - last_update_ts
                remaining_sec  = max(5, 600 - elapsed_sec)

                # ★ 수렴 강도: 초반 느리게, 후반 조금 빠르게 (전체 10분 사용)
                convergence_ratio    = min(1.0, elapsed_sec / 600)
                convergence_strength = 0.15 + convergence_ratio * 0.25   # 0.15 ~ 0.40
                ideal_step           = (distance / remaining_sec) * convergence_strength

                # ★ 방향 결정: candle_mode로 분봉 단위 음봉 확정
                state   = tick_state.get(ticker, {'counter': 0, 'dir': 1})
                counter = state['counter']
                cur_dir = state['dir']
                mode    = candle_mode.get(ticker, 'normal')

                if counter <= 0:
                    rand = random.random()

                    if abs_dist < 0.002:
                        # 수렴 근처: 50/50 진동
                        cur_dir = 1 if rand < 0.50 else -1
                        counter = random.randint(1, 2)
                    elif mode == 'reverse':
                        # 역방향 우세(70%) 이지만 30%는 정방향으로도 갈 수 있음
                        rev_dir = -1 if distance > 0 else 1
                        cur_dir = rev_dir if rand < 0.70 else -rev_dir
                        same_rev = (cur_dir == rev_dir)
                        counter = random.randint(2, 4) if same_rev else random.randint(1, 2)
                    else:
                        # 정방향 우세(65%) 이지만 35%는 역방향으로도 갈 수 있음
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

                # ★ 이동량: 수렴 완료 후에도 최소 변동폭 보장
                MIN_STEP = 0.0008   # 최소 틱 이동량 (봉 몸통 보장)
                volatility = 0.00080 + abs_dist * 0.010

                if abs_dist < 0.002:
                    # 수렴 근처 or 완료: 최소 변동폭으로 자연스러운 진동
                    step = MIN_STEP * random.uniform(0.6, 1.4)
                    move = cur_dir * step
                    move = float(np.clip(move, -MIN_STEP * 1.5, MIN_STEP * 1.5))
                else:
                    # 틱당 최대 이동폭: abs_dist 비례 + 절대값 상한선 0.0005 (봉 급등 방지)
                    max_step = min(abs_dist * 0.003, 0.0005)
                    same_dir_move = (cur_dir > 0 and distance > 0) or (cur_dir < 0 and distance < 0)
                    if same_dir_move:
                        step = max_step * random.uniform(0.7, 1.0)
                    else:
                        step = max_step * 0.5 * random.uniform(0.6, 1.0)
                    move = cur_dir * step + np.random.normal(0, volatility * 0.2)
                    move = float(np.clip(move, -max_step, max_step))
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
# 4. 봉 마감: :57초 스냅샷 → :00초 분봉 저장  ★ 신규
# ---------------------------------------------------------
def take_candle_snapshot():
    """매 분 :57초 — 현재 OHLC 스냅샷 저장"""
    if not market_open:
        return
    for ticker in TICKER_KEYS:
        buf = ohlc_buffer.get(ticker)
        if buf:
            candle_snapshot[ticker] = dict(buf)  # 현재 버퍼 복사


def record_minute_candle():
    """매 분 :00초 — 스냅샷으로 분봉 저장, 새 봉은 스냅샷 close 에서 정확히 시작"""
    if not market_open:
        return
    try:
        now_utc   = datetime.now(pytz.utc).replace(second=0, microsecond=0)
        candle_ts = int(now_utc.timestamp()) - 60
        current_updates = {}

        for ticker in TICKER_KEYS:
            # 스냅샷 우선, 없으면 현재 버퍼 사용
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

            # 최초 세팅: baseline=0이면 이번 값을 baseline으로 저장만 (diff=0 처리)
            if baseline == 0:
                baseline = naver_score  # 이번 값을 baseline으로 삼고 diff=0으로 계산
                updates_db[f'{ticker}/baseline']   = naver_score
                updates_db[f'{ticker}/last_score'] = naver_score
                print(f"  {ticker.ljust(12)}: 최초 세팅 ({int(naver_score):,}건)")

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
# 장 시작 / 마감
# ---------------------------------------------------------
def market_start():
    """매일 09:00 — 장 시작"""
    global market_open, fetch_count
    print(f"\n{'='*52}")
    print(f"[장 시작] {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*52}")

    market_open = True
    fetch_count = 0   # 수집 카운터 초기화

    # 수렴 기준값 리셋 (current_yield/target 0으로)
    now_ts  = int(time.time())
    updates = {}
    for ticker in TICKER_KEYS:
        updates[ticker] = {
            'baseline': 0, 'last_score': 0,
            'target_yield': 0.0, 'current_yield': 0.0,
            'last_update_ts': now_ts
        }
        ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
        tick_state[ticker]  = {'counter': 0, 'dir': 1}
        candle_mode[ticker] = 'normal'
    db.reference('chart_data/trends').set(updates)
    db.reference('chart_data/live_data').set({})
    candle_snapshot.clear()

    # 1차 수집: 다음 분 정각
    next_mark = (datetime.now(KST) + timedelta(minutes=1)).replace(second=0, microsecond=0)
    print(f"[→] 1차 수집 예정: {next_mark.strftime('%H:%M:%S')}")
    scheduler.add_job(fetch_and_update, 'date', run_date=next_mark,
                      max_instances=1, id='fetch_1st',
                      replace_existing=True)


def market_close():
    """매일 00:00 — 장 마감"""
    global market_open
    print(f"\n{'='*52}")
    print(f"[장 마감] {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*52}")

    market_open = False

    # 수집 관련 job 모두 제거
    for job_id in ('fetch_1st', 'fetch_2nd', 'fetch_10min'):
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass

    candle_snapshot.clear()
    print("틱 엔진 및 수집 중단. 차트 히스토리 유지.")

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
    """서버 시작 시 초기화 — Firebase 기존값 유지, 메모리만 복원"""
    print("초기화 중... (Firebase 기존값 유지)")
    now_ts     = int(time.time())
    all_trends = db.reference('chart_data/trends').get() or {}

    for ticker in TICKER_KEYS:
        data    = all_trends.get(ticker, {})
        current = data.get('current_yield', 0.0)
        # 메모리 복원 (Firebase 기존값 그대로)
        ohlc_buffer[ticker] = {
            'open': current, 'high': current,
            'low':  current, 'close': current
        }
        tick_state[ticker]  = {'counter': 0, 'dir': 1}
        candle_mode[ticker] = 'normal'

        # baseline이 없는 경우만 초기화
        if not data.get('baseline'):
            db.reference(f'chart_data/trends/{ticker}').set({
                'baseline': 0, 'last_score': 0,
                'target_yield': 0.0, 'current_yield': 0.0,
                'last_update_ts': now_ts
            })

    candle_snapshot.clear()
    print("초기화 완료\n")


# ---------------------------------------------------------
# 8. 스케줄러
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

def run_ticks():
    generate_ticks()
    delay    = random.uniform(0.5, 1.0)   # ★ 0.5~1.0초 랜덤
    next_run = datetime.now(KST) + timedelta(seconds=delay)
    scheduler.add_job(run_ticks, 'date', run_date=next_run)


def is_market_hours():
    """현재 장 운영 시간 여부 (09:00 ~ 00:00 KST)"""
    now = datetime.now(KST)
    return 9 <= now.hour < 24


if __name__ == "__main__":
    initialize_app()

    now_kst = datetime.now(KST)
    print(f"서버 시작: {now_kst.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"장 운영 시간: 09:00 ~ 00:00 KST")
    print()

    # 매 분 :57초 — 봉 스냅샷 (항상 등록, market_open 체크는 내부에서)
    scheduler.add_job(take_candle_snapshot, 'cron', second=57, max_instances=1)

    # 매 분 :00초 — 분봉 저장
    scheduler.add_job(record_minute_candle, 'cron', second=0, max_instances=1)

    # 매일 09:00 — 장 시작
    scheduler.add_job(market_start, 'cron', hour=9, minute=0, second=0)

    # 매일 00:00 — 장 마감
    scheduler.add_job(market_close, 'cron', hour=0, minute=0, second=0)

    # 자정 리셋
    scheduler.add_job(daily_reset, 'cron', hour=0, minute=0, second=0)

    # 서버 시작 시 이미 장 시간이면 즉시 장 시작
    h, m = now_kst.hour, now_kst.minute
    if 9 <= h < 24:
        print("[서버 시작] 현재 장 시간 → 즉시 장 시작")
        market_open = True
        fetch_count = 0
        next_mark = (now_kst + timedelta(minutes=1)).replace(second=0, microsecond=0)
        scheduler.add_job(fetch_and_update, 'date', run_date=next_mark,
                          max_instances=1, id='fetch_1st', replace_existing=True)
        print(f"[→] 1차 수집 예정: {next_mark.strftime('%H:%M:%S')}")
    else:
        print(f"[서버 시작] 장 외 시간 → 09:00에 자동 시작")

    run_ticks()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
