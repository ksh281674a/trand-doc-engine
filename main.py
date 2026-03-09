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

TICKER_KEYS    = list(SEARCH_MAPPING.keys())
ohlc_buffer    = {}   # 현재 진행 중인 분봉 OHLC
tick_state     = {}   # 틱 방향 상태
candle_snapshot = {}  # :57초에 찍는 봉 마감 스냅샷
fetch_count    = 0    # 수집 횟수 카운터

# ---------------------------------------------------------
# 3. 유틸
# ---------------------------------------------------------
def next_minute_mark(dt: datetime) -> datetime:
    """dt 이후 첫 번째 분 정각(KST)"""
    return (dt + timedelta(minutes=1)).replace(second=0, microsecond=0)


# ---------------------------------------------------------
# 4. 틱 엔진 (0.5 ~ 1.0 초 랜덤 간격)
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

                # ohlc_buffer 우선 사용 (Firebase sync 지연 무시)
                if ticker in ohlc_buffer:
                    current = ohlc_buffer[ticker]['close']
                else:
                    current = data.get('current_yield', 0.0)
                    ohlc_buffer[ticker] = {
                        'open': current, 'high': current,
                        'low':  current, 'close': current
                    }

                # ★ 올바른 거리 계산: target - current
                #   예) current=10%, target=2% → distance=-8% (하락 수렴)
                distance  = target - current
                abs_dist  = abs(distance)

                last_update_ts  = data.get('last_update_ts', now_ts - 600)
                elapsed_sec     = now_ts - last_update_ts
                remaining_sec   = max(5, 600 - elapsed_sec)

                # 수렴 강도: 시간이 지날수록 강해짐
                convergence_ratio    = min(1.0, elapsed_sec / 600)
                convergence_strength = 1.0 + convergence_ratio * 2.0
                ideal_step           = (distance / remaining_sec) * convergence_strength

                # ★ target 0.3% 이내 = "수렴 근처" → 양봉/음봉 균형 유지
                near_target = abs_dist < 0.003

                # --- 방향 결정 ---
                state   = tick_state.get(ticker, {'counter': 0, 'dir': 1})
                counter = state['counter']
                cur_dir = state['dir']

                if counter <= 0:
                    rand = random.random()
                    if near_target:
                        # 수렴 근처: 50/50 양봉·음봉 혼합
                        cur_dir = 1 if rand < 0.50 else -1
                        counter = random.randint(1, 2)
                    else:
                        # 목표 방향 60% 우세, 역방향 40%
                        if distance > 0:
                            cur_dir = 1 if rand < 0.60 else -1
                        elif distance < 0:
                            cur_dir = -1 if rand < 0.60 else 1
                        else:
                            cur_dir = 1 if rand < 0.50 else -1

                        # 주 방향이면 1~3틱 유지, 역방향이면 1틱만
                        same_dir = (cur_dir > 0 and distance > 0) or (cur_dir < 0 and distance < 0)
                        counter  = random.randint(1, 3) if same_dir else 1

                    tick_state[ticker] = {'counter': counter, 'dir': cur_dir}
                else:
                    tick_state[ticker]['counter'] = counter - 1

                # --- 이동량 계산 ---
                volatility = 0.00060 + abs_dist * 0.012

                if near_target:
                    # 수렴 근처: 작은 랜덤 진동
                    move = cur_dir * abs(np.random.normal(0, volatility * 1.2))
                else:
                    base = abs(ideal_step) * random.uniform(2.0, 4.0)
                    move = cur_dir * base + np.random.normal(0, volatility * 0.3)

                # 최대 이동폭 제한
                max_step = max(0.0008, abs_dist * 0.40)
                move     = float(np.clip(move, -max_step, max_step))

                # target 초과 방지
                projected = current + move
                if distance > 0 and projected > target:
                    move = target - current
                elif distance < 0 and projected < target:
                    move = target - current

                next_tick = round(current + move, 6)
                updates_trends[f'{ticker}/current_yield'] = next_tick

                buf         = ohlc_buffer[ticker]
                buf['high'] = max(buf['high'],  next_tick)
                buf['low']  = min(buf['low'],   next_tick)
                buf['close']= next_tick

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


def run_ticks():
    """0.5 ~ 1.0 초 랜덤 간격으로 틱 반복"""
    generate_ticks()
    delay    = random.uniform(0.5, 1.0)
    next_run = datetime.now(KST) + timedelta(seconds=delay)
    scheduler.add_job(run_ticks, 'date', run_date=next_run)


# ---------------------------------------------------------
# 5. 봉 마감 처리
#    :57초 → 스냅샷 저장
#    :00초 → 스냅샷으로 분봉 기록 후 새 봉 시작(스냅샷 close 값)
# ---------------------------------------------------------
def take_candle_snapshot():
    """매 분 :57초 호출 — 현재 OHLC를 스냅샷으로 저장"""
    for ticker in TICKER_KEYS:
        buf = ohlc_buffer.get(ticker)
        if buf:
            candle_snapshot[ticker] = {
                'open':  buf['open'],
                'high':  buf['high'],
                'low':   buf['low'],
                'close': buf['close']
            }
    print(f"[{datetime.now(KST).strftime('%H:%M:%S')}] ▶ 봉 스냅샷 저장 ({len(candle_snapshot)}개)")


def record_minute_candle():
    """매 분 :00초 호출 — :57 스냅샷으로 분봉 저장, 새 봉은 스냅샷 close 에서 시작"""
    try:
        now_utc  = datetime.now(pytz.utc).replace(second=0, microsecond=0)
        candle_ts = int(now_utc.timestamp()) - 60  # 직전 분의 타임스탬프

        current_updates = {}

        for ticker in TICKER_KEYS:
            # :57 스냅샷 우선, 없으면 현재 buffer
            candle = candle_snapshot.get(ticker) or ohlc_buffer.get(ticker)
            if not candle:
                continue

            close_price = candle['close']

            # 분봉 히스토리 저장
            db.reference(f'chart_data/chart_history/{ticker}/1m').push({
                'time':  candle_ts,
                'open':  candle['open'], 'high': candle['high'],
                'low':   candle['low'],  'close': close_price
            })

            # ★ 새 봉은 정확히 스냅샷 close 에서 시작
            ohlc_buffer[ticker] = {
                'open':  close_price, 'high': close_price,
                'low':   close_price, 'close': close_price
            }
            tick_state[ticker]     = {'counter': 0, 'dir': 1}
            current_updates[f'{ticker}/current_yield'] = close_price

        candle_snapshot.clear()

        if current_updates:
            db.reference('chart_data/trends').update(current_updates)

        print(f"[{datetime.now(KST).strftime('%H:%M:%S')}] ▶ 분봉 저장 완료")

    except Exception as e:
        print(f"record_minute_candle 에러: {e}")


# ---------------------------------------------------------
# 6. 네이버 수집
# ---------------------------------------------------------
def fetch_and_update():
    global fetch_count
    fetch_count += 1
    current_count = fetch_count

    now_ts  = int(time.time())
    now_kst = datetime.now(KST)

    print(f"\n{'─'*52}")
    print(f"[{now_kst.strftime('%H:%M:%S')}] {current_count}차 수집 시작 (34개)")
    print(f"{'─'*52}")

    headers = {
        "X-Naver-Client-Id":     "0G9LeMqi2n9OQTmH0ueC",
        "X-Naver-Client-Secret": "6tgdSvlfjA"
    }

    all_trends  = db.reference('chart_data/trends').get() or {}
    updates_db  = {}
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
                sign         = 1 if diff > 0 else -1
                base         = random.uniform(0.001, 0.002)
                log_val      = np.log1p(abs(diff)) * 0.018
                target_yield = sign * (base + log_val)

            target_yield = float(np.clip(target_yield, -0.30, 0.30))

            # current_yield 는 건드리지 않음 — 틱 엔진이 수렴
            updates_db[f'{ticker}/baseline']       = naver_score
            updates_db[f'{ticker}/last_score']     = naver_score
            updates_db[f'{ticker}/target_yield']   = target_yield
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

    elapsed = int(time.time() - now_ts)
    print(f"{'─'*52}")
    print(f"완료: 성공 {success} / 실패 {fail}  ({elapsed}초 소요)")
    print(f"{'─'*52}\n")

    # ★ 수집 완료 후 다음 수집 스케줄링
    _schedule_next_fetch(current_count)


def _schedule_next_fetch(completed_count: int):
    """
    수집 완료 직후 호출.
    - 1·2차 완료 → 완료 시각 기준 다음 분 정각에 재수집
    - 3차 완료  → 다음 분 정각 + 10분 간격으로 반복
    """
    now_kst    = datetime.now(KST)
    next_mark  = next_minute_mark(now_kst)

    if completed_count < 3:
        print(f"[→] {completed_count + 1}차 수집 예정: {next_mark.strftime('%H:%M:%S')}")
        scheduler.add_job(
            fetch_and_update, 'date',
            run_date=next_mark,
            max_instances=1,
            id=f'fetch_once_{completed_count + 1}'
        )
    else:
        # 3차 완료: 다음 분 정각 + 10분 뒤부터 10분 간격 반복
        interval_start = next_mark + timedelta(minutes=10)
        print(f"[→] 10분 간격 시작: {interval_start.strftime('%H:%M:%S')} (이후 매 10분)")
        scheduler.add_job(
            fetch_and_update, 'interval', minutes=10,
            start_date=interval_start,
            max_instances=1, coalesce=True,
            id='fetch_interval'
        )


# ---------------------------------------------------------
# 7. 자정 리셋
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
        print("자정 리셋 완료")
    except Exception as e:
        print(f"리셋 에러: {e}")


# ---------------------------------------------------------
# 8. 초기화 (시작 시 Firebase + 로컬 버퍼 완전 초기화)
# ---------------------------------------------------------
def initialize_app():
    print("=" * 52)
    print("앱 초기화 — Firebase 데이터 및 로컬 버퍼 리셋")
    print("=" * 52)
    now_ts   = int(time.time())
    updates  = {}
    for ticker in TICKER_KEYS:
        updates[ticker] = {
            'baseline':       0,
            'last_score':     0,
            'target_yield':   0.0,
            'current_yield':  0.0,
            'last_update_ts': now_ts
        }
        ohlc_buffer[ticker] = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
        tick_state[ticker]  = {'counter': 0, 'dir': 1}

    # chart_history 초기화
    db.reference('chart_data').set({
        'trends':       updates,
        'live_data':    {},
        'chart_history': {}
    })
    candle_snapshot.clear()
    print("초기화 완료\n")


# ---------------------------------------------------------
# 9. 스케줄러 설정 및 실행
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

if __name__ == "__main__":
    initialize_app()

    now_kst    = datetime.now(KST)
    first_sync = next_minute_mark(now_kst)

    print(f"현재 시각:       {now_kst.strftime('%H:%M:%S')}")
    print(f"1차 수집 예정:   {first_sync.strftime('%H:%M:%S')}")
    print(f"(이후 수집은 각 완료 직후 다음 분 정각에 자동 예약)")
    print()

    # 1차 수집: 다음 분 정각
    scheduler.add_job(
        fetch_and_update, 'date',
        run_date=first_sync,
        max_instances=1,
        id='fetch_once_1'
    )

    # 매 분 :57초 — 봉 마감 스냅샷
    scheduler.add_job(
        take_candle_snapshot, 'cron',
        second=57,
        max_instances=1
    )

    # 매 분 :00초 — 스냅샷으로 분봉 저장 + 새 봉 시작
    scheduler.add_job(
        record_minute_candle, 'cron',
        second=0,
        max_instances=1
    )

    # 자정 리셋
    scheduler.add_job(
        daily_reset, 'cron',
        hour=0, minute=0, second=0
    )

    # 틱 엔진 시작 (0.5 ~ 1.0 초 랜덤 간격)
    run_ticks()

    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
