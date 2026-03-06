import time
import requests
import random
import numpy as np
import warnings
import sys
import os
from datetime import datetime, timedelta, timezone
from threading import Thread
from collections import deque
from flask import Flask
from pytrends.request import TrendReq

# --- 1. 로그 및 환경 설정 ---
warnings.filterwarnings('ignore')
KST = timezone(timedelta(hours=9))
app = Flask(__name__)

def log(msg):
    print(f"[{datetime.now(KST).strftime('%H:%M:%S')}] {msg}", file=sys.stdout, flush=True)

@app.route('/')
def home():
    return "TrendDoc Stealth Engine V50.5 Active"

# --- 2. 데이터 경로 ---
FIREBASE_LIVE_URL = "https://trand-doc-default-rtdb.firebaseio.com/live_data.json"
FIREBASE_CHART_URL = "https://trand-doc-default-rtdb.firebaseio.com/chart_history"

STOCK_BASES = {
    '유튜브': 187420, '구글': 224160, '네이버': 172330, '쿠팡': 34570, '넷플릭스': 89450,
    '인스타그램': 42680, '배달의민족': 51240, '치지직': 15240, '틱톡': 9740, '하이브': 195640,
    '카카오': 38450, '네이버웹툰': 55180, '라이엇': 45120, '스팀': 62340, '티빙': 58420,
    '멜론': 52150, '넥슨': 24180, '유튜브 뮤직': 58120, '무신사': 65230, '테무': 21870,
    'SM': 38250, 'X (트위터)': 49820, 'SOOP': 51850, '쿠팡플레이': 45180, '카카오페이지': 40150,
    '애플뮤직': 35240, '요기요': 29850, '알리': 28150, 'YG': 35420, 'JYP': 7450,
    '다음': 35120, 'MS (Bing)': 28140, '쿠팡이츠': 45320, '왓챠': 1248
}

stock_names = list(STOCK_BASES.keys())
stock_queue = deque(stock_names)

current_candle_time = (int(time.time()) // 60) * 60
lock_engine = False

data_map = {}
for name in stock_names:
    bp = float(STOCK_BASES[name])
    data_map[name] = {
        "base_p": bp,
        "curr_p": bp,
        "open": bp,
        "high": bp,
        "low": bp,
        "target_p": bp,
        "velocity": 0.0,
        "last_update_ts": time.time(),
        "volatility": 0.0002,
        "momentum_dir": 1,
        "reversal_count": 0
    }

def snap_to_tick(price):
    if price >= 100000:
        return int(round(price / 100) * 100)
    elif price >= 50000:
        return int(round(price / 50) * 50)
    else:
        return int(round(price / 10) * 10)

# --- 3. 물리 엔진 (백그라운드) ---
def physics_engine():
    global lock_engine, current_candle_time

    while True:
        if lock_engine:
            time.sleep(0.1)
            continue

        sync_data = {}
        now_ts = time.time()

        for name in stock_names:
            s = data_map[name]

            elapsed = now_ts - s["last_update_ts"]
            time_left = max(1.0, 420.0 - elapsed)

            dist = s["target_p"] - s["curr_p"]
            gravity = dist / time_left
            noise = np.random.normal(0, s["base_p"] * s["volatility"])

            s["velocity"] = (s["velocity"] * 0.85) + (gravity * 0.15) + (noise * s["momentum_dir"])
            s["curr_p"] += s["velocity"]

            if time_left < 5:
                s["curr_p"] = s["target_p"] * random.uniform(0.9985, 1.0015)

            dp = snap_to_tick(s["curr_p"])

            if dp > s["high"]:
                s["high"] = dp
            if dp < s["low"]:
                s["low"] = dp

            sync_data[name] = {
                "종목": name,
                "변동%": round(((dp - s["base_p"]) / s["base_p"]) * 100, 2),
                "time": current_candle_time,
                "open": int(s["open"]),
                "high": int(s["high"]),
                "low": int(s["low"]),
                "close": dp
            }

        try:
            requests.patch(FIREBASE_LIVE_URL, json=sync_data, timeout=1)
        except:
            pass

        time.sleep(0.5)

# --- 4. 스텔스 수집 엔진 ---
def clock_master():

    log("🚀 [엔진 시작] 스텔스 수집 모드 가동")

    UA_LIST = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36"
    ]

    while True:

        now = datetime.now(KST)

        wait_sec = (60 - now.second) + 30
        if wait_sec > 60:
            wait_sec -= 60

        time.sleep(wait_sec)

        global lock_engine, current_candle_time

        lock_engine = True

        if now.hour == 0 and now.minute == 0:
            for name in stock_names:
                data_map[name]["base_p"] = data_map[name]["curr_p"]

        prev_ts = current_candle_time
        current_candle_time = (int(time.time()) // 60) * 60

        history_batch = {}

        for name in stock_names:

            s = data_map[name]

            history_batch[f"{name}/{prev_ts}"] = {
                "time": prev_ts,
                "open": int(s["open"]),
                "high": int(s["high"]),
                "low": int(s["low"]),
                "close": snap_to_tick(s["curr_p"])
            }

            price = float(snap_to_tick(s["curr_p"]))
            s["open"] = price
            s["high"] = price
            s["low"] = price

        def push_history():
            try:
                requests.patch(f"{FIREBASE_CHART_URL}.json", json=history_batch, timeout=5)
            except:
                pass

        Thread(target=push_history, daemon=True).start()

        subset = [stock_queue.popleft() for _ in range(5)]
        stock_queue.extend(subset)

        log(f"🔔 [수집 시도] 대상: {', '.join(subset)}")

        try:

            headers = {
                'User-Agent': random.choice(UA_LIST),
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': 'https://trends.google.co.kr/trends/'
            }

            time.sleep(random.uniform(5, 15))

            pytrends = TrendReq(
                hl='ko-KR',
                tz=540,
                requests_args={'headers': headers, 'timeout': 30}
            )

            pytrends.build_payload(subset, timeframe='now 1-H', geo='KR')

            df = pytrends.interest_over_time()

            if not df.empty:

                for name in subset:

                    val = int(df[name].iloc[-1]) if name in df.columns else random.randint(58, 62)

                    target_ratio = (val - 60) * 0.005

                    s = data_map[name]

                    s["target_p"] = s["base_p"] * (1 + target_ratio)
                    s["last_update_ts"] = time.time()

                    log(f"   ㄴ {name}: 점수 {val} -> 목표 {target_ratio*100:+.2f}% 설정")

            else:
                raise Exception("Empty Data")

        except Exception:

            log("   ❌ [수집 실패] 구글 차단 감지 - 임의 보정 작동")

            for name in subset:

                s = data_map[name]

                s["target_p"] = s["base_p"] * (1 + random.uniform(-0.02, 0.02))
                s["last_update_ts"] = time.time()

                log(f"   ㄴ {name}: 보정 목표 {((s['target_p']/s['base_p'])-1)*100:+.2f}%")

        lock_engine = False

# --- 5. 실행 ---
if __name__ == "__main__":

    port = int(os.environ.get("PORT", 8080))

    Thread(target=lambda: app.run(
        host='0.0.0.0',
        port=port,
        use_reloader=False
    ), daemon=True).start()

    Thread(target=physics_engine, daemon=True).start()

    clock_master()
