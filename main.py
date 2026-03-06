import time
import requests
import random
import numpy as np
import warnings
import sys
from datetime import datetime, timedelta, timezone
from threading import Thread
from collections import deque
from flask import Flask
from pytrends.request import TrendReq

# --- 설정 ---
warnings.filterwarnings('ignore')
KST = timezone(timedelta(hours=9))
app = Flask(__name__)

def log(msg):
    print(f"[{datetime.now(KST).strftime('%H:%M:%S')}] {msg}", flush=True)
    sys.stdout.flush()

@app.route('/')
def home(): return "TrendDoc Anti-Block Engine V50.3"

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
        "base_p": bp, "curr_p": bp, "open": bp, "high": bp, "low": bp,
        "target_p": bp, "velocity": 0.0, "last_update_ts": time.time(),
        "volatility": 0.0002, "momentum_dir": 1, "reversal_count": 0
    }

def snap_to_tick(price):
    if price >= 100000: return int(round(price / 100) * 100)
    elif price >= 50000: return int(round(price / 50) * 50)
    else: return int(round(price / 10) * 10)

# 물리 엔진 (백그라운드 처리, 로그 없음)
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
            
            # 모멘텀 반전
            if random.random() < (s["reversal_count"] / 840.0): s["momentum_dir"] *= -1
            
            dist = s["target_p"] - s["curr_p"]
            gravity = dist / time_left
            noise = np.random.normal(0, s["base_p"] * s["volatility"])
            
            s["velocity"] = (s["velocity"] * 0.85) + (gravity * 0.15) + (noise * s["momentum_dir"])
            s["curr_p"] += s["velocity"]
            
            if time_left < 5: s["curr_p"] = s["target_p"] * random.uniform(0.9985, 1.0015)
            
            dp = snap_to_tick(s["curr_p"])
            if dp > s["high"]: s["high"] = dp
            if dp < s["low"]: s["low"] = dp
            
            sync_data[name] = {
                "종목": name, "변동%": round(((dp - s["base_p"]) / s["base_p"]) * 100, 2),
                "time": current_candle_time, "open": int(s["open"]), "high": int(s["high"]), "low": int(s["low"]), "close": dp
            }
        try: requests.patch(FIREBASE_LIVE_URL, json=sync_data, timeout=0.8)
        except: pass
        time.sleep(0.5)

def clock_master():
    log("🚀 수집 방어 강화 엔진 가동")
    
    UA_LIST = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15) Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) Firefox/123.0"
    ]

    while True:
        now = datetime.now(KST)
        # 매분 35초에 작업 시작 (구글 봇 탐지 회피용 지연)
        wait_sec = (60 - now.second) + 35
        if wait_sec > 60: wait_sec -= 60
        time.sleep(wait_sec)
        
        global lock_engine, current_candle_time
        lock_engine = True
        
        # 자정 리셋
        if now.hour == 0 and now.minute == 0:
            for name in stock_names:
                data_map[name]["base_p"] = data_map[name]["curr_p"]
                data_map[name]["open"] = data_map[name]["high"] = data_map[name]["low"] = data_map[name]["curr_p"]

        prev_ts = current_candle_time
        current_candle_time = (int(time.time()) // 60) * 60
        
        history_batch = {}
        for name in stock_names:
            s = data_map[name]
            history_batch[f"{name}/{prev_ts}"] = {
                "time": prev_ts, "open": int(s["open"]), "high": int(s["high"]), "low": int(s["low"]), "close": snap_to_tick(s["curr_p"])
            }
            s["open"] = s["high"] = s["low"] = float(snap_to_tick(s["curr_p"]))
        Thread(target=lambda: requests.patch(f"{FIREBASE_CHART_URL}.json", json=history_batch)).start()

        subset = [stock_queue.popleft() for _ in range(5)]
        stock_queue.extend(subset)

        # 수집 시도 (세션 초기화 포함)
        success = False
        try:
            # 매번 새로운 세션 할당
            pytrends = TrendReq(hl='ko-KR', tz=540, requests_args={'headers': {'User-Agent': random.choice(UA_LIST)}, 'timeout': 20})
            pytrends.build_payload(subset, timeframe='now 1-H', geo='KR')
            df = pytrends.interest_over_time()
            
            if not df.empty:
                log(f"🔔 [정각 수집] 대상: {', '.join(subset)}")
                for name in subset:
                    val = int(df[name].iloc[-1]) if name in df.columns else random.randint(58, 62)
                    target_ratio = (val - 60) * 0.005
                    s = data_map[name]
                    s["target_p"] = s["base_p"] * (1 + target_ratio)
                    s["last_update_ts"] = time.time()
                    s["reversal_count"] = random.randint(0, 5)
                    log(f"   ㄴ {name}: 목표 변동 {target_ratio*100:+.2f}%")
                success = True
        except: pass
        
        if not success:
            log(f"⚠️ 구글 수집 지연 (임의 보정 실행)")
            for name in subset:
                target_ratio = random.uniform(-0.015, 0.015)
                s = data_map[name]
                s["target_p"] = s["base_p"] * (1 + target_ratio)
                s["last_update_ts"] = time.time()
                log(f"   ㄴ {name}: 보정 목표 {target_ratio*100:+.2f}%")
        
        lock_engine = False

if __name__ == "__main__":
    Thread(target=lambda: app.run(host='0.0.0.0', port=8080), daemon=True).start()
    Thread(target=physics_engine, daemon=True).start()
    clock_master()
