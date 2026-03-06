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

# --- 초기 설정 ---
warnings.filterwarnings('ignore')
KST = timezone(timedelta(hours=9))
app = Flask(__name__)

def log(msg):
    print(f"[{datetime.now(KST).strftime('%H:%M:%S')}] {msg}", flush=True)
    sys.stdout.flush()

@app.route('/')
def home(): return "TrendDoc Physics Engine V50.1 Active"

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

# [물리 엔진] 백그라운드 실행 (로그 없음)
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
            
            # 모멘텀 반전 로직 (트렌드 강도에 따라 방향 전환)
            if random.random() < (s["reversal_count"] / 840.0): 
                s["momentum_dir"] *= -1
            
            # 평균 회귀 및 가우시안 노이즈 결합
            dist_to_target = s["target_p"] - s["curr_p"]
            gravity = dist_to_target / time_left
            noise = np.random.normal(0, s["base_p"] * s["volatility"])
            
            s["velocity"] = (s["velocity"] * 0.85) + (gravity * 0.15) + (noise * s["momentum_dir"])
            s["curr_p"] += s["velocity"]
            
            # 7분 도달 시 수렴 보정 (+-0.15% 랜덤)
            if time_left < 5:
                s["curr_p"] = s["target_p"] * random.uniform(0.9985, 1.0015)
            
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

# [시간 관리 및 구글 수집]
def clock_master():
    global lock_engine, current_candle_time
    log("🚀 V50.1 물리 엔진 가동 시작")
    pytrends = TrendReq(hl='ko-KR', tz=540)

    while True:
        now = datetime.now(KST)
        # 매분 30초 대기 (예: 20:10:30)
        wait_sec = (60 - now.second) + 30
        if wait_sec > 60: wait_sec -= 60
        time.sleep(wait_sec)
        
        lock_engine = True
        
        # 한국시간 00시 일일 리셋 (0% 시작)
        if now.hour == 0 and now.minute == 0:
            log("🌅 [자정 리셋] 전날 종가를 새로운 기준가로 설정합니다.")
            for name in stock_names:
                data_map[name]["base_p"] = data_map[name]["curr_p"]
                data_map[name]["open"] = data_map[name]["curr_p"]
                data_map[name]["high"] = data_map[name]["curr_p"]
                data_map[name]["low"] = data_map[name]["curr_p"]

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

        # 5개 종목 수집 및 로그 출력
        subset = [stock_queue.popleft() for _ in range(5)]
        stock_queue.extend(subset)
        
        log(f"📍 [업데이트 대상] {', '.join(subset)}")
        
        try:
            pytrends.build_payload(subset, timeframe='now 1-H', geo='KR')
            df = pytrends.interest_over_time()
            
            for name in subset:
                val = int(df[name].iloc[-1]) if (not df.empty and name in df.columns) else random.randint(58, 62)
                if val == 0: val = random.randint(58, 62)
                
                # 점수 환산: 1점 = 0.5% (60점 기준)
                target_ratio = (val - 60) * 0.005
                s = data_map[name]
                s["target_p"] = s["base_p"] * (1 + target_ratio)
                s["last_update_ts"] = time.time()
                s["reversal_count"] = random.randint(0, 5) 
                
                # 로그 출력: 업데이트된 종목과 목표치
                log(f"   ㄴ {name}: 현재 목표 {target_ratio*100:+.2f}% (수렴 시작)")
        except:
            log("⚠️ 구글 수집 실패 (자동 보정 모드)")
            for name in subset:
                s = data_map[name]
                s["target_p"] = s["base_p"] * (1 + random.uniform(-0.01, 0.01))
                s["last_update_ts"] = time.time()
            
        lock_engine = False

if __name__ == "__main__":
    Thread(target=lambda: app.run(host='0.0.0.0', port=8080), daemon=True).start()
    Thread(target=physics_engine, daemon=True).start()
    clock_master()
