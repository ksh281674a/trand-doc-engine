import time
import requests
import random
import numpy as np
import warnings
from datetime import datetime, timedelta, timezone
from threading import Thread
from collections import deque
from flask import Flask
from pytrends.request import TrendReq

# --- 설정 ---
warnings.filterwarnings('ignore')
KST = timezone(timedelta(hours=9))
app = Flask(__name__)

@app.route('/')
def home(): return "TrendDoc Engine V40.8 Active"

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
    data_map[name] = {
        "base_p": float(STOCK_BASES[name]), "curr_p": float(STOCK_BASES[name]),
        "open": float(STOCK_BASES[name]), "high": float(STOCK_BASES[name]),
        "low": float(STOCK_BASES[name]), "target_p": float(STOCK_BASES[name]),
        "velocity": 0.0, "last_update_ts": time.time(), "updated": False
    }

def snap_to_tick(price):
    if price >= 100000: return int(round(price / 100) * 100)
    elif price >= 50000: return int(round(price / 50) * 50)
    else: return int(round(price / 10) * 10)

# [1] 물리 엔진 로그 (30초마다 요약 보고)
def physics_engine():
    global lock_engine, current_candle_time
    last_log_time = time.time()
    
    while True:
        if lock_engine:
            time.sleep(0.1)
            continue
        sync_data = {}
        now_ts = time.time()
        for name in stock_names:
            s = data_map[name]
            elapsed = now_ts - s["last_update_ts"]
            time_remaining = max(1.0, 420.0 - elapsed)
            req_vel = (s["target_p"] - s["curr_p"]) / time_remaining
            s["velocity"] = s["velocity"] * 0.85 + req_vel * 0.15
            noise = np.random.normal(0, s["base_p"] * 0.0001)
            s["curr_p"] += s["velocity"] + noise
            dp = snap_to_tick(s["curr_p"])
            if dp > s["high"]: s["high"] = dp
            if dp < s["low"]: s["low"] = dp
            cp = round(((dp - s["base_p"]) / s["base_p"]) * 100, 2)
            sync_data[name] = {
                "종목": name, "변동%": cp, "time": current_candle_time,
                "open": int(s["open"]), "high": int(s["high"]), "low": int(s["low"]), "close": dp
            }
        
        # 30초마다 현재가 로그 출력
        if time.time() - last_log_time > 30:
            sample = random.choice(stock_names)
            print(f"📊 [실시간] {sample}: {sync_data[sample]['close']}원 ({sync_data[sample]['변동%']}% 변동 중)")
            last_log_time = time.time()

        try: requests.patch(FIREBASE_LIVE_URL, json=sync_data, timeout=0.8)
        except: pass
        time.sleep(0.5)

# [2] 구글 트렌드 수집 로그 (수집 시마다 출력)
def clock_master():
    global lock_engine, current_candle_time
    pytrends = TrendReq(hl='ko-KR', tz=540, timeout=(10, 25), retries=5)
    print("🚀 TrendDoc 엔진 가동 시작!")

    while True:
        now = datetime.now()
        wait = 60 - now.second - (now.microsecond / 1000000.0)
        time.sleep(wait)
        
        lock_engine = True
        prev_ts = current_candle_time
        current_candle_time = (int(time.time()) // 60) * 60
        
        history_batch = {}
        reset_live = {}
        for name in stock_names:
            s = data_map[name]
            history_batch[f"{name}/{prev_ts}"] = {
                "time": prev_ts, "open": int(s["open"]), "high": int(s["high"]), 
                "low": int(s["low"]), "close": snap_to_tick(s["curr_p"])
            }
            new_v = float(snap_to_tick(s["curr_p"]))
            s["open"] = s["high"] = s["low"] = new_v
            reset_live[name] = {
                "종목": name, "변동%": round(((new_v-s["base_p"])/s["base_p"])*100, 2),
                "time": current_candle_time, "open": int(new_v), "high": int(new_v), 
                "low": int(new_v), "close": int(new_v)
            }
        
        requests.patch(FIREBASE_LIVE_URL, json=reset_live)
        Thread(target=lambda: requests.patch(f"{FIREBASE_CHART_URL}.json", json=history_batch)).start()
        print(f"\n🔔 [{datetime.now(KST).strftime('%H:%M:%S')}] 정각: {len(stock_names)}종목 봉 교체 완료")

        subset = [stock_queue.popleft() for _ in range(5)]
        stock_queue.extend(subset)
        
        try:
            print(f"🔍 [수집중] {', '.join(subset)}...")
            time.sleep(random.uniform(2, 5))
            pytrends.build_payload(subset, timeframe='now 1-H', geo='KR')
            df = pytrends.interest_over_time()
            if not df.empty:
                for name in subset:
                    val = int(df[name].iloc[-1]) if name in df.columns else 60
                    if val == 0: val = 60
                    
                    drift_pct = (val - 60) * 0.005 # 목표 변동률
                    data_map[name]["target_p"] = data_map[name]["base_p"] * (1 + drift_pct)
                    data_map[name]["last_update_ts"] = time.time()
                    data_map[name]["updated"] = True
                    
                    # 로그 출력: 종목명, 점수, 목표 변동%
                    print(f" ✅ [완료] {name}: {val}점 확보 -> 목표가 {drift_pct*100:+.2f}% 설정")
        except Exception as e:
            print(f" ⚠️ [에러] 구글 수집 실패: {e}")
            
        lock_engine = False

if __name__ == "__main__":
    # 포트 8080 고정 (Cloudtype용)
    Thread(target=lambda: app.run(host='0.0.0.0', port=8080), daemon=True).start()
    Thread(target=physics_engine, daemon=True).start()
    clock_master()
