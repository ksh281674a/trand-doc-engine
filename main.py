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

warnings.filterwarnings("ignore")

KST = timezone(timedelta(hours=9))
app = Flask(__name__)

# ---------------- 로그 ----------------

def log(msg):
    print(f"[{datetime.now(KST).strftime('%H:%M:%S')}] {msg}", flush=True)

@app.route("/")
def home():
    return "TrendDoc Engine V60 Running"

# ---------------- 환경 ----------------

port = int(os.environ.get("PORT", 8080))

FIREBASE_LIVE_URL = "https://trand-doc-default-rtdb.firebaseio.com/live_data.json"
FIREBASE_CHART_URL = "https://trand-doc-default-rtdb.firebaseio.com/chart_history.json"

session = requests.Session()

# ---------------- 종목 ----------------

STOCK_BASES = {
    '유튜브':187420,'구글':224160,'네이버':172330,'쿠팡':34570,'넷플릭스':89450,
    '인스타그램':42680,'배달의민족':51240,'치지직':15240,'틱톡':9740,'하이브':195640,
    '카카오':38450,'네이버웹툰':55180,'라이엇':45120,'스팀':62340,'티빙':58420,
    '멜론':52150,'넥슨':24180,'유튜브 뮤직':58120,'무신사':65230,'테무':21870,
    'SM':38250,'X (트위터)':49820,'SOOP':51850,'쿠팡플레이':45180,'카카오페이지':40150,
    '애플뮤직':35240,'요기요':29850,'알리':28150,'YG':35420,'JYP':7450,
    '다음':35120,'MS (Bing)':28140,'쿠팡이츠':45320,'왓챠':1248
}

stock_names=list(STOCK_BASES.keys())
stock_queue=deque(stock_names)

current_candle_time=(int(time.time())//60)*60
lock_engine=False

# ---------------- 데이터맵 ----------------

data_map={}

for name in stock_names:

    bp=float(STOCK_BASES[name])

    data_map[name]={
        "base_p":bp,
        "curr_p":bp,
        "open":bp,
        "high":bp,
        "low":bp,
        "target_p":bp,
        "velocity":0.0,
        "last_update_ts":time.time(),
        "volatility":0.0002
    }

# ---------------- 틱 ----------------

def snap(price):

    if price>=100000:
        return int(round(price/100)*100)

    elif price>=50000:
        return int(round(price/50)*50)

    else:
        return int(round(price/10)*10)

# ---------------- 물리엔진 ----------------

def physics_engine():

    global lock_engine,current_candle_time

    while True:

        if lock_engine:
            time.sleep(0.1)
            continue

        sync={}

        now=time.time()

        for name in stock_names:

            s=data_map[name]

            elapsed=now-s["last_update_ts"]
            time_left=max(1.0,420-elapsed)

            dist=s["target_p"]-s["curr_p"]

            gravity=dist/time_left

            noise=np.random.normal(0,s["base_p"]*s["volatility"])

            s["velocity"]=(s["velocity"]*0.85)+(gravity*0.15)+noise

            s["curr_p"]+=s["velocity"]

            dp=snap(s["curr_p"])

            if dp>s["high"]:
                s["high"]=dp

            if dp<s["low"]:
                s["low"]=dp

            sync[name]={
                "종목":name,
                "변동%":round(((dp-s["base_p"])/s["base_p"])*100,2),
                "time":current_candle_time,
                "open":int(s["open"]),
                "high":int(s["high"]),
                "low":int(s["low"]),
                "close":dp
            }

        try:
            session.patch(FIREBASE_LIVE_URL,json=sync,timeout=2)
        except:
            pass

        time.sleep(0.5)

# ---------------- Google Trends ----------------

UA_LIST=[
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0",
"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/121.0.0.0",
"Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0"
]

def get_trend(subset):

    for retry in range(3):

        try:

            headers={
                "User-Agent":random.choice(UA_LIST),
                "Accept-Language":"ko-KR,ko;q=0.9"
            }

            pytrends=TrendReq(
                hl="ko-KR",
                tz=540,
                requests_args={"headers":headers,"timeout":20}
            )

            pytrends.build_payload(subset,timeframe="now 1-H",geo="KR")

            df=pytrends.interest_over_time()

            if not df.empty:
                return df

        except Exception as e:

            log(f"구글 재시도 {retry+1}/3")

            time.sleep(random.uniform(5,10))

    return None

# ---------------- 클럭 ----------------

def clock_master():

    global lock_engine,current_candle_time

    log("🚀 TrendDoc 엔진 시작")

    while True:

        now=datetime.now(KST)

        wait=(60-now.second)+30
        if wait>60:
            wait-=60

        time.sleep(wait)

        lock_engine=True

        prev=current_candle_time
        current_candle_time=(int(time.time())//60)*60

        history={}

        for name in stock_names:

            s=data_map[name]

            history[f"{name}/{prev}"]={
                "time":prev,
                "open":int(s["open"]),
                "high":int(s["high"]),
                "low":int(s["low"]),
                "close":snap(s["curr_p"])
            }

            p=float(snap(s["curr_p"]))

            s["open"]=p
            s["high"]=p
            s["low"]=p

        Thread(target=lambda:session.patch(FIREBASE_CHART_URL,json=history)).start()

        subset=[stock_queue.popleft() for _ in range(5)]
        stock_queue.extend(subset)

        log(f"수집 대상: {subset}")

        df=get_trend(subset)

        if df is not None:

            for name in subset:

                val=int(df[name].iloc[-1]) if name in df else random.randint(58,62)

                ratio=(val-60)*0.005

                s=data_map[name]

                s["target_p"]=s["base_p"]*(1+ratio)
                s["last_update_ts"]=time.time()

                log(f"{name} -> {val}")

        else:

            log("Google 차단 → fallback")

            for name in subset:

                s=data_map[name]

                s["target_p"]=s["base_p"]*(1+random.uniform(-0.02,0.02))
                s["last_update_ts"]=time.time()

        lock_engine=False

# ---------------- 실행 ----------------

if __name__=="__main__":

    Thread(target=lambda:app.run(
        host="0.0.0.0",
        port=port,
        use_reloader=False
    ),daemon=True).start()

    Thread(target=physics_engine,daemon=True).start()

    clock_master()
