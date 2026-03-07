import time
import os
import json
import random
import numpy as np
from datetime import datetime, timedelta
import pytz
from pytrends_modern import TrendReq
import firebase_admin
from firebase_admin import credentials, db
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

app = Flask(__name__)
KST = pytz.timezone('Asia/Seoul')

# ---------------------------------------------------------
# 1. Firebase 인증
# ---------------------------------------------------------
cred = credentials.Certificate(json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"]))
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://trand-doc-default-rtdb.firebaseio.com/'
})

# ---------------------------------------------------------
# 2. 34개 종목 데이터
# ---------------------------------------------------------
TICKERS_DATA = {
    "카카오": 42, "인스타그램": 55, "틱톡": 48, "X (트위터)": 50,
    "유튜브": 89, "치지직": 68, "SOOP": 52, "쿠팡": 78,
    "알리": 74, "무신사": 65, "테무": 72, "네이버": 85,
    "구글": 92, "다음": 35, "MS (Bing)": 28, "배달의민족": 62,
    "쿠팡이츠": 45, "요기요": 30, "유튜브 뮤직": 58, "멜론": 52,
    "애플뮤직": 35, "라이엇": 45, "스팀": 42, "넥슨": 48,
    "넷플릭스": 70, "티빙": 58, "쿠팡플레이": 45, "왓챠": 25,
    "네이버웹툰": 55, "카카오페이지": 40, "하이브": 48, "SM": 38,
    "YG": 35, "JYP": 32
}

# ---------------------------------------------------------
# 3. 실시간 알고리즘 (2초마다 실행)
# ---------------------------------------------------------
def generate_ticks():
    all_trends = db.reference('trends').get()
    if not all_trends: return
    updates = {}
    for ticker, data in all_trends.items():
        try:
            target = data.get('target_yield', 0.0)
            current = data.get('current_yield', 0.0)
            noise = np.random.normal(0, 0.012)
            pull = (target - current) * 0.06
            next_tick = current + (noise * 1.0) + pull
            updates[f'{ticker}/current_yield'] = round(next_tick, 4)
        except:
            continue
    if updates:
        db.reference('trends').update(updates)

def daily_midnight_reset():
    all_trends = db.reference('trends').get()
    if not all_trends: return
    for ticker in TICKERS_DATA.keys():
        data = all_trends.get(ticker, {})
        last_score = data.get('last_score', TICKERS_DATA[ticker])
        db.reference(f'trends/{ticker}').update({
            'baseline': last_score, 'target_yield': 0.0, 'current_yield': 0.0
        })

# 구글 차단 방지용 브라우저 헤더 리스트
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
]

# ---------------------------------------------------------
# 💡 핵심: 통신 시간을 계산하여 오차 없이 정확히 12초 간격 맞춤
# ---------------------------------------------------------
def fetch_and_update():
    now = datetime.now(KST)
    print(f"\n📊 [수집 라운드 시작] {now.strftime('%H:%M:%S')} (정확히 1분에 5개 속도)")

    for ticker in TICKERS_DATA.keys():
        loop_start_time = time.time() # 종목 수집 시작 시간 기록
        
        try:
            ref = db.reference(f'trends/{ticker}')
            data = ref.get()
            baseline = data.get('baseline', TICKERS_DATA[ticker])
            
            current_ua = random.choice(USER_AGENTS)
            pt = TrendReq(
                hl='ko-KR',
                tz=540,
                requests_args={'headers': {'User-Agent': current_ua}}
            )
            
            pt.build_payload([ticker], timeframe='now 1-H')
            df = pt.interest_over_time()
            current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
            target_yield = (current_score - baseline) * 0.5
            
            ref.update({'last_score': current_score, 'target_yield': target_yield})
            now_log = datetime.now(KST)
            print(f" ✅ [{now_log.strftime('%H:%M:%S')}] {ticker}: {target_yield:+.2f}%")
            
        except Exception as e:
            now_log = datetime.now(KST)
            print(f" ❌ [{now_log.strftime('%H:%M:%S')}] {ticker} 오류: {e}")
            
        finally:
            # [오차 보정 타이머] 데이터를 가져오는데 걸린 시간을 잰 후, 12초에서 남은 시간만 대기
            elapsed_time = time.time() - loop_start_time
            sleep_time = 12.0 - elapsed_time
            
            if sleep_time > 0:
                time.sleep(sleep_time) # 남은 시간만큼만 정확히 쉼
            
    print(f"🏁 [수집 라운드 종료] 34개 종목 업데이트 완료 (총 소요시간: 약 6분 48초)")

def initialize_app():
    print("🚀 Firebase 초기화 중...")
    for ticker, avg in TICKERS_DATA.items():
        ref = db.reference(f'trends/{ticker}')
        if not ref.get():
            ref.set({
                'baseline': avg,
                'last_score': avg,
                'target_yield': 0.0,
                'current_yield': 0.0
            })
    print("✅ 모든 데이터 연결 완료!")

# ---------------------------------------------------------
# 4. 스케줄러 (7분 주기)
# ---------------------------------------------------------
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

now_kst = datetime.now(KST)
next_minute = (now_kst + timedelta(minutes=1)).replace(second=0, microsecond=0)

scheduler.add_job(
    fetch_and_update,
    'interval',
    minutes=7,                # 34개 도는데 6분 48초가 걸리므로 7분 주기가 완벽함
    next_run_time=next_minute, 
    max_instances=1,
    coalesce=True
)

scheduler.add_job(generate_ticks, 'interval', seconds=2)
scheduler.add_job(daily_midnight_reset, 'cron', hour=0, minute=0)

if __name__ == "__main__":
    initialize_app()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
