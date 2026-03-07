import time
import os
import random
import numpy as np
from datetime import datetime, timedelta
from pytrends.request import TrendReq
import firebase_admin
from firebase_admin import credentials, db
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

app = Flask(__name__)

# ---------------------------------------------------------
# 1. Firebase 인증 (serviceAccountKey.json 파일 경로 보정)
# ---------------------------------------------------------
# [수정됨] 서버 환경에서도 파일을 확실히 찾을 수 있도록 경로를 자동 계산합니다.
base_path = os.path.dirname(os.path.abspath(__file__))
key_path = os.path.join(base_path, "serviceAccountKey.json")

if not os.path.exists(key_path):
    print(f"❌ 오류: '{key_path}' 파일을 찾을 수 없습니다!")
    print("GitHub에 serviceAccountKey.json 파일이 main.py와 같은 위치에 있는지 확인하세요.")
else:
    cred = credentials.Certificate("/절대/경로/serviceAccountKey.json")
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://trand-doc-default-rtdb.firebaseio.com/'
    })

pytrends = TrendReq(hl='ko-KR', tz=540)

# ---------------------------------------------------------
# 2. [데이터 삽입] 34개 종목명 및 평균 점수 (기준점수)
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
# 3. 실시간 틱 엔진 (4대 알고리즘)
# ---------------------------------------------------------
def generate_ticks():
    """2초마다 current_yield를 꿈틀거리게 업데이트"""
    all_trends = db.reference('trends').get()
    if not all_trends: return

    updates = {}
    for ticker, data in all_trends.items():
        try:
            target = data.get('target_yield', 0.0)
            current = data.get('current_yield', 0.0)
            
            # 가우시안 노이즈 + 평균 회귀 + 변동성 클러스터링
            noise = np.random.normal(0, 0.012)
            pull = (target - current) * 0.06
            clustering = 1.4 if abs(target - current) > 0.5 else 1.0
            
            next_tick = current + (noise * clustering) + pull
            updates[f'{ticker}/current_yield'] = round(next_tick, 4)
        except: continue
    
    if updates:
        db.reference('trends').update(updates)

# ---------------------------------------------------------
# 4. 자정 리셋 & 7분 주기 수집 (로그 강화)
# ---------------------------------------------------------
def daily_midnight_reset():
    """KST 00:00:00 - 어제 마지막 점수가 오늘의 기준가(0%)가 됨"""
    print(f"\n🌕 [Midnight Reset] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    all_trends = db.reference('trends').get()
    if not all_trends: return

    for ticker in TICKERS_DATA.keys():
        data = all_trends.get(ticker, {})
        # 어제 마지막으로 수집된 점수 (없으면 초기 평균점수 사용)
        last_score = data.get('last_score', TICKERS_DATA[ticker])
        
        db.reference(f'trends/{ticker}').update({
            'baseline': last_score,
            'target_yield': 0.0,
            'current_yield': 0.0,
            'reset_at': datetime.now().isoformat()
        })
        print(f" > {ticker}: 기준가 {last_score} 갱신 (수익률 0% 초기화)")

def fetch_and_update():
    """7분 주기 수집 - 1분 5개 제한 및 업데이트 로그 출력"""
    now = datetime.now()
    print(f"\n📊 [Trend Update] {now.strftime('%H:%M:%S')} 수집 시작")
    print("-" * 50)

    for ticker in TICKERS_DATA.keys():
        try:
            ref = db.reference(f'trends/{ticker}')
            data = ref.get()
            # Firebase에 저장된 기준가 로드 (자정 리셋 시 갱신된 값)
            baseline = data.get('baseline', TICKERS_DATA[ticker]) if data else TICKERS_DATA[ticker]
            
            # 구글 트렌드 API 호출
            pytrends.build_payload([ticker], timeframe='now 1-H')
            df = pytrends.interest_over_time()
            current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
            
            # 수익률 계산: (현재 점수 - 기준가) * 0.5
            target_yield = (current_score - baseline) * 0.5
            
            ref.update({
                'last_score': current_score,
                'target_yield': target_yield,
                'last_update': now.isoformat()
            })
            
            # 로그 출력
            status = "▲" if target_yield > 0 else "▼" if target_yield < 0 else "-"
            print(f" ✅ [{ticker}] {status} {target_yield:+.2f}% (점수: {current_score} / 기준: {baseline})")
            
            time.sleep(12) # 💡 속도 제한 (1분 5개)
            
        except Exception as e:
            print(f" ❌ [{ticker}] 오류: {e}")
            time.sleep(20)

# ---------------------------------------------------------
# 5. 초기화 및 앱 가동
# ---------------------------------------------------------
def initialize_app():
    print("🚀 [System] Firebase 초기 데이터 확인 중...")
    for ticker, avg in TICKERS_DATA.items():
        ref = db.reference(f'trends/{ticker}')
        if not ref.get():
            ref.set({
                'baseline': avg, 
                'last_score': avg, 
                'target_yield': 0.0, 
                'current_yield': 0.0
            })
    print("✅ 데이터 준비 완료.")

scheduler = BackgroundScheduler(timezone="Asia/Seoul")
scheduler.add_job(fetch_and_update, 'cron', minute='*/7', second='0')
scheduler.add_job(generate_ticks, 'interval', seconds=2)
scheduler.add_job(daily_midnight_reset, 'cron', hour=0, minute=0)

if __name__ == "__main__":
    initialize_app()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
