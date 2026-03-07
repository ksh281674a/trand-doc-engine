import gspread
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
# 1. 초기 설정 및 인증
# ---------------------------------------------------------
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://trand-doc-default-rtdb.firebaseio.com/'
})

gc = gspread.service_account(filename='google_sheets_key.json')
sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1e6OWQVszOLxyfaQxPwFT7MyH5yOOF1dmhooVqwK2CTA/edit")
worksheet = sh.get_worksheet(0)

pytrends = TrendReq(hl='ko-KR', tz=540)

# ---------------------------------------------------------
# 2. 실시간 틱 엔진 (4대 알고리즘 반영)
# ---------------------------------------------------------
def generate_ticks():
    """
    매 2초마다 모든 종목의 실시간 '틱'을 생성하여 Firebase에 전송.
    가우시안 노이즈, 평균 회귀, 모멘텀 알고리즘 적용.
    """
    all_trends = db.reference('trends').get()
    if not all_trends: return

    for ticker, data in all_trends.items():
        try:
            target_yield = data.get('target_yield', 0.0)
            current_yield = data.get('current_yield', 0.0)
            
            # [알고리즘 1] 가우시안 노이즈 (미세 떨림)
            # 수렴 시점(7분 끝)에 가까운지 체크 (단, 여기선 단순 구현을 위해 0.02 고정)
            noise = np.random.normal(0, 0.015) 
            
            # [알고리즘 2] 평균 회귀 (Mean Reversion)
            # 현재가가 목표가와 멀어지면 목표가 방향으로 당기는 인력 (강도 0.05)
            pull = (target_yield - current_yield) * 0.05
            
            # [알고리즘 3] 변동성 클러스터링
            # 목표치와 현재 차이가 크면 노이즈를 키움
            clustering = 1.5 if abs(target_yield - current_yield) > 0.5 else 1.0
            
            # 새로운 틱 계산
            next_yield = current_yield + (noise * clustering) + pull
            
            # Firebase 실시간 필드만 업데이트 (부하 최소화)
            db.reference(f'trends/{ticker}').update({
                'current_yield': round(next_yield, 4)
            })
        except:
            continue

# ---------------------------------------------------------
# 3. 자정 리셋 및 정시 수집 로직
# ---------------------------------------------------------
def daily_midnight_reset():
    """KST 00:00:00 시가 초기화 및 기준가 갱신"""
    print(f"\n🌕 [Midnight Reset] {datetime.now()}")
    all_trends = db.reference('trends').get()
    if not all_trends: return

    for ticker, data in all_trends.items():
        last_day_score = data.get('last_score', data.get('baseline', 0))
        db.reference(f'trends/{ticker}').update({
            'baseline': last_day_score,
            'target_yield': 0.0,
            'current_yield': 0.0, # 시가 0% 초기화
            'reset_at': datetime.now().isoformat()
        })
        print(f" > {ticker} 리셋 완료")

def fetch_and_update():
    """7분 주기 목표치 수집 (1분 5개 제한)"""
    now = datetime.now()
    all_trends = db.reference('trends').get()
    if not all_trends: return

    print(f"\n📊 [Update Start] {now.strftime('%H:%M:%S')}")
    for ticker, data in all_trends.items():
        try:
            baseline = data.get('baseline', 0)
            pytrends.build_payload([ticker], timeframe='now 1-H')
            df = pytrends.interest_over_time()
            current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
            
            target_yield = (current_score - baseline) * 0.5
            
            db.reference(f'trends/{ticker}').update({
                'last_score': current_score,
                'target_yield': target_yield,
                'last_update': now.isoformat(),
                'next_update': (now + timedelta(minutes=7)).replace(second=0, microsecond=0).isoformat()
            })
            print(f"[{ticker}] 목표수익률: {target_yield:+.2f}%")
            time.sleep(12) # 1분 5개 속도제한
        except Exception as e:
            print(f"Error {ticker}: {e}")
            time.sleep(20)

# ---------------------------------------------------------
# 4. 초기화 및 스케줄러 실행
# ---------------------------------------------------------
def initialize_app():
    print("🚀 [System] 앱 초기화...")
    records = worksheet.get_all_records()
    for row in records:
        ticker = row['종목명']
        avg_score = float(row['평균점수'])
        ref = db.reference(f'trends/{ticker}')
        if not ref.get():
            ref.set({
                'baseline': avg_score, 'last_score': avg_score,
                'target_yield': 0.0, 'current_yield': 0.0
            })

scheduler = BackgroundScheduler(timezone="Asia/Seoul")

# 미션 1: 7분 주기 정시 수집
scheduler.add_job(fetch_and_update, 'cron', minute='*/7', second='0')

# 미션 2: 2초마다 실시간 틱(알고리즘) 생성하여 Firebase 전송
scheduler.add_job(generate_ticks, 'interval', seconds=2)

# 미션 3: 자정 리셋 (시가 0% 초기화)
scheduler.add_job(daily_midnight_reset, 'cron', hour=0, minute=0, second=0)

if __name__ == "__main__":
    initialize_app()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
