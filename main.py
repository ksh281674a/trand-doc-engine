import gspread
import time
from datetime import datetime, timedelta
from pytrends.request import TrendReq
import firebase_admin
from firebase_admin import credentials, db
from apscheduler.schedulers.background import BackgroundScheduler

# 1. 초기 설정 (Firebase & Google Sheets)
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred, {'databaseURL': 'YOUR_FIREBASE_URL'})
gc = gspread.service_account(filename='google_sheets_key.json')
sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1e6OWQVszOLxyfaQxPwFT7MyH5yOOF1dmhooVqwK2CTA/edit")
worksheet = sh.get_worksheet(0)

pytrends = TrendReq(hl='ko-KR', tz=540)

def fetch_and_update():
    # 시트에서 평균점수 로드 (예: A열 종목명, B열 평균점수)
    records = worksheet.get_all_records()
    
    for row in records:
        ticker = row['종목명']
        baseline = float(row['평균점수'])
        
        try:
            # 구글 트렌드 수집
            pytrends.build_payload([ticker], timeframe='now 1-H')
            df = pytrends.interest_over_time()
            current_score = df[ticker].iloc[-1] if not df.empty else baseline
            
            # 수익률 계산 식: (현재점수 - 평균점수) * 0.5
            target_yield = (current_score - baseline) * 0.5
            
            # Firebase 업데이트 (목표치와 업데이트 시각 전송)
            ref = db.reference(f'trends/{ticker}')
            ref.update({
                'target_yield': target_yield,
                'last_update': datetime.now().isoformat(),
                'next_update': (datetime.now() + timedelta(minutes=7)).replace(second=0, microsecond=0).isoformat()
            })
        except Exception as e:
            print(f"Error updating {ticker}: {e}")

# 2. 정시(00초) 스케줄러 설정
scheduler = BackgroundScheduler(timezone="Asia/Seoul")
# 7분 간격으로 매 정각(00초)에 실행
scheduler.add_job(fetch_and_update, 'cron', minute='*/7', second='0')
scheduler.start()

# FastAPI/Flask 엔드포인트 생략 (Background에서 계속 실행됨)
