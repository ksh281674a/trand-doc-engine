import gspread
import time
from datetime import datetime, timedelta
from pytrends.request import TrendReq
import firebase_admin
from firebase_admin import credentials, db
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

app = Flask(__name__)

# 1. 초기 설정
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred, {'databaseURL': 'YOUR_FIREBASE_URL'})
gc = gspread.service_account(filename='google_sheets_key.json')
sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1e6OWQVszOLxyfaQxPwFT7MyH5yOOF1dmhooVqwK2CTA/edit")
worksheet = sh.get_worksheet(0)

pytrends = TrendReq(hl='ko-KR', tz=540)

# 전역 변수로 기준가(Baseline) 관리 (서버 메모리에 저장)
current_baselines = {}

def sync_initial_baselines():
    """최초 실행 시 시트에서 기준가를 가져옴"""
    print("--- [System] Initializing Baselines from Google Sheets ---")
    records = worksheet.get_all_records()
    for row in records:
        ticker = row['종목명']
        current_baselines[ticker] = float(row['평균점수'])
    print(f"--- [System] {len(current_baselines)} Tickers Loaded ---\n")

def daily_reset_job():
    """자정(00:00:00) 리셋: 전날 마지막 점수를 오늘용 기준가로 교체"""
    print(f"\n🚀 [Midnight Reset] {datetime.now().strftime('%Y-%m-%d')} 리셋 시작")
    for ticker in current_baselines.keys():
        ref = db.reference(f'trends/{ticker}')
        last_data = ref.get()
        
        if last_data and 'last_score' in last_data:
            new_baseline = last_data['last_score']
            current_baselines[ticker] = new_baseline # 메모리 갱신
            
            # Firebase에 새로운 기준가 저장 및 수익률 0% 초기화
            ref.update({
                'baseline': new_baseline,
                'target_yield': 0.0,
                'reset_time': datetime.now().isoformat()
            })
            print(f" > {ticker}: 새로운 기준가 {new_baseline} 설정 완료 (0% 리셋)")

def fetch_and_update():
    """7분 주기 수집 및 로그 출력"""
    now = datetime.now()
    print(f"\n📊 [Update] {now.strftime('%H:%M:%S')} 데이터 수집 시작")
    print("-" * 50)
    
    # 상위 5개 종목 로그 출력을 위한 카운터
    log_count = 0
    
    for ticker, baseline in current_baselines.items():
        try:
            # 구글 트렌드 수집
            pytrends.build_payload([ticker], timeframe='now 1-H')
            df = pytrends.interest_over_time()
            
            # 데이터가 없을 경우 baseline 유지
            current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
            
            # 4. 수익률 및 가격 환산 수식: (현재 - 기준) * 0.5
            target_yield = (current_score - baseline) * 0.5
            
            # Firebase 업데이트
            ref = db.reference(f'trends/{ticker}')
            ref.update({
                'last_score': current_score,
                'baseline': baseline,
                'target_yield': target_yield,
                'last_update': now.isoformat(),
                'next_update': (now + timedelta(minutes=7)).replace(second=0, microsecond=0).isoformat()
            })

            # Cloudtype 로그 출력 (상위 5개 종목 중심)
            if log_count < 5:
                print(f"[{ticker}] 현재점수: {current_score} | 기준가: {baseline} | 목표수익률: {target_yield:+.2f}%")
                log_count += 1
            
            # 구글 차단 방지를 위한 미세 지연
            time.sleep(1.5)
            
        except Exception as e:
            print(f"Error updating {ticker}: {e}")

    print("-" * 50)

# --- 스케줄러 설정 ---
sync_initial_baselines() # 시작 시 시트 로드
scheduler = BackgroundScheduler(timezone="Asia/Seoul")

# 1. 7분 주기 정각 수집 (00초)
scheduler.add_job(fetch_and_update, 'cron', minute='*/7', second='0')

# 2. 자정 리셋 (00:00:00)
scheduler.add_job(daily_reset_job, 'cron', hour=0, minute=0, second=0)

scheduler.start()

@app.route('/')
def health_check():
    return "Trend-Doc Backend is Running!"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
