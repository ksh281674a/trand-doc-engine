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
# 1. Firebase 인증 (보내주신 JSON 내용을 코드에 직접 삽입)
# ---------------------------------------------------------
# 파일(serviceAccountKey.json)을 읽지 않고, 아래 데이터를 직접 사용합니다.
service_account_info = {
  "type": "service_account",
  "project_id": "trand-doc",
  "private_key_id": "eb94df5a568c0b644922b418c9aab01b588d9f06",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDYZMCIOTCDp1b0\nyCThF144nIhaH7rjL+xicU/azv9IWJIaOb5uz9X0Qnu0ePBx0VR/AtcELcO+OZRe\nS+eCgtDQ2RCK8tdd8mUnLkoBvR4ENY2HnTMomYJ3FEIRkCyjWnhjABYDBXt1DNa6\nj0SG8hJ2NDQeX/disjeCEKP8uYr5IIjpZabU9tsi+Sy74Aoli3UVQufoy0Wpy/Nf\nv9NBxeIFpoaxUB3iZpO3alFr7FU9a1aXcPxu+0CHcatnD4Pp4ywPaupCxFTxBxOo\nLR9nuzk2GHD0lSG6CzBFt+1RvZzUrO0kTkNZWUgL8M66FsjWq+5VbGDKhuvfn2fl\nv1FSAEo5AgMBAAECggEAKd2oUFK8O6R7EuXMGM6EGLDUQpeD+WNyuQMSP5Ov2Ufl\nkHRejyLf0p+wPlTttB+bWO1sOy89gUazmWDXHC2CB/4PEMr19wOoJWHzyI1Vytuv\nk67K0I6Oqso9FBfQQxSEWScRmykK3zUKMFL6n58jCkKEWOIZQzuWuK/Ogl1FcXhK\nJdQG+8VwzQurtoFv7uEgautqsay3F4oVY/HTEpbliSaCAoJ11gkxAkDHGmQP/C9A\nxwLnBiS1TFEBJzVhrujkDq7HUlcFfT6IMfG6REJ1GJSLP147jhgpad55+/jo4PpE\n26JVCsk9kUDdgB9X5x8v1ssfb7S19t32QhNqI62yLwKBgQD84TR9Z9D5txfgf4iL\nvh6gDlb5fjYu1YGvyCEmOE6sCJeoF2CRPr6CPIhSxxcWIYf0vT+g9oPyOqBwqVfD\nDklZRHVBUIBtYLWApVSajid+oSBOYA/1TjZ8WpjFHKgKRop/EH2v6Y16OOdgC/BW\nN9HDAG5nFb78WVcCUjwSm6pMWwKBgQDbEEt5KThSnghkAYZqrpgLARcLf4/LQ0V4\ZhluY5CBq4eaWTJm3R3tSlmk2vBlAsRXPLJPgcycLsne7CTnLsrlYunUFxMt4/c2\n/s8v1wImuBt2CZ9+KgmCgKv5xW9xBZNyfareDouAZ9lf9kMd/AR6dRCWBWrLoAGl\n85VpZGXX+wKBgQDtfd+bCx4S9+Zfu8aUXyhJ7021oNfoIjJa0Rx41ZblMGilv9a8\nce3fSFRLUZkX+sPBkN6qH/qJSpAVXqUClm9Ce+2XOrByiMnNGPic8nabEV3S3Zr/\nKcY4AanKLQCQLfGyJd20kSaCq+B4rp23i1LfzY7iF2U3f/wcPPkYDMPiUwKBgQC6\nXPB4URLzZjJpMcrysznyEqlSROFF4SMWvHViLh3f0td3/e4dCHvPRXNiBkiBBouW\nU7K2ZQx2ym8+0NLMQkimQTIAFulgHxJPnRMR9e6Elhf2oVUodgbFGUY1JTwbgMzh\nl/tnEiSnxrFtLAoJVj9RFopXtqAWPzdnwQQZNypVRQKBgG0IIxTzZhtDZE8BShFW\n83trbUHkw0KBsztoU7C4LcsaP7It3CpGvaNC6sU0LKHadGQ3fw2A/J4kInZ3aHv0\n6VfSzrbKmz3Vprvzlh80wfZiifuRVTFKcWP8TLN1RQYOi9yYsRtvd9aeo/t6MiqE\nz+g2BVwl576hZrh3pJSM2c0a\n-----END PRIVATE KEY-----\n",
  "client_email": "firebase-adminsdk-fbsvc@trand-doc.iam.gserviceaccount.com",
  "client_id": "104483003439945965503",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40trand-doc.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# 인증 수행 (파일을 참조하지 않고 딕셔너리 데이터를 직접 전달)
cred = credentials.Certificate(service_account_info)
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://trand-doc-default-rtdb.firebaseio.com/'
})

pytrends = TrendReq(hl='ko-KR', tz=540)

# ---------------------------------------------------------
# 2. [데이터] 사용자님이 주신 34개 종목 및 평균 점수
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
# 3. 실시간 틱 엔진 (알고리즘 유지)
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
            clustering = 1.4 if abs(target - current) > 0.5 else 1.0
            next_tick = current + (noise * clustering) + pull
            updates[f'{ticker}/current_yield'] = round(next_tick, 4)
        except: continue
    
    if updates:
        db.reference('trends').update(updates)

# ---------------------------------------------------------
# 4. 자정 리셋 & 7분 주기 수집
# ---------------------------------------------------------
def daily_midnight_reset():
    print(f"\n🌕 [Midnight Reset] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    all_trends = db.reference('trends').get()
    if not all_trends: return

    for ticker in TICKERS_DATA.keys():
        data = all_trends.get(ticker, {})
        last_score = data.get('last_score', TICKERS_DATA[ticker])
        db.reference(f'trends/{ticker}').update({
            'baseline': last_score,
            'target_yield': 0.0,
            'current_yield': 0.0
        })

def fetch_and_update():
    now = datetime.now()
    print(f"\n📊 [Trend Update] {now.strftime('%H:%M:%S')} 수집 시작")
    for ticker in TICKERS_DATA.keys():
        try:
            ref = db.reference(f'trends/{ticker}')
            data = ref.get()
            baseline = data.get('baseline', TICKERS_DATA[ticker])
            
            pytrends.build_payload([ticker], timeframe='now 1-H')
            df = pytrends.interest_over_time()
            current_score = float(df[ticker].iloc[-1]) if not df.empty else baseline
            
            target_yield = (current_score - baseline) * 0.5
            ref.update({
                'last_score': current_score,
                'target_yield': target_yield,
                'last_update': now.isoformat()
            })
            print(f" ✅ [{ticker}] {target_yield:+.2f}%")
            time.sleep(12) 
        except Exception as e:
            print(f" ❌ [{ticker}] 오류: {e}")
            time.sleep(20)

# ---------------------------------------------------------
# 5. 가동
# ---------------------------------------------------------
def initialize_app():
    print("🚀 [System] Firebase 데이터 초기화 및 점검...")
    for ticker, avg in TICKERS_DATA.items():
        ref = db.reference(f'trends/{ticker}')
        if not ref.get():
            ref.set({'baseline': avg, 'last_score': avg, 'target_yield': 0.0, 'current_yield': 0.0})
    print("✅ 모든 준비가 완료되었습니다.")

scheduler = BackgroundScheduler(timezone="Asia/Seoul")
scheduler.add_job(fetch_and_update, 'cron', minute='*/7', second='0')
scheduler.add_job(generate_ticks, 'interval', seconds=2)
scheduler.add_job(daily_midnight_reset, 'cron', hour=0, minute=0)

if __name__ == "__main__":
    initialize_app()
    scheduler.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
