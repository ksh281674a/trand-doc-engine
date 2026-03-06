import time
import random
import numpy as np
from datetime import datetime, timedelta
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
import firebase_admin
from firebase_admin import credentials, db

# --- [1] 초기 설정 및 Firebase 연결 ---
# Cloudtype 환경에서는 serviceAccountKey.json 파일이 프로젝트 루트에 있어야 합니다.
try:
    cred = credentials.Certificate('serviceAccountKey.json')
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://your-project-id.firebaseio.com/'
    })
except Exception as e:
    print(f"Firebase 연결 정보를 확인해주세요 (현재 로컬 모드): {e}")

KST = pytz.timezone('Asia/Seoul')
STOCKS = [f"종목_{i:02d}" for i in range(1, 35)] # 총 34개 종목
stock_states = {}

# 모든 종목의 초기 기본 점수를 60점으로 설정
for ticker in STOCKS:
    stock_states[ticker] = {
        "base_score": 60.0,    # 기준 점수 (전날 종가 또는 초기 60점)
        "current_pct": 0.0,    # 현재 변동률 (%) - 하루 시작은 항상 0%
        "target_pct": 0.0,     # 트렌드 점수 기반 목표 변동률 (%)
        "remaining_sec": 420,  # 7분(420초) 수렴 타이머
        "rev_left": 0,         # 남은 반전 횟수 (0~5회)
        "direction": 1,        # 현재 모멘텀 방향
        "volatility": 0.05     # 변동성 계수
    }

# --- [2] 4가지 핵심 엔진 로직 (수렴 및 변동) ---
def calculate_next_tick(ticker):
    s = stock_states[ticker]
    rem = s["remaining_sec"]
    
    if rem <= 0:
        # 7분 종료 시: 목표치 근처 +-0.15% 랜덤 수렴
        s["current_pct"] = s["target_pct"] + random.uniform(-0.15, 0.15)
    else:
        # 1. 수렴 동력 (Drift): 남은 시간 동안 목표치로 유도
        drift = (s["target_pct"] - s["current_pct"]) / (rem + 1)

        # 2. 가우시안 노이즈: 미세한 틱 떨림
        noise = np.random.normal(0, 0.015)

        # 3. 모멘텀 및 반전 시스템: 설정된 횟수만큼 방향 전환
        if s["rev_left"] > 0 and random.random() < (s["rev_left"] / rem):
            s["direction"] *= -1
            s["rev_left"] -= 1
        momentum = s["direction"] * (abs(s["target_pct"]) * 0.005 + 0.005)

        # 4. 변동성 클러스터링 및 평균 회귀
        delta = drift + noise + momentum
        s["current_pct"] += delta
        s["remaining_sec"] -= 1

    # 최종 점수 계산: 기준 점수 * (1 + 현재 변동률 / 100)
    final_score = s["base_score"] * (1 + s["current_pct"] / 100)
    return round(final_score, 2), round(s["current_pct"], 2)

# --- [3] 데이터 수집 및 로그 출력 ---
def update_stock_batch(batch_num):
    start_idx = batch_num * 5
    end_idx = min(start_idx + 5, len(STOCKS))
    current_batch = STOCKS[start_idx:end_idx]
    
    now_str = datetime.now(KST).strftime('%H:%M:%S')
    print(f"\n[{now_str}] 🚀 {batch_num+1}번 배수 종목 수집 시작 (5개)")
    print("-" * 65)

    for ticker in current_batch:
        # 트렌드 점수 1점 = 0.5% (예: 12점 = 6%)
        trend_score = random.randint(1, 20)
        new_target_pct = trend_score * 0.5
        
        # 현재 점수 대비 목표까지 이동해야 할 거리 계산
        current_score = stock_states[ticker]["base_score"] * (1 + stock_states[ticker]["current_pct"] / 100)
        target_score = stock_states[ticker]["base_score"] * (1 + new_target_pct / 100)
        score_diff = target_score - current_score
        
        # 상태 갱신
        stock_states[ticker].update({
            "target_pct": new_target_pct,
            "remaining_sec": 420,
            "rev_left": random.randint(0, 5),
            "direction": 1 if new_target_pct > stock_states[ticker]["current_pct"] else -1
        })

        # Cloudtype 로그 출력
        status = "▲ 상승" if score_diff > 0 else "▼ 하락"
        print(f"  [수집완료] {ticker:7} | 트렌드: {trend_score:2}점 | 목표: {target_score:>6.2f}점 ({status} {abs(score_diff):>5.2f}점 이동)")
    print("-" * 65)

# --- [4] 자정 리셋 로직 (KST 00:00) ---
def daily_reset():
    now = datetime.now(KST)
    print(f"\n[{now.strftime('%Y-%m-%d')}] 🕒 자정 리셋: 전날 마지막 데이터를 기준 점수로 덮어씀")
    for ticker in STOCKS:
        # 현재 점수를 계산하여 다음 날의 base_score로 설정
        final_score_today = stock_states[ticker]["base_score"] * (1 + stock_states[ticker]["current_pct"] / 100)
        stock_states[ticker]["base_score"] = final_score_today
        stock_states[ticker]["current_pct"] = 0.0 # 변동률은 0%로 초기화
        stock_states[ticker]["target_pct"] = 0.0

# --- [5] 스케줄러 설정 ---
scheduler = BackgroundScheduler(timezone=KST)

# 20:10:30부터 1분 간격으로 7번 배치 수집 (총 34개 종목 완료)
for i in range(7):
    scheduler.add_job(update_stock_batch, 'cron', hour=20, minute=10+i, second=30, args=[i])

# 매일 자정 리셋
scheduler.add_job(daily_reset, 'cron', hour=0, minute=0)
scheduler.start()

# --- [6] 실시간 메인 루프 ---
print("✅ 주가 시뮬레이션 서버 가동 중... (기본 점수: 60점)")
try:
    while True:
        updates = {}
        for ticker in STOCKS:
            current_score, current_pct = calculate_next_tick(ticker)
            # Firebase 업데이트 경로 설정
            updates[f"stocks/{ticker}/score"] = current_score
            updates[f"stocks/{ticker}/change_pct"] = current_pct
        
        # db.reference().update(updates) # 실사용 시 주석 해제
        time.sleep(1)
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
