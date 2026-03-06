"""
trend_engine.py
═══════════════════════════════════════════════════════════════════
[ 점수 계층 ]

  base_score  (기본 트렌드 점수)
  ├─ 최초 : 0.0 %
  ├─ 매일 KST 20:11:00 → 오늘 하루 cycle_target 평균값으로 교체
  └─ 다음날 기준점 역할

  cycle_target  (사이클 수렴 목표)
  ├─ 구글 트렌드 원시점수(0~200) 수집 후 계산  (1점 = 0.5%)
  ├─ 진폭 기준: 60점 (= 30%)
  └─ base_score 를 중심으로 ▲ or ▼ 결정

  display_score  (화면 표시값)
  ├─ 매일 KST 00:00:00 → 0% 로 리셋  (base_score 는 유지)
  └─ 매 1초 tick → cycle_target 으로 수렴
       ① 가우시안 노이즈    (σ ∝ 트렌드 강도)
       ② 모멘텀 시스템      (0~5회 반전/사이클)
       ③ 변동성 클러스터링  (충격 후 여운)
       ④ 평균 회귀력        (진행률↑ 수록 강하게 수렴)

[ 수집 ]  34개 종목 / 1분당 5개 / ≈ 7분 1사이클

[ 모든 시간 기준 ]  KST (Asia/Seoul)
═══════════════════════════════════════════════════════════════════
"""

import random
import time
import threading
from datetime import datetime
from collections import defaultdict
import pytz

KST = pytz.timezone("Asia/Seoul")

# ───────────────────────────────────────────────────────────────
# 34개 종목 정의
# ───────────────────────────────────────────────────────────────
TICKERS: dict[str, str] = {
    # SNS (4)
    "카카오":       "sns",
    "인스타그램":   "sns",
    "틱톡":         "sns",
    "X(트위터)":    "sns",
    # 방송 (3)
    "유튜브":       "broadcast",
    "치지직":       "broadcast",
    "SOOP":         "broadcast",
    # 이슈 (4)
    "네이버":       "issue",
    "구글":         "issue",
    "다음":         "issue",
    "MS(Bing)":     "issue",
    # 쇼핑 (4)
    "쿠팡":         "shopping",
    "알리":         "shopping",
    "무신사":       "shopping",
    "테무":         "shopping",
    # 게임 (3)
    "스팀":         "game",
    "넥슨":         "game",
    "라이엇":       "game",
    # OTT (4)
    "넷플릭스":     "ott",
    "쿠팡플레이":   "ott",
    "왓챠":         "ott",
    "티빙":         "ott",
    # 음악 (3)
    "유튜브뮤직":   "music",
    "멜론":         "music",
    "애플뮤직":     "music",
    # 배달 (3)
    "배달의민족":   "delivery",
    "쿠팡이츠":     "delivery",
    "요기요":       "delivery",
    # 엔터 (4)
    "하이브":       "enter",
    "SM":           "enter",
    "YG":           "enter",
    "JYP":          "enter",
    # 웹툰 (2)
    "네이버웹툰":   "webtoon",
    "카카오페이지": "webtoon",
}
TICKER_LIST: list[str] = list(TICKERS.keys())  # 34개

# ───────────────────────────────────────────────────────────────
# 상수
# ───────────────────────────────────────────────────────────────
POINTS_PER_PCT  = 2.0    # 2점 = 1%  (즉 1점 = 0.5%)
CYCLE_AMPLITUDE = 60.0   # 사이클 등락 기준 진폭 (구글 원시점수)
                         # 60점 = 30%  →  base_score 기준 ±30% 범위에서 목표 설정
CYCLE_DURATION  = 420.0  # 7분 = 420초


# ═══════════════════════════════════════════════════════════════
# TrendState  ─  종목 하나의 상태 전체
# ═══════════════════════════════════════════════════════════════
class TrendState:

    def __init__(self, name: str):
        self.name = name

        # ── 점수 계층 ──────────────────────────────────────────
        self.base_score:    float = 0.0   # 기본 트렌드 점수 (전날 평균, 첫날=0%)
        self.cycle_target:  float = 0.0   # 이번 사이클 수렴 목표 %
        self.prev_target:   float = 0.0   # 직전 사이클 목표 (방향 표시용)
        self.display_score: float = 0.0   # 화면에 보이는 현재 % (매일 00:00 리셋)

        # ── 모멘텀 ─────────────────────────────────────────────
        self.momentum:        float = 0.0
        self.momentum_steps:  int   = 0
        self.reversal_count:  int   = 0
        self.reversal_budget: int   = 0

        # ── 변동성 클러스터링 ──────────────────────────────────
        self.volatility_mult: float = 1.0
        self.shock_decay:     float = 0.0

        # ── 사이클 타이머 ──────────────────────────────────────
        self.cycle_start: float = time.time()

    # ── 새 사이클 시작 ─────────────────────────────────────────
    def start_cycle(self, google_raw: float) -> None:
        """
        google_raw : 구글 트렌드 원시점수 (0 ~ 200)
        """
        self.prev_target  = self.cycle_target
        self.cycle_start  = time.time()

        # 원시점수 → % 변환
        google_pct = google_raw / POINTS_PER_PCT          # 0 ~ 100%

        # 기준 진폭(60점=30%)과의 차이 → base_score 에 더함
        # 60점보다 높으면 ▲, 낮으면 ▼
        delta_pct = google_pct - (CYCLE_AMPLITUDE / POINTS_PER_PCT)  # ± %

        # 새 목표 (음수 방지)
        self.cycle_target = max(0.0, self.base_score + delta_pct)

        # 모멘텀 반전 예산: delta 클수록 반전 더 많이 허용 (0~5회)
        intensity            = min(abs(delta_pct) / 30.0, 1.0)
        self.reversal_budget = int(intensity * 5)
        self.reversal_count  = 0
        self.momentum        = 0.0
        self.momentum_steps  = 0

    # ── 가우시안 σ ────────────────────────────────────────────
    def _sigma(self) -> float:
        base = 0.02 + (abs(self.cycle_target) / 100.0) * 0.10
        return base * self.volatility_mult

    # ── 1틱 ───────────────────────────────────────────────────
    def tick(self) -> float:
        elapsed   = time.time() - self.cycle_start
        remaining = max(0.0, CYCLE_DURATION - elapsed)
        progress  = min(elapsed / CYCLE_DURATION, 1.0)

        target  = self.cycle_target
        current = self.display_score
        diff    = target - current

        # ④ 평균 회귀력
        rev_str = 0.005 + progress * 0.05
        if remaining < 30:
            rev_str = 0.15 + (1.0 - remaining / 30.0) * 0.35
        reversion = diff * rev_str

        # ① 가우시안 노이즈
        sigma = self._sigma()
        if remaining < 60:
            sigma *= (remaining / 60.0) * 0.5
        noise = random.gauss(0, sigma)

        # ② 모멘텀 시스템
        momentum_force = 0.0
        if self.momentum_steps > 0:
            momentum_force       = self.momentum * 0.3
            self.momentum_steps -= 1
            if self.momentum_steps == 0:
                self.momentum *= -1   # 반전
                if abs(self.momentum) > 1e-4:
                    self.reversal_count += 1
        else:
            budget_left  = self.reversal_budget - self.reversal_count
            trig_prob    = (abs(self.cycle_target) / 100.0) * 0.02
            if budget_left > 0 and remaining > 60 and random.random() < trig_prob:
                direction           = 1 if random.random() > 0.5 else -1
                strength            = sigma * (2 + random.random() * 3)
                self.momentum       = direction * strength
                self.momentum_steps = random.randint(5, 20)
                momentum_force      = self.momentum * 0.3

        # ③ 변동성 클러스터링
        raw_move = noise + momentum_force
        if abs(raw_move) > sigma * 3:
            self.shock_decay = min(abs(self.cycle_target) / 100.0 * 3.0, 3.0)

        if self.shock_decay > 0:
            self.volatility_mult  = 1.0 + self.shock_decay * 0.5
            self.shock_decay     -= 0.1
            if self.shock_decay <= 0:
                self.shock_decay     = 0.0
                self.volatility_mult = 1.0
        else:
            self.volatility_mult = max(1.0, self.volatility_mult * 0.98)

        # 최종 delta
        delta = reversion + raw_move

        # 목표 근접 시 ±0.15% 랜덤 떨림
        if abs(diff) < 0.15:
            delta = random.uniform(-0.15, 0.15) * 0.3

        new_val = current + delta

        # 이탈 방지
        max_dev = max(0.5, abs(self.cycle_target) * 0.3)
        new_val = max(target - max_dev, min(target + max_dev, new_val))

        self.display_score = new_val
        return self.display_score


# ═══════════════════════════════════════════════════════════════
# TrendEngine  ─  전체 엔진
# ═══════════════════════════════════════════════════════════════
class TrendEngine:

    COLLECT_INTERVAL = 60.0   # 1분
    BATCH_SIZE       = 5      # 1회 수집 종목 수

    def __init__(self):
        self.states: dict[str, TrendState] = {
            name: TrendState(name) for name in TICKER_LIST
        }

        self._collect_index: int   = 0
        self._last_collect:  float = 0.0

        # 일별 사이클 목표 누적
        self._daily_acc:  dict[str, list[float]] = defaultdict(list)
        self._pending_avg: dict[str, float]      = {}

        # 중복 발화 방지
        self._fired: set[str] = set()

        self._lock    = threading.Lock()
        self._running = False

    # ── 시작 / 종료 ────────────────────────────────────────────
    def start(self):
        self._running = True
        threading.Thread(target=self._loop, daemon=True).start()
        print("[TrendEngine] 시작 ▶  기준 시간: KST")

    def stop(self):
        self._running = False

    # ── 메인 루프 ──────────────────────────────────────────────
    def _loop(self):
        while self._running:
            now     = time.time()
            kst_now = datetime.now(KST)

            self._daily_routine(kst_now)

            if now - self._last_collect >= self.COLLECT_INTERVAL:
                self._collect_batch()
                self._last_collect = now

            with self._lock:
                for s in self.states.values():
                    s.tick()

            time.sleep(1.0)

    # ── 일별 루틴 (KST) ────────────────────────────────────────
    def _daily_routine(self, kst_now: datetime):
        date = kst_now.strftime("%Y-%m-%d")
        hms  = kst_now.strftime("%H:%M:%S")

        # KST 20:10:30 → 오늘 평균 계산 (pending)
        k_avg = date + "_avg"
        if hms == "20:10:30" and k_avg not in self._fired:
            self._fired.add(k_avg)
            self._calc_daily_avg()
            print(f"[KST {date} 20:10:30] 오늘 평균 계산 완료")

        # KST 20:11:00 → base_score 교체
        k_write = date + "_write"
        if hms == "20:11:00" and k_write not in self._fired:
            self._fired.add(k_write)
            self._apply_base_score()
            print(f"[KST {date} 20:11:00] base_score 교체 완료")

        # KST 00:00:00 → display_score 0% 리셋  (base_score 유지)
        k_reset = date + "_reset"
        if hms == "00:00:00" and k_reset not in self._fired:
            self._fired.add(k_reset)
            self._midnight_reset()
            self._daily_acc.clear()
            print(f"[KST {date} 00:00:00] display 0% 리셋 완료")

    # ── 수집 배치 ──────────────────────────────────────────────
    def _collect_batch(self):
        batch = [
            TICKER_LIST[(self._collect_index + i) % len(TICKER_LIST)]
            for i in range(self.BATCH_SIZE)
        ]
        self._collect_index += self.BATCH_SIZE
        print(f"[Collect] {batch}")

        for name in batch:
            raw = self._fetch_google_trend(name)   # 0 ~ 200
            with self._lock:
                s = self.states[name]
                s.start_cycle(raw)
                self._daily_acc[name].append(s.cycle_target)

    # ── 구글 트렌드 API 연동 지점 ──────────────────────────────
    def _fetch_google_trend(self, name: str) -> float:
        """
        ※ 실제 연동 시 아래 pytrends 코드를 사용하세요.

        from pytrends.request import TrendReq
        pt = TrendReq(hl='ko', tz=540)
        pt.build_payload([name], timeframe='now 1-d', geo='KR')
        df = pt.interest_over_time()
        return float(df[name].iloc[-1]) if not df.empty else 0.0
        """
        # 시뮬레이션: CYCLE_AMPLITUDE(60점) 중심 ± 랜덤
        return max(0.0, min(200.0, random.gauss(CYCLE_AMPLITUDE, 15)))

    # ── 일별 내부 처리 ─────────────────────────────────────────
    def _calc_daily_avg(self):
        """오늘 모든 cycle_target 의 평균 → pending"""
        self._pending_avg = {
            name: (sum(vals) / len(vals)) if vals else 0.0
            for name, vals in self._daily_acc.items()
        }
        # 수집 없던 종목은 현재 base_score 유지
        for name in TICKER_LIST:
            if name not in self._pending_avg:
                self._pending_avg[name] = self.states[name].base_score

    def _apply_base_score(self):
        """pending 평균을 내일의 base_score 로 교체"""
        if not self._pending_avg:
            return
        with self._lock:
            for name, avg in self._pending_avg.items():
                self.states[name].base_score = avg

    def _midnight_reset(self):
        """
        자정: display_score → 0%  리셋
        base_score 는 절대 건드리지 않음 (내일 기준점으로 유지)
        """
        with self._lock:
            for s in self.states.values():
                s.display_score  = 0.0
                s.cycle_target   = 0.0
                s.prev_target    = 0.0
                s.momentum       = 0.0
                s.shock_decay    = 0.0
                s.volatility_mult = 1.0

    # ── 외부 스냅샷 (API 응답용) ───────────────────────────────
    def get_snapshot(self) -> dict:
        with self._lock:
            out = {}
            for name, s in self.states.items():
                change = s.cycle_target - s.prev_target
                out[name] = {
                    "display_pct":  round(s.display_score, 4),   # 화면 표시 %
                    "cycle_target": round(s.cycle_target,  4),   # 이번 사이클 목표 %
                    "base_score":   round(s.base_score,    4),   # 기본 트렌드 점수 %
                    "change":       round(change,          4),   # 전 사이클 대비 변화
                    "direction":    "▲" if change >= 0 else "▼",
                    "category":     TICKERS[name],
                    "volatility":   round(s.volatility_mult, 3),
                }
            return out


# ═══════════════════════════════════════════════════════════════
# FastAPI 연동 예시  (cloudtype 백엔드 / main.py)
# ═══════════════════════════════════════════════════════════════
"""
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from trend_engine import TrendEngine

app    = FastAPI()
engine = TrendEngine()

@app.on_event("startup")
def on_startup():
    engine.start()

@app.get("/trends")
def all_trends():
    return JSONResponse(engine.get_snapshot())

@app.get("/trends/{ticker}")
def one_trend(ticker: str):
    snap = engine.get_snapshot()
    if ticker not in snap:
        return JSONResponse({"error": "종목 없음"}, status_code=404)
    return JSONResponse(snap[ticker])
"""


# ═══════════════════════════════════════════════════════════════
# 로컬 테스트
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    engine = TrendEngine()
    engine.start()

    WATCH = ["카카오", "유튜브", "네이버", "왓챠", "JYP", "배달의민족"]
    print("=== 트렌드 엔진 실행 중  (Ctrl+C 종료) ===\n")

    try:
        for i in range(60):
            time.sleep(3)
            snap = engine.get_snapshot()
            kst  = datetime.now(KST).strftime("%H:%M:%S")
            print(f"── KST {kst}  ({i*3}s) ─────────────────────")
            for name in WATCH:
                d = snap[name]
                print(
                    f"  {name:10s}"
                    f"  표시={d['display_pct']:+8.4f}%"
                    f"  목표={d['cycle_target']:+8.4f}%"
                    f"  기준={d['base_score']:+8.4f}%"
                    f"  {d['direction']}  변동성x{d['volatility']:.2f}"
                )
    except KeyboardInterrupt:
        engine.stop()
        print("\n종료")
