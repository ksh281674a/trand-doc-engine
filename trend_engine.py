import random
import time
import threading
from datetime import datetime
from collections import defaultdict
import pytz

KST = pytz.timezone("Asia/Seoul")

TICKERS: dict[str, str] = {
    "카카오": "sns", "인스타그램": "sns", "틱톡": "sns", "X(트위터)": "sns",
    "유튜브": "broadcast", "치지직": "broadcast", "SOOP": "broadcast",
    "네이버": "issue", "구글": "issue", "다음": "issue", "MS(Bing)": "issue",
    "쿠팡": "shopping", "알리": "shopping", "무신사": "shopping", "테무": "shopping",
    "스팀": "game", "넥슨": "game", "라이엇": "game",
    "넷플릭스": "ott", "쿠팡플레이": "ott", "왓챠": "ott", "티빙": "ott",
    "유튜브뮤직": "music", "멜론": "music", "애플뮤직": "music",
    "배달의민족": "delivery", "쿠팡이츠": "delivery", "요기요": "delivery",
    "하이브": "enter", "SM": "enter", "YG": "enter", "JYP": "enter",
    "네이버웹툰": "webtoon", "카카오페이지": "webtoon",
}
TICKER_LIST: list[str] = list(TICKERS.keys())

POINTS_PER_PCT  = 2.0
CYCLE_AMPLITUDE = 60.0
CYCLE_DURATION  = 420.0


class TrendState:
    def __init__(self, name: str):
        self.name = name
        self.base_score:    float = 0.0
        self.cycle_target:  float = 0.0
        self.prev_target:   float = 0.0
        self.display_score: float = 0.0
        self.momentum:        float = 0.0
        self.momentum_steps:  int   = 0
        self.reversal_count:  int   = 0
        self.reversal_budget: int   = 0
        self.volatility_mult: float = 1.0
        self.shock_decay:     float = 0.0
        self.cycle_start: float = time.time()

    def start_cycle(self, google_raw: float) -> None:
        self.prev_target  = self.cycle_target
        self.cycle_start  = time.time()
        google_pct        = google_raw / POINTS_PER_PCT
        delta_pct         = google_pct - (CYCLE_AMPLITUDE / POINTS_PER_PCT)
        self.cycle_target = max(0.0, self.base_score + delta_pct)
        intensity            = min(abs(delta_pct) / 30.0, 1.0)
        self.reversal_budget = int(intensity * 5)
        self.reversal_count  = 0
        self.momentum        = 0.0
        self.momentum_steps  = 0

    def _sigma(self) -> float:
        return (0.02 + (abs(self.cycle_target) / 100.0) * 0.10) * self.volatility_mult

    def tick(self) -> float:
        elapsed   = time.time() - self.cycle_start
        remaining = max(0.0, CYCLE_DURATION - elapsed)
        progress  = min(elapsed / CYCLE_DURATION, 1.0)
        target    = self.cycle_target
        current   = self.display_score
        diff      = target - current

        rev_str = 0.005 + progress * 0.05
        if remaining < 30:
            rev_str = 0.15 + (1.0 - remaining / 30.0) * 0.35
        reversion = diff * rev_str

        sigma = self._sigma()
        if remaining < 60:
            sigma *= (remaining / 60.0) * 0.5
        noise = random.gauss(0, sigma)

        momentum_force = 0.0
        if self.momentum_steps > 0:
            momentum_force       = self.momentum * 0.3
            self.momentum_steps -= 1
            if self.momentum_steps == 0:
                self.momentum *= -1
                if abs(self.momentum) > 1e-4:
                    self.reversal_count += 1
        else:
            budget_left = self.reversal_budget - self.reversal_count
            trig_prob   = (abs(self.cycle_target) / 100.0) * 0.02
            if budget_left > 0 and remaining > 60 and random.random() < trig_prob:
                direction           = 1 if random.random() > 0.5 else -1
                self.momentum       = direction * sigma * (2 + random.random() * 3)
                self.momentum_steps = random.randint(5, 20)
                momentum_force      = self.momentum * 0.3

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

        delta = reversion + raw_move
        if abs(diff) < 0.15:
            delta = random.uniform(-0.15, 0.15) * 0.3

        new_val = current + delta
        max_dev = max(0.5, abs(self.cycle_target) * 0.3)
        new_val = max(target - max_dev, min(target + max_dev, new_val))
        self.display_score = new_val
        return self.display_score


class TrendEngine:
    COLLECT_INTERVAL = 60.0
    BATCH_SIZE       = 5

    def __init__(self):
        self.states: dict[str, TrendState] = {
            name: TrendState(name) for name in TICKER_LIST
        }
        self._collect_index: int   = 0
        self._last_collect:  float = 0.0
        self._daily_acc:  dict[str, list[float]] = defaultdict(list)
        self._pending_avg: dict[str, float]      = {}
        self._fired: set[str] = set()
        self._lock    = threading.Lock()
        self._running = False

    def start(self):
        self._running = True
        threading.Thread(target=self._loop, daemon=True).start()
        print("[TrendEngine] 시작 ▶  기준 시간: KST")

    def stop(self):
        self._running = False

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

    def _daily_routine(self, kst_now: datetime):
        date = kst_now.strftime("%Y-%m-%d")
        hms  = kst_now.strftime("%H:%M:%S")

        k_avg = date + "_avg"
        if hms == "20:10:30" and k_avg not in self._fired:
            self._fired.add(k_avg)
            self._calc_daily_avg()

        k_write = date + "_write"
        if hms == "20:11:00" and k_write not in self._fired:
            self._fired.add(k_write)
            self._apply_base_score()

        k_reset = date + "_reset"
        if hms == "00:00:00" and k_reset not in self._fired:
            self._fired.add(k_reset)
            self._midnight_reset()
            self._daily_acc.clear()

    def _collect_batch(self):
        batch = [
            TICKER_LIST[(self._collect_index + i) % len(TICKER_LIST)]
            for i in range(self.BATCH_SIZE)
        ]
        self._collect_index += self.BATCH_SIZE
        for name in batch:
            raw = self._fetch_google_trend(name)
            with self._lock:
                s = self.states[name]
                s.start_cycle(raw)
                self._daily_acc[name].append(s.cycle_target)

    def _fetch_google_trend(self, name: str) -> float:
        # 실제 연동 시 pytrends 사용
        # from pytrends.request import TrendReq
        # pt = TrendReq(hl='ko', tz=540)
        # pt.build_payload([name], timeframe='now 1-d', geo='KR')
        # df = pt.interest_over_time()
        # return float(df[name].iloc[-1]) if not df.empty else 0.0
        return max(0.0, min(200.0, random.gauss(CYCLE_AMPLITUDE, 15)))

    def _calc_daily_avg(self):
        self._pending_avg = {
            name: (sum(vals) / len(vals)) if vals else 0.0
            for name, vals in self._daily_acc.items()
        }
        for name in TICKER_LIST:
            if name not in self._pending_avg:
                self._pending_avg[name] = self.states[name].base_score

    def _apply_base_score(self):
        if not self._pending_avg:
            return
        with self._lock:
            for name, avg in self._pending_avg.items():
                self.states[name].base_score = avg

    def _midnight_reset(self):
        with self._lock:
            for s in self.states.values():
                s.display_score   = 0.0
                s.cycle_target    = 0.0
                s.prev_target     = 0.0
                s.momentum        = 0.0
                s.shock_decay     = 0.0
                s.volatility_mult = 1.0

    def get_snapshot(self) -> dict:
        with self._lock:
            out = {}
            for name, s in self.states.items():
                change = s.cycle_target - s.prev_target
                out[name] = {
                    "display_pct":  round(s.display_score, 4),
                    "cycle_target": round(s.cycle_target,  4),
                    "base_score":   round(s.base_score,    4),
                    "change":       round(change,          4),
                    "direction":    "▲" if change >= 0 else "▼",
                    "category":     TICKERS[name],
                    "volatility":   round(s.volatility_mult, 3),
                }
            return out
