import os
import time
import math
import threading
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# 설정 api 키 많으면 다 넣으시면 됩니다
# =========================
API_KEYS = [
]
SHARD = "steam"  # 업로드하신 파일에 맞춰 steam / kakao 설정
SEASON_ID = "division.bro.official.pc-2018-33"
RANKED_MODE = "squad"

INPUT_FILE = "0210_steam_재민님.csv"
OUTPUT_FILE = "0210_steam_재민님_결과.csv"

# 중간 저장(재시작 용)
CACHE_DIR = "./cache_ranked"
CACHE_EVERY = 25

# 네트워크 설정
TIMEOUT = (7, 25)
MAX_RETRY = 6
BASE_BACKOFF = 1.3
MIN_INTERVAL_PER_KEY = 6.1

# RateLimiter (키별)
class RateLimiter:
    def __init__(self, min_interval_sec: float = 0.0):
        self.min_interval_sec = min_interval_sec
        self._lock = threading.Lock()
        self._last_ts = 0.0

    def wait_before_call(self):
        if self.min_interval_sec <= 0:
            return
        with self._lock:
            now = time.time()
            gap = now - self._last_ts
            if gap < self.min_interval_sec:
                time.sleep(self.min_interval_sec - gap)
            self._last_ts = time.time()

    def on_response(self, resp: requests.Response):
        # 남은 요청이 거의 없으면 reset까지 대기
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset_ts = resp.headers.get("X-RateLimit-Reset")
        try:
            remaining_i = int(remaining) if remaining is not None else None
        except:
            remaining_i = None
        try:
            reset_i = int(reset_ts) if reset_ts is not None else None
        except:
            reset_i = None

        if remaining_i is not None and remaining_i <= 1 and reset_i is not None:
            wait = max(1, reset_i - int(time.time()))
            print(f"[RL] remaining={remaining_i}, wait until reset {wait}s")
            time.sleep(wait)

    def on_429(self, resp: requests.Response):
        reset_ts = resp.headers.get("X-RateLimit-Reset")
        wait = 10
        if reset_ts:
            try:
                wait = max(1, int(reset_ts) - int(time.time()))
            except:
                wait = 10
        print(f"[429] rate limit. wait {wait}s")
        time.sleep(wait)


@dataclass
class KeyClient:
    api_key: str
    session: requests.Session
    rl: RateLimiter

# PUBG 호출 (/ranked) - rankPoint만 가져오도록 수정
def fetch_ranked_one(client: KeyClient, player_id: str) -> Tuple[str, Optional[float]]:
    """
    return: (playerId, rankPoint)
    """
    url = f"https://api.pubg.com/shards/{SHARD}/players/{player_id}/seasons/{SEASON_ID}/ranked"
    headers = {
        "Authorization": f"Bearer {client.api_key}",
        "Accept": "application/vnd.api+json"
    }

    for attempt in range(1, MAX_RETRY + 1):
        client.rl.wait_before_call()
        try:
            resp = client.session.get(url, headers=headers, timeout=TIMEOUT)
        except requests.RequestException as e:
            # 네트워크 오류 -> backoff
            wait = (BASE_BACKOFF ** attempt)
            print(f"[NET] {player_id} attempt={attempt} err={type(e).__name__} wait={wait:.1f}s")
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            client.rl.on_response(resp)
            data = resp.json()
            ranked = data.get("data", {}).get("attributes", {}).get("rankedGameModeStats", {}) or {}

            mode_stats = ranked.get(RANKED_MODE) or ranked.get("squad") or {}
            rp = mode_stats.get("currentRankPoint")
            try:
                rp = float(rp) if rp is not None else None
            except:
                rp = None
            return (player_id, rp)

        if resp.status_code == 429:
            client.rl.on_429(resp)
            continue

        # 그 외: 404/400 등
        return (player_id, None)

    return (player_id, None)

# 캐시 I/O
def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def load_global_cache() -> pd.DataFrame:
    if not os.path.exists(CACHE_DIR):
        return pd.DataFrame(columns=["playerId", "rankPoint"])
    files = [os.path.join(CACHE_DIR, f) for f in os.listdir(CACHE_DIR) if f.endswith(".csv")]
    if not files:
        return pd.DataFrame(columns=["playerId", "rankPoint"])
    dfs = [pd.read_csv(f) for f in files]
    out = pd.concat(dfs, ignore_index=True)
    out = out.dropna(subset=["playerId"]).drop_duplicates(subset=["playerId"], keep="last")
    return out

# 메인 로직
def run():
    # 1) 파일 로드 및 중복 제거 (ValueError 방지)
    if not os.path.exists(INPUT_FILE):
        raise RuntimeError(f"INPUT_FILE을 찾을 수 없습니다: {INPUT_FILE}")

    ensure_dir(CACHE_DIR)
    df = pd.read_csv(INPUT_FILE)
    df = df.drop_duplicates(subset=['playerId'], keep='first')

    # 기존 캐시 로드해서 먼저 채우기
    cache_df = load_global_cache()
    if not cache_df.empty:
        cache_map = cache_df.set_index("playerId")["rankPoint"].to_dict()
        if "rankPoint" not in df.columns:
            df["rankPoint"] = None
        df["rankPoint"] = df.apply(lambda r: cache_map.get(r["playerId"], r.get("rankPoint")), axis=1)
        print(f"[CACHE] loaded={len(cache_df)} players from {CACHE_DIR}")

    # 2) /ranked 호출 대상 선정: rankPoint가 비어있는 모든 사람 (제한 해제)
    if "rankPoint" not in df.columns:
        df["rankPoint"] = None

    need_mask = df["rankPoint"].isna()
    ranked_targets = df.loc[need_mask, "playerId"].tolist()
    total = len(ranked_targets)

    print(f"[INFO] ranked_call_targets={total} (전체 인원)")

    # 3) 키 4개로 동적 큐 병렬 처리
    clients: List[KeyClient] = []
    for k in API_KEYS:
        sess = requests.Session()
        rl = RateLimiter(min_interval_sec=MIN_INTERVAL_PER_KEY)
        clients.append(KeyClient(api_key=k, session=sess, rl=rl))

    rr_lock = threading.Lock()
    rr_idx = {"i": 0}

    def pick_client() -> KeyClient:
        with rr_lock:
            c = clients[rr_idx["i"] % len(clients)]
            rr_idx["i"] += 1
            return c

    result_lock = threading.Lock()
    results: Dict[str, Optional[float]] = {}

    def task(player_id: str):
        client = pick_client()
        pid, rp = fetch_ranked_one(client, player_id)
        with result_lock:
            results[pid] = rp
        return pid

    # 실행
    max_workers = len(API_KEYS) * 2
    done = 0
    print(f"[RUN] ranked fetch with {len(API_KEYS)} keys")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(task, pid) for pid in ranked_targets]
        for f in as_completed(futures):
            _pid = f.result()
            done += 1
            if done % 10 == 0 or done == total:
                print(f"[PROGRESS] {done}/{total} done")

            # 주기적으로 캐시 저장
            if done % CACHE_EVERY == 0 or done == total:
                cache_out = os.path.join(CACHE_DIR, "ranked_cache_current_run.csv")
                snap = []
                with result_lock:
                    for pid, rp in results.items():
                        snap.append({"playerId": pid, "rankPoint": rp})
                pd.DataFrame(snap).to_csv(cache_out, index=False, encoding="utf-8-sig")

    # 4) df에 ranked 결과 반영 (기존 컬럼 유지)
    if results:
        df_idx = df.set_index("playerId")
        for pid, rp in results.items():
            if pid in df_idx.index:
                df_idx.loc[pid, "rankPoint"] = rp
        df = df_idx.reset_index()

    # 5) 저장
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"[DONE] saved -> {OUTPUT_FILE} rows={len(df)}")

if __name__ == "__main__":
    run()