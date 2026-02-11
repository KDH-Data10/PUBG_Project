import requests
import pandas as pd
import time
import os
from multiprocessing import Pool

# 1. 설정
API_KEYS = [
]
SHARD = "steam"
SEASON_ID = "division.bro.official.pc-2018-40"
INPUT_FILE = "0210_steam_.csv"
OUTPUT_FILE = "0210_steam_결과.csv"

def fetch_data_worker(chunk_info):
    df_subset, api_key, worker_id = chunk_info
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/vnd.api+json"
    }
    temp_file = f"temp_worker_{worker_id}.csv"
    
    if os.path.exists(temp_file):
        df_subset = pd.read_csv(temp_file)
        print(f"worker {worker_id}: 기존 파일 로드 완료")
    else:
        df_subset['currentRankPoint'] = None

    print(f"worker {worker_id} 시작 (대상: {len(df_subset)}명)")

    for idx, row in df_subset.iterrows():
        if pd.notna(row['currentRankPoint']):
            continue
            
        p_id = row['playerId']
        p_name = row['name']
        url = f"https://api.pubg.com/shards/{SHARD}/players/{p_id}/seasons/{SEASON_ID}/ranked"
        
        try:
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                data = res.json()
                stats = data['data']['attributes']['rankedGameModeStats']
                rp = stats.get('squad', {}).get('currentRankPoint', 0)
                df_subset.at[idx, 'currentRankPoint'] = rp
                print(f"[worker {worker_id}] {idx+1}/{len(df_subset)} | {p_name}: {rp} RP")
            elif res.status_code == 429:
                print(f"worker {worker_id}: RPM 초과 10초 대기")
                time.sleep(10)
                continue
            else:
                df_subset.at[idx, 'currentRankPoint'] = 0
                print(f"[worker {worker_id}] {p_name}: 데이터 없음(0)")
        except:
            df_subset.at[idx, 'currentRankPoint'] = 0
            
        if (idx + 1) % 10 == 0:
            df_subset.to_csv(temp_file, index=False, encoding='utf-8-sig')
        
        time.sleep(6.1)

    df_subset.to_csv(temp_file, index=False, encoding='utf-8-sig')
    return df_subset

if __name__ == "__main__":
    full_df = pd.read_csv(INPUT_FILE)
    num_keys = len(API_KEYS)
    
    chunks = []
    chunk_size = len(full_df) // num_keys
    for i in range(num_keys):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i != num_keys - 1 else len(full_df)
        chunks.append((full_df.iloc[start:end].copy(), API_KEYS[i], i))

    print(f"총 {num_keys}개의 키로 병렬 수집 시작")

    with Pool(num_keys) as p:
        final_dfs = p.map(fetch_data_worker, chunks)
    
    final_result = pd.concat(final_dfs)
    final_result.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"최종 수집 완료: {OUTPUT_FILE}")