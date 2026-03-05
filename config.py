# config.py — 프로젝트 공통 설정
# 모든 노트북 Cell 0에서 %run config.py 로 실행
import os

# ──경로 설정 
BASE_DIR   = r'C:\배그분석'
OUTPUT_DIR = os.path.join(BASE_DIR, 'analysis_output')
MODEL_DIR  = os.path.join(OUTPUT_DIR, 'models')

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(MODEL_DIR,  exist_ok=True)

# ─데이터 수집 범위
DATE_START = '20260212'
DATE_END   = '20260227'

# ─ 분석 대상 맵
MAP_NAME   = 'Erangel'
OTHER_MAPS = ['Miramar', 'Taego', 'Rondo']
ALL_MAPS   = [MAP_NAME] + OTHER_MAPS

# ─ True Table 파일명 고정
# 이 파일들이 파이프라인의 단일 진실 테이블(Single Source of Truth)
# 스키마 변경 시 반드시 이 목록도 함께 갱신할 것
TRUTH_TABLES = {
    'erangel_features':  os.path.join(OUTPUT_DIR, 'erangel_features.parquet'),
    'erangel_clustered': os.path.join(OUTPUT_DIR, 'erangel_clustered.parquet'),
    'miramar_clustered': os.path.join(OUTPUT_DIR, 'miramar_clustered.parquet'),
    'taego_clustered':   os.path.join(OUTPUT_DIR, 'taego_clustered.parquet'),
    'rondo_clustered':   os.path.join(OUTPUT_DIR, 'rondo_clustered.parquet'),
}

# ─ erangel_features.parquet 고정 스키마
# 01_data_pipeline이 생성하는 컬럼 목록 (타입 포함)
# 03/04가 이 컬럼에 의존 — 변경 시 하위 노트북도 함께 수정 필요
FEATURE_SCHEMA = {
    'matchId':                    'str',
    'accountId':                  'str',
    'drop_distance_from_path':    'float64',
    'early_enemy_density':        'float64',
    'rotation_timing_score':      'float64',
    'vehicle_use_ratio':          'float64',
    'bluezone_exposure_ratio':    'float64',
    'safezone_proximity_mean':    'float64',
    'safezone_edge_ratio':        'float64',
    'altitude_variance':          'float64',
    'survival_time':              'float64',
    'total_movement':             'float64',
    'max_vehicle_distance':       'float64',
    'kills':                      'float64',
    'damageDealt':                'float64',
    'winPlace':                   'float64',
    'win_flag':                   'int64',
    'top3_flag':                  'int64',
    'timeSurvived':               'float64',
    'walkDistance':               'float64',
    'rideDistance':               'float64',
    'heals':                      'float64',
    'boosts':                     'float64',
    'kill_rate':                  'float64',
    'move_speed':                 'float64',
    'heal_boost_use':             'float64',
}
REQUIRED_COLS = list(FEATURE_SCHEMA.keys())

# ─ 세그먼트 설정
# 이 구조는 "발견된 군집(클러스터)"이 아니라
# "정의된 페르소나 점수 기반 세그먼트"입니다.
#   Robust Z-Score 4개를 계산 → argmax로 배정 (규칙 기반)
PERSONA_LABELS = {
    'C0_center':  '🏃 중앙 점령형',
    'C1_edge':    '🌿 외곽 운영형',
    'C2_aggro':   '⚔️ 하이리스크 어태커',
    'C3_guerilla':'🦅 게릴라 운영형',
    'uncertain':  '❓ 불확실',
}
PERSONA_COLORS = {
    '🏃 중앙 점령형':        '#E74C3C',
    '🌿 외곽 운영형':        '#2ECC71',
    '⚔️ 하이리스크 어태커':  '#E67E22',
    '🦅 게릴라 운영형':      '#9B59B6',
    '❓ 불확실':             '#95A5A6',
}
MARGIN_THRESHOLD = 0.2

# ─ DuckDB 설정 
DUCKDB_THREADS = 6
DUCKDB_MEMORY  = '16GB' # 컴퓨터 램에 맞게 조절하세요

print(f"   config.py 로드 완료")
print(f"   BASE_DIR  : {BASE_DIR}")
print(f"   DATE      : {DATE_START} ~ {DATE_END}")
print(f"   MAPS      : {ALL_MAPS}")
