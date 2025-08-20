import os
import sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pipeline_lib.config as cfg
import pipeline_lib.pipeline_utils as pu

RAWDATA_BASE_PATH = cfg.RAWDATA_ROOT_PATH
FILE_PATH = os.path.join(RAWDATA_BASE_PATH, "01TR00000KeKFrYAN_MVP-Generative composition", "WE 2025.07.18", "WE 2025.07.18 - Copy.csv")

print(f"\n[TEST] Tentativo 1")
try:
    df = pd.read_csv(FILE_PATH, dtype=str, nrows=10)
    print(f"Numero colonne: {len(df.columns)}")
    print("Nomi colonne:")
    for i, col in enumerate(df.columns, start=1):
        print(f"{i:2d}. {col}")
except Exception as e:
    print(f"Errore nell'apertura del file: {e}")

print(f"\n[TEST] Tentativo 2")
df2 = pu.load_df_from_filepath(FILE_PATH)
print(f"Numero colonne: {len(df2.columns)}")
print("Nomi colonne:")
for i, col in enumerate(df2.columns, start=1):
    print(f"{i:2d}. {col}")