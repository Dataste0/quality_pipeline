import os
import sys
from pathlib import Path
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pipeline_lib.config as cfg

TRANSFORMED_BASE_PATH = cfg.DATA_TRANSFORMED_DIR_PATH
OLAP_BASE_PATH = cfg.OLAP_EXPORT_DIR_PATH

import pyarrow.parquet as pq

# Directory di partenza (sostituisci con la tua)
base_dir = os.path.join(TRANSFORMED_BASE_PATH, "a01Hs00001q1a21IAA")

# Mappa dei nomi da rinominare
rename_map = {
    "is_correct": "job_correct"
    #"old_name2": "new_name2",
}

# Funzione che rinomina le colonne di un file parquet
def rename_parquet_columns(file_path):
    table = pq.read_table(file_path)
    new_names = [rename_map.get(name, name) for name in table.schema.names]
    table = table.rename_columns(new_names)
    pq.write_table(table, file_path)  # sovrascrive il file originale
    print(f"✔ Rinominate colonne in: {file_path}")

# Scansione ricorsiva di tutti i .parquet nella directory
for root, _, files in os.walk(base_dir):
    for file in files:
        if file.endswith(".parquet"):
            print(f"🔍 Trovato file: {file} in {root}")
            rename_parquet_columns(os.path.join(root, file))

print("✅ Rinomina completata per tutti i file Parquet.")
