import os
import sys
from pathlib import Path
import shutil
import tempfile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pipeline_lib.config as cfg
OLAP_BASE_PATH = cfg.OLAP_EXPORT_DIR_PATH

import os
import csv

# Cartella di partenza
base_dir = OLAP_BASE_PATH

# Itera tutti i file nella cartella e sottocartelle
for root, _, files in os.walk(base_dir):
    for file in files:
        if file.endswith("_smr-rater-label.csv"):
            file_path = os.path.join(root, file)
            print(f"Processing: {file_path}")

            # File temporaneo per scrittura sicura
            temp_fd, temp_path = tempfile.mkstemp()
            os.close(temp_fd)

            with open(file_path, 'r', newline='', encoding='utf-8') as infile, \
                 open(temp_path, 'w', newline='', encoding='utf-8') as outfile:

                reader = csv.reader(infile)
                writer = csv.writer(outfile)

                # Legge header e individua indice della colonna target_goal
                header = next(reader)
                if "target_goal" in header:
                    idx = header.index("target_goal")
                    header = header[:idx] + header[idx+1:]
                else:
                    idx = None

                writer.writerow(header)

                # Scrive righe senza la colonna
                for row in reader:
                    if idx is not None:
                        row = row[:idx] + row[idx+1:]
                    writer.writerow(row)

            # Sostituisce il file originale
            shutil.move(temp_path, file_path)

print("✅ Operazione completata.")