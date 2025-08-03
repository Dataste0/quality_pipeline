import os
import pandas as pd

def scan_headers(root_dir, output_file="header_report.csv"):
    header_rows = []

    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            fpath = os.path.join(dirpath, fname)
            ext = os.path.splitext(fname)[-1].lower()

            try:
                if ext == ".csv":
                    df = pd.read_csv(fpath, nrows=0)
                elif ext in [".xls", ".xlsx"]:
                    df = pd.read_excel(fpath, sheet_name=0, nrows=0)
                else:
                    continue  # skip non-matching files

                header = list(df.columns)
                header_rows.append({
                    "file_path": fpath,
                    "columns": ";".join(header)
                })

            except Exception as e:
                print(f"Errore nel file {fpath}: {e}")
                #header_rows.append({
                #    "file_path": fpath,
                #    "columns": f"ERROR: {e}"
                #})

    # Scrivi su CSV
    pd.DataFrame(header_rows).to_csv(output_file, index=False)
    print(f"\n✅ Report generato: {output_file}")


scan_headers("/mnt/c/rawdata")