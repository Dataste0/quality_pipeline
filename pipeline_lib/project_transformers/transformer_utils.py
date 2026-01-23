import pandas as pd
import re
import math
import numpy as np


# --- Logger
import logging
logger = logging.getLogger(__name__)



#####################
# ID, DATE FORMAT
#####################

from dateutil import parser

def convert_tricky_date(date_value):
    if pd.isna(date_value) or str(date_value).strip() == "":
        return None

    # Try parsing with dateutil (very flexible parser)
    if isinstance(date_value, str):
        try:
            converted_date = parser.parse(date_value, fuzzy=True)
            return converted_date.strftime("%Y-%m-%d")
        except Exception:
            pass

    # Try parsing standard ISO formats
    try:
        converted_date = pd.to_datetime(date_value, errors='coerce')
        if pd.notna(converted_date):
            return converted_date.strftime("%Y-%m-%d")
    except Exception:
        pass

    # Try Excel serial date
    try:
        if isinstance(date_value, (int, float)) or (isinstance(date_value, str) and date_value.replace(".", "", 1).isdigit()):
            converted_date = pd.to_datetime(float(date_value), origin="1899-12-30", unit="D")
            return converted_date.strftime("%Y-%m-%d")
    except Exception as e:
        logging.warning(f"Failed to parse Excel date: {date_value}. Error: {e}")

    logging.warning(f"Unrecognized date format: {date_value}")
    return None


# Get content week of job date
def compute_content_week(dates: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dates, errors="coerce")

    weekday = dt.dt.weekday  # 0=Mon ... 6=Sun
    custom_weekday = (weekday + 2) % 7
    days_to_friday = (6 - custom_weekday) % 7
    result_date = dt + pd.to_timedelta(days_to_friday, unit="d")

    return result_date.dt.strftime('%Y-%m-%d')


# Actor ID/Job ID check
"""
def id_format_check(val):
    int_number_regex = re.compile(r'^\d+$')
    if isinstance(val, str) and int_number_regex.fullmatch(val) and 'e' not in val.lower():
        return val
    return None
"""

def id_format_check(val):
    _int_re = re.compile(r'^\d+$')  # only digits

    # NaN / None
    if val is None:
        return pd.NA
    if isinstance(val, float) and math.isnan(val):
        return pd.NA
    if pd.isna(val):
        return pd.NA

    # Bigints (over 10 digits)
    if isinstance(val, (int, np.integer)):
        s = str(val)
        return val if len(s) > 10 else pd.NA

    # Strings
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return pd.NA
        # Refuse exponential notation 'e'/'E'
        if 'e' in s.lower():
            return pd.NA
        # only digits
        if not _int_re.fullmatch(s):
            return pd.NA
        # constraint: over 10 digits
        if len(s) <= 10:
            return pd.NA
        return s

    # unsupported types
    return pd.NA


#####################
# DATAFRAME FUNCTIONS
#####################

def column_replacer(df, replace_dict):
    origin = replace_dict.get("from", None)
    destination = replace_dict.get("to", None)
    if origin and destination and origin in df.columns:
        df.rename(columns={origin: destination}, inplace=True)

def string_replacer(df, replace_dict):
    origin = replace_dict.get("find", None)
    destination = replace_dict.get("replace", "")
    column_list = replace_dict.get("columns", [])
    if not origin or not column_list:
        return
    for col in column_list:
        if col in df.columns:
            df[col] = df[col].str.replace(origin, destination, regex=False)

def regex_replacer(df, replace_dict):
    import re
    
    pattern = replace_dict.get("pattern")
    replace = replace_dict.get("replace", "")
    column_list = replace_dict.get("columns", [])
    if not pattern or not column_list:
        return
    if isinstance(pattern, str):
        pat = re.compile(pattern)
    else:
        pat = pattern
    for col in column_list:
        if col in df.columns:
            df[col] = df[col].str.replace(pat, replace, regex=True)


def expand_label_columns(df, label_col, prefix, excluded_list=None):
    excluded_set = set(x.strip() for x in (excluded_list or []))
    tmp = df[[label_col]].copy()
    tmp[label_col] = tmp[label_col].apply(lambda x: x if isinstance(x, list) else [])
    exploded = tmp.explode(label_col).reset_index()  # keeing original index

    def split_kv(s):
        if not isinstance(s, str) or "::" not in s:
            return pd.Series({f"{prefix}_key": None, f"{prefix}_value": None})
        k, v = s.split("::", 1)
        k_clean = k.strip()
        return pd.Series({f"{prefix}_key": k_clean, f"{prefix}_value": v})

    kv = exploded[label_col].apply(split_kv)
    exploded = pd.concat([exploded, kv], axis=1)

    # scarta le key escluse (senza prefisso: qui è solo 'quality', 'speed', ecc.)
    exploded = exploded[~exploded[f"{prefix}_key"].isin(excluded_set)]

    pivoted = (
        exploded
        .dropna(subset=[f"{prefix}_key"])
        .pivot_table(
            index=exploded["index"],
            columns=f"{prefix}_key",
            values=f"{prefix}_value",
            aggfunc=lambda x: x.iloc[0] if len(x) else None,
        )
    )
    pivoted.columns = [f"{prefix}_{col}" for col in pivoted.columns]
    pivoted = pivoted.reindex(df.index, fill_value=None)
    return pivoted



def to_long(result: pd.DataFrame,
            base_cols: list[str],
            all_labels: list[str] | None = None) -> pd.DataFrame:
    # deduci le label se non fornite
    if all_labels is None:
        r_labels = [c[2:] for c in result.columns if c.startswith("r_")]
        a_labels = [c[2:] for c in result.columns if c.startswith("a_")]
        # preserva ordine: prima rater poi eventuali extra auditor
        all_labels = list(dict.fromkeys(r_labels + a_labels))

    has_auditor = any(c.startswith("a_") for c in result.columns)
    n = len(result)
    base = result[base_cols].copy()

    frames = []
    for label in all_labels:
        r_col = f"r_{label}"
        r = result[r_col] if r_col in result.columns else pd.Series([""]*n, index=result.index)

        df_lab = base.copy()
        df_lab["parent_label"] = label
        df_lab["rater_response"] = r.fillna("")

        if has_auditor:
            a_col = f"a_{label}"
            a = result[a_col] if a_col in result.columns else pd.Series([""]*n, index=result.index)
            df_lab["auditor_response"] = a.fillna("")

        frames.append(df_lab)

    out = pd.concat(frames, ignore_index=True)

    # opzionale: ordina in modo stabile
    #sort_cols = base_cols + ["parent_label"]
    #out = out.sort_values(sort_cols, kind="stable").reset_index(drop=True)
    return out



def add_binary_flags(df_long: pd.DataFrame, binary_labels: list[dict]) -> pd.DataFrame:
    out = df_long.copy()

    # mappa: parent_label -> binary_pos_value (case-insensitive su label e valori)
    pos_map = {d["label_name"]: str(d["binary_positive_value"])
               for d in binary_labels}

    # normalizziamo label e risposte
    lbl = out["parent_label"].astype(str)
    r_resp = out["rater_response"].fillna("").astype(str)
    has_aud = "auditor_response" in out.columns
    if has_aud:
        a_resp = out["auditor_response"].fillna("").astype(str)

    # is_label_binary: True se la label è nel dizionario
    out["is_label_binary"] = lbl.isin(pos_map.keys())

    # valore positivo atteso per la label (NaN se non binaria)
    expected_pos = lbl.map(pos_map)

    # rater_positive: True se binaria e match col valore positivo
    out["rater_positive"] = out["is_label_binary"] & (r_resp == expected_pos)

    # auditor_positive: se c'è la colonna; altrimenti crea colonna con NA
    if has_aud:
        out["auditor_positive"] = out["is_label_binary"] & (a_resp == expected_pos)
    #else:
    #    out["auditor_positive"] = pd.NA  # nessun auditor in questo dataset

    # confusion_type solo per label binarie e solo se l'auditor esiste
    if has_aud:
        rp = out["rater_positive"]
        ap = out["auditor_positive"]

        out["confusion_type"] = ""
        # TP, FP, FN, TN
        out.loc[out["is_label_binary"] & (rp & ap), "confusion_type"] = "TP"
        out.loc[out["is_label_binary"] & (rp & ~ap), "confusion_type"] = "FP"
        out.loc[out["is_label_binary"] & (~rp & ap), "confusion_type"] = "FN"
        out.loc[out["is_label_binary"] & (~rp & ~ap), "confusion_type"] = "TN"
    #else:
        # senza auditor non ha senso la confusione → stringa vuota
        #out["confusion_type"] = ""
    
    out.drop(columns=["rater_positive", "auditor_positive"], inplace=True, errors="ignore")

    return out



def add_responses_match(df_long: pd.DataFrame,
                        col_name: str = "is_correct",
                        case_sensitive: bool = True,
                        strip: bool = True) -> pd.DataFrame:
    out = df_long.copy()

    if "auditor_response" not in out.columns:
        # Se non esiste la colonna auditor, non è confrontabile: metto NA booleani
        #out[col_name] = pd.Series([pd.NA] * len(out), dtype="boolean")
        return out

    r = out["rater_response"].fillna("").astype("string")
    a = out["auditor_response"].fillna("").astype("string")

    if strip:
        r = r.str.strip()
        a = a.str.strip()
    if not case_sensitive:
        r = r.str.lower()
        a = a.str.lower()

    out[col_name] = (r == a)

    return out



def generate_rubric(
    df,
    rubric_column,
    score_column,
    provided_rubric=None,
    rubric_entries_cols=None,
    warn_on_conflicts=True
):
    import re

    df = df.copy()
    df[score_column] = pd.to_numeric(df[score_column], errors="coerce")
    df = df[df[score_column].notna()]
    
    #df[score_column] = df[score_column].astype(float)
    df["deduction"] = 100 - df[score_column]

    provided_rubric = provided_rubric or []

    # --------------------
    # helper: normalize / slugify short name
    # --------------------
    def to_short_name(s):
        s = str(s).strip().lower()
        s = s.replace("/", "_").replace("-", "_").replace(" ", "_")
        s = re.sub(r"[^a-z0-9_]+", "", s)   # keep only alnum + underscore
        s = re.sub(r"_+", "_", s)          # collapse multiple underscores
        s = s.strip("_")
        return s

    # --------------------
    # STEP 0: seed penalties (source of truth) from provided_rubric
    # map "extended name" -> penalty
    # --------------------
    seed_penalties = {}
    seed_meta = {}  # extended -> full provided object (for priority output)

    for item in provided_rubric:
        ext = item.get("rubric_entry", item.get("rubric_column"))
        if ext is None:
            continue
        ext = str(ext).strip()

        pen = item.get("rubric_penalty", None)
        if pen is None:
            continue
        pen = float(pen)

        seed_penalties[ext] = pen
        seed_meta[ext] = {
            "rubric_entry": ext,  # normalize key in output
            "rubric_name": item.get("rubric_name") or to_short_name(ext),
            "rubric_penalty": pen
        }

    penalties = dict(seed_penalties)       # extended -> penalty
    seeded_keys = set(seed_penalties.keys())

    # --------------------
    # helpers: read rubric as list of (extended_entry_name, factor) for a row
    # --------------------
    def row_items(r):
        # WIDE format
        if rubric_entries_cols is not None:
            items = []
            for col in rubric_entries_cols:
                val = r[col]

                # skip None / NaN
                if val is None:
                    continue
                if isinstance(val, float) and val != val:
                    continue

                try:
                    f = float(val)
                except Exception:
                    continue

                if f == 0:
                    continue

                items.append((str(col).strip(), f))
            return items

        # DICT format
        rub = r[rubric_column]
        return [(str(k).strip(), float(v)) for k, v in rub.items()]

    # helper: set penalty with priority to seeds
    def set_penalty(entry_ext, value, source_label):
        # never overwrite seeded
        if entry_ext in seeded_keys:
            if warn_on_conflicts and penalties.get(entry_ext) != value:
                print(
                    f"WARNING (seed priority): {entry_ext} kept at {penalties[entry_ext]} "
                    f"but {source_label} suggests {value}"
                )
            return False

        # keep first computed value (no overwrites); warn if conflicting
        if entry_ext in penalties:
            if warn_on_conflicts and penalties[entry_ext] != value:
                print(
                    f"WARNING: {entry_ext} already {penalties[entry_ext]} "
                    f"but {source_label} suggests {value} (keeping existing)"
                )
            return False

        penalties[entry_ext] = float(value)
        return True

    # --------------------
    # STEP 1: bootstrap from single-entry rows
    # --------------------
    for _, r in df.iterrows():
        items = row_items(r)
        if len(items) != 1:
            continue

        entry_ext, factor = items[0]
        if factor == 0:
            continue

        value = float(r["deduction"]) / float(factor)
        set_penalty(entry_ext, value, source_label="single-entry bootstrap")

    # --------------------
    # STEP 2: iterative propagation (rows with exactly 1 unknown)
    # --------------------
    changed = True
    while changed:
        changed = False

        for _, r in df.iterrows():
            items = row_items(r)
            if not items:
                continue

            d = float(r["deduction"])
            known_sum = 0.0
            unknown = []

            for entry_ext, factor in items:
                if entry_ext in penalties:
                    known_sum += float(factor) * float(penalties[entry_ext])
                else:
                    unknown.append((entry_ext, float(factor)))

            if len(unknown) == 1:
                entry_u, factor_u = unknown[0]
                if factor_u == 0:
                    continue
                value = (d - known_sum) / factor_u
                if set_penalty(entry_u, value, source_label="propagation"):
                    changed = True

    # --------------------
    # STEP 3: build final rubric output (provided has priority)
    # --------------------
    final = []

    # 3a) include provided (priority) as-is (normalized keys + ensured short name)
    # also track used short names to avoid duplicates
    used_short = set()
    provided_ext_set = set()

    for ext, obj in seed_meta.items():
        short = obj.get("rubric_name") or to_short_name(ext)
        short = to_short_name(short)  # normalize even if user provided something odd
        pen = float(obj["rubric_penalty"])

        final.append({
            "rubric_extended": ext,
            "rubric_name": short,
            "rubric_penalty": pen
        })
        used_short.add(short)
        provided_ext_set.add(ext)

    # 3b) add discovered entries not in provided
    # deterministic order
    for ext in sorted(penalties.keys()):
        if ext in provided_ext_set:
            continue

        short = to_short_name(ext)
        base = short
        i = 2
        while short in used_short:
            short = f"{base}_{i}"
            i += 1

        final.append({
            "rubric_extended": ext,
            "rubric_name": short,
            "rubric_penalty": float(penalties[ext])
        })
        used_short.add(short)

    return final
