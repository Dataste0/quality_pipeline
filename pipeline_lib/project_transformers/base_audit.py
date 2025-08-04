###############
# BASE - AUDIT
###############

# INPUT Taken
# [workflow, job_date, rater_id, auditor_id, job_id] \\ [r_label1, a_label1] [r_label2, a_label2] ...

# OUTPUT
# [base, content_week] [workflow, job_date, rater_id, auditor_id, job_id] \\ [final_job_score] 
# [parent_label, rater_response, auditor_response, is_correct, is_label_binary, confusion_type, weight]


import pandas as pd
import numpy as np
import logging
import traceback
from pipeline_lib.project_transformers import transformer_utils

MODEL_BASE = "audit"



TRUE_TOKENS  = {"1", "true", "yes", "y", "t", "agree", "correct"}
FALSE_TOKENS = {"0", "false", "no", "n", "f", "disagree", "incorrect"}

def _as_boolish(series: pd.Series) -> pd.Series:
    """Converte una serie in boolean (True/False) dove possibile, altrimenti NA."""
    s = series.copy()
    # numerici -> bool (0=False, altri=True)
    is_num = s.apply(lambda x: isinstance(x, (int, float)) and not pd.isna(x))
    s.loc[is_num] = s.loc[is_num].astype(float).astype(int).astype(str)

    # stringhe normalizzate
    s = s.astype(str).str.strip().str.lower()
    s = s.mask(s.isin({"nan", "none", ""}), other=np.nan)

    out = pd.Series(pd.NA, index=series.index, dtype="boolean")
    out = out.mask(s.isin(TRUE_TOKENS), True)
    out = out.mask(s.isin(FALSE_TOKENS), False)
    return out




def base_audit_etl(df, stats, base_config):
    """
    BASE_CONFIG EXAMPLE

    base_config = {
        
        "labels": [
            {
                "label_name": "able_to_eval",
                "auditor_column_type": "answer", #"answer/disagreement/agreement",
                "is_label_binary": true,
                "label_binary_pos_value": "EB_yes",
                "weight": 1
            },
            {
                "label_name": "withhold",
                "auditor_column_type": "answer",
                "is_label_binary": false,
                "weight": 1
            }
        ]
    }
    """

    try:
        label_dicts = base_config.get("labels", [])

        # Map label columns
        label_column_map = {
            f"rater_{v['label_name']}": f"{v['label_name']}|rater"
            for v in label_dicts
        } | {
            f"auditor_{v['label_name']}": f"{v['label_name']}|auditor"
            for v in label_dicts
        }
        df = df.rename(columns=label_column_map)
        
        # Needed columns
        info_columns = ["workflow", "job_date", "rater_id", "auditor_id", "job_id"]
        label_cols_renamed = list(label_column_map.values())

        required_df_cols = info_columns + label_cols_renamed
        df = df.loc[:, [col for col in required_df_cols if col in df.columns]]

        # ["workflow", "job_date", "rater_id", "job_id"] [is_rateable|rater, is_rateable|auditor] [withhold|rater, withhold|auditor] ...

        # Melt
        df_long = df.melt(
            id_vars=info_columns,
            value_vars=label_cols_renamed,
            var_name="label_role",
            value_name="response"
        )

        df_long[["parent_label", "role"]] = df_long["label_role"].str.rsplit("|", n=1, expand=True)

        df_long = df_long.drop(columns=["label_role"], errors="ignore")

        # Split
        rater_df = df_long[df_long["role"] == "rater"].copy()
        auditor_df = df_long[df_long["role"] == "auditor"].copy()

        rater_df = rater_df.rename(columns={"response": "rater_response"})
        auditor_df = auditor_df.rename(columns={"response": "auditor_response"})

        key_cols = info_columns + ["parent_label"]
        rater_df = rater_df[key_cols + ["rater_response"]]
        auditor_df = auditor_df[key_cols + ["auditor_response"]]

        
        # Recombine
        df_wide = pd.merge(
            rater_df,
            auditor_df,
            on=key_cols,
            how="left" 
        )
    
        df = df_wide


        # Map binary
        binary_map = {
            d["label_name"]: bool(d.get("is_label_binary", False))
            for d in label_dicts
            if "label_name" in d
        }
        df["is_label_binary"] = df["parent_label"].map(binary_map).fillna(False).astype("boolean")

        pos_value_map = {
            d["label_name"]: d.get("label_binary_pos_value", "True")
            for d in label_dicts
            if "label_name" in d
        }        
        expected_pos = df["parent_label"].map(pos_value_map)
        df["is_positive"] = (df["rater_response"] == expected_pos).where(df["is_label_binary"]).astype("boolean")

        
        weight_map = {
            d["label_name"]: (int(d["weight"]) if isinstance(d.get("weight"), (int, float)) \
                              or (isinstance(d.get("weight"), str) and d["weight"].strip().isdigit()) else 1)
            for d in label_dicts
            if "label_name" in d
        }
        df["weight"] = df["parent_label"].map(weight_map).fillna(1).astype(int)



        # Map Outcome
        df["is_correct"] = pd.Series(pd.NA, index=df.index, dtype="boolean")

        auditor_col_type = {
            d["label_name"]: d.get("auditor_column_type", "answer")
            for d in label_dicts
            if "label_name" in d
        }

        col_type_series = df["parent_label"].map(auditor_col_type).fillna("answer")
        r = df["rater_response"]
        a = df["auditor_response"]
        a_bool = _as_boolish(a)

        # masks
        mask_answer = col_type_series == "answer"
        mask_agreement = col_type_series == "agreement"
        mask_disagreement = col_type_series == "disagreement"

        is_correct = pd.Series(pd.NA, index=df.index, dtype="boolean")

        # answer: match if both not null
        eq_or_both_null = r.eq(a) | (r.isna() & a.isna())
        is_correct.loc[mask_answer] = eq_or_both_null.loc[mask_answer].astype("boolean")

        # agreement: prende il valore booleano dell'auditor
        is_correct.loc[mask_agreement] = a_bool.fillna(False)

        # disagreement: inversione (NA resta NA)
        is_correct.loc[mask_disagreement] = (~a_bool).astype("boolean")

        df["is_correct"] = is_correct



        # Confusion type
        is_binary = df["is_label_binary"] == True  # bool mask
        is_positive = df["is_positive"] == True
        is_correct = df["is_correct"] == True

        # Inizializza con NA
        confusion = pd.Series(pd.NA, index=df.index, dtype="string")

        # True Positive: positivo e corretto
        tp_mask = is_binary & is_positive & is_correct
        confusion.loc[tp_mask] = "TP"

        # True Negative: negativo (non positivo) e corretto
        tn_mask = is_binary & (~is_positive) & is_correct
        confusion.loc[tn_mask] = "TN"

        # False Positive: positivo e non corretto
        fp_mask = is_binary & is_positive & (~is_correct)
        confusion.loc[fp_mask] = "FP"

        # False Negative: negativo e non corretto
        fn_mask = is_binary & (~is_positive) & (~is_correct)
        confusion.loc[fn_mask] = "FN"

        # Categorizza con ordine (opzionale)
        confusion = pd.Categorical(
            confusion,
            categories=["TP", "TN", "FP", "FN"],
            ordered=False
        )
        df["confusion_type"] = pd.Series(confusion, index=df.index)


        final_cols = info_columns + \
            ["parent_label", "rater_response", "auditor_response", "is_label_binary", "is_positive", "weight", "is_correct", "confusion_type"]
        df = df[final_cols]
        
        return df

        

    except Exception as e:
        tb = traceback.format_exc()
        stats["transform_error"] = f"Unexpected error: {e}"
        print(f"BASE AUDIT ERROR: Unexpected error: {e}\n{tb}")
        return None
        



def base_transform(df, base_config):
    stats = {}
    stats["model_base"] = f"BASE-{MODEL_BASE.upper()}"

    stats["rows_before_transformation"] = len(df)

    if not base_config:
        stats["transform_error"] = "base_config_missing"
        return pd.DataFrame(), stats
    
    df = base_audit_etl(df, stats, base_config)
    
    df.insert(0, "base", MODEL_BASE)
    df.insert(1, "content_week", transformer_utils.compute_content_week(df["job_date"]))

    stats["rows_after_transformation"] = len(df)

    df.to_csv("base_audit_debug_output.csv", index=False)

    return df, stats