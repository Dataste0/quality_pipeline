import pandas as pd
import csv
import json
import re
import os
import ast
import hashlib
import zipfile
from datetime import datetime, timedelta

# --- Logger
import logging
logger = logging.getLogger(__name__)


# --- Load Project Masterfile DF

def load_project_info(filepath, sheet_name="Project List", active_only=False):
    logger.debug(f"Loading Project Masterfile (active_only parameter:{active_only})")
    df = pd.read_excel(filepath, sheet_name=sheet_name, engine='openpyxl')

    underscore_columns = [col for col in df.columns if col.startswith("_")]
    df_selected = df[underscore_columns].copy()
    df_selected.columns = [col[1:] for col in df_selected.columns]
    df = df_selected.copy()
    
    df = df[df["project_status"] != "Hidden"]  # Remove hidden projects
    
    if active_only:
        df = df[df["project_status"] == "Active"]  # Use only active projects
    
    logger.debug(f"Returning Project List")
    return df

def filter_inactive_projects(new_entries_df, project_df):
    active_projects = project_df[project_df["project_status"] == "Active"]
    merged_df = new_entries_df.merge(active_projects[["project_id"]], on="project_id", how="inner")
    return merged_df


# --- Extract File Pattern from Project Config

def extract_file_pattern(files_filter_dict):

    starts = files_filter_dict.get("begins_with", "")
    ends = files_filter_dict.get("ends_with", "")
    contains = files_filter_dict.get("contains", [])

    # Normalize contains
    if isinstance(contains, str):
        contains = [contains]
    elif not isinstance(contains, list):
        contains = []

    pattern = "^" # Case sensitive
    #pattern = "(?i)^"  # Case insensitive

    # Start pattern
    if starts:
        pattern += re.escape(starts)
    else:
        pattern += ".*"

    # Contains
    for item in contains:
        pattern += ".*" + re.escape(item)

    # End pattern
    if ends:
        pattern += ".*" + re.escape(ends) + "$"
    else:
        pattern += ".*$"
    
    logger.debug(f"Pattern extracted. Metadata: {files_filter_dict} >>> Pattern: {pattern}")
    return pattern



def get_project_target(project_id, project_list):
    match = project_list[project_list["project_id"] == project_id]
    if match.empty:
        logger.error(f"Get-Project-Target: Project ID '{project_id}' not found in project list")
        return None
    
    raw_target = match["project_target"].iloc[0]

    try:
        # String with %
        if isinstance(raw_target, str) and raw_target.strip().endswith('%'):
            return float(raw_target.strip().replace('%', '')) / 100.0

        # If numeric string (decimal)
        target = float(raw_target)

        # If > 1, probably in percent format (eg. 90)
        if target > 1:
            return target / 100.0
        return target

    except Exception as e:
        logger.error(f"Get-Project-Target: Invalid target format for project '{project_id}': {raw_target} ({e})")
        return None


def get_project_methodology(project_id, project_list):
    match = project_list[project_list["project_id"] == project_id]
    if match.empty:
        logger.error(f"Get-Project-Methodology: Project ID '{project_id}' not found in project list")
        return None
    
    project_methodology = match.iloc[0]["project_methodology"]
    #methodology_map = {
    #    "Audits": "audit",
    #    "Spot-check": "audit",
    #    "Golden Sets": "audit",
    #    "Quiz Sets": "audit",
    #    "Multi-Review": "multi",
    #    "Rubric": "rubric"
    #}

    #result = methodology_map.get(project_methodology)
    result = project_methodology
    if result is None:
        logger.error(f"Unknown project methodology '{project_methodology}' for project ID '{project_id}'")
    return result

def get_project_base(project_id, project_list):
    match = project_list[project_list["project_id"] == project_id]
    if match.empty:
        logger.error(f"Get-Project-Base: Project ID '{project_id}' not found in project list")
        return None
    
    project_base = match.iloc[0]["project_base"]

    if project_base is None:
        logger.error(f"Unknown project methodology '{project_base}' for project ID '{project_id}'")
    return project_base


# DATES

def get_friday_of_week(date):
    try:
        date = pd.to_datetime(date, errors='coerce')

        if pd.isna(date):
            raise ValueError("Invalid or missing date")

        weekday = date.weekday()  # 0=Mon, ..., 6=Sun
        custom_weekday = (weekday + 2) % 7
        days_to_friday = (6 - custom_weekday) % 7

        return date + timedelta(days=days_to_friday)

    except Exception as e:
        print(f"get_friday_of_week: invalid input '{date}' — {e}")
        return pd.NaT

def generate_we_dates(start_date, end_date=None):
    first_friday = get_friday_of_week(start_date)
    today = pd.Timestamp.today()

    if not end_date or pd.isna(end_date):
        end_date = today-timedelta(weeks=0)
    else:
        end_date = pd.to_datetime(end_date)

    end_friday = get_friday_of_week(end_date)
    return [first_friday + timedelta(weeks=i) for i in range(((end_friday - first_friday).days // 7) + 1)]



# Extracts list of weekendings from transformed dataframes' data

def get_content_weeks(submission_date_series):
    if submission_date_series is None or submission_date_series.empty:
        logger.warning(f"Submission date series is None or empty. Returning empty array")
        return []
    dates = pd.to_datetime(submission_date_series, errors='coerce')
    weekending_dates = dates.map(get_friday_of_week)
    unique_dates = weekending_dates.dropna().unique()
    return sorted([d.strftime("%Y-%m-%d") for d in unique_dates])


# TRANSFORMED FILES FUNCTIONS

def clean_filename(orig_filename, max_len=21):
    #cleaned = re.sub(r'[:/&%\[\]\(\)]', '', orig_filename)
    #cleaned = re.sub(r'\s{2,}', ' ', cleaned)
    #cleaned = re.sub(r'\s*-\s*', '-', cleaned)
    #cleaned = cleaned.replace(' ', '-')
    #cleaned = cleaned.replace('_', '-')
    #cleaned = cleaned.strip('-')
    cleaned = re.sub(r'[^a-zA-Z0-9]', '', orig_filename)
    if max_len:
        cleaned = cleaned[:max_len].ljust(max_len, "0")
    return cleaned


def generate_transformed_filename(project_id, data_week, orig_filename, content_week=None):
    new_name_no_ext = os.path.splitext(orig_filename)[0]
    cleaned = clean_filename(new_name_no_ext)
    max_middle_len = 21
    #max_middle_len = 58 - len(base) - len(suffix)
    short_clean_name = cleaned[:max_middle_len].ljust(max_middle_len, "0")
    week_str = pd.to_datetime(data_week).strftime("%Y-%m-%d")
    base = f"{project_id}_{week_str}_{short_clean_name}_"

    if content_week:
        # Parquet
        content_week_str = pd.to_datetime(content_week).strftime("%Y%m%d")
        suffix = f"C{content_week_str}.parquet"
    else:
        # UQ (CSV)
        suffix = "UQ.csv"
    
    filename = f"{base}{suffix}"
    return filename



# FILE HANDLING

def load_df_from_filepath(file_path):
    logger.debug(f"Loading dataframe from path: {file_path}")
    if not os.path.exists(file_path):
        logger.error(f"Unable to load dataframe from filepath. File not found {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")
    
    _, ext = os.path.splitext(file_path.lower())

    try:
        #if ext == ".csv":
        #    logger.debug(f"Loading CSV file")
        #    return pd.read_csv(file_path, dtype=str, keep_default_na=False)
        if ext == ".csv":
            logger.debug("Loading CSV file with UTF-8 encoding")
            try:
                return pd.read_csv(file_path, dtype=str, keep_default_na=False, encoding='utf-8')
            except UnicodeDecodeError as e:
                logger.warning(f"UTF-8 decoding failed: {e}. Retrying with latin1 encoding.")
                return pd.read_csv(file_path, dtype=str, keep_default_na=False, encoding='latin1')
            
        elif ext in [".xlsx", ".xls"]:
            logger.debug(f"Loading Excel file. Sheet[0]")
            return pd.read_excel(file_path, sheet_name=0, engine='openpyxl', dtype=str, keep_default_na=False)
        else:
            logger.error(f"Unsupported file type: {ext}")
            raise ValueError(f"Unsupported file type: {ext}")
    except Exception as e:
        logger.error(f"Failed to load file {file_path}: {e}")
        raise ValueError(f"Failed to load file {file_path}: {e}")

def save_df_to_filepath(df, output_path):
    logger.debug(f"Saving dataframe to path: {output_path}")
    df.to_csv(
        output_path,
        index=False,
        sep=",",
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n"
    )

def has_at_least_one_data_row(file_path):
    try:
        if file_path.lower().endswith(".csv"):
            df_sample = pd.read_csv(file_path, nrows=2)
        elif file_path.lower().endswith((".xls", ".xlsx")):
            df_sample = pd.read_excel(file_path, nrows=2)
        else:
            return False  # unsupported extension
        return len(df_sample) >= 1
    except Exception as e:
        logging.warning(f"Error reading file {file_path}: {e}")
        return False

# PROJECT FOLDERS AND WEEK FOLDERS

def get_project_folder(project_id, rawdata_root):
    logger.debug(f"Getting Project Folder of Project ID: {project_id}")
    if not isinstance(project_id, str) or not project_id.strip():
        logger.error(f"Invalid project ID provided: {project_id}")
        raise ValueError("Invalid project ID provided.")
    try:
        folders = [f for f in os.listdir(rawdata_root) if os.path.isdir(os.path.join(rawdata_root, f))]
    except FileNotFoundError:
        logger.error(f"Root folder not found: {rawdata_root}")
        raise FileNotFoundError(f"Root folder not found: {rawdata_root}")

    # Filter folders that start with the given project_id
    project_folders = [f for f in folders if f.startswith(f"{project_id}_")]
    exists = bool(project_folders)
    logger.debug(f"Project ID: {project_id} - Project Folder exists {exists}")

    if not exists:
        return {
            "exists": False,
            "name": None,
            "path": None
        }

    # Take the first matching folder (assuming project_id is unique)
    project_folder_name = project_folders[0]
    project_folder_path = os.path.join(rawdata_root, project_folder_name)
    logger.debug(f"Project ID: {project_id} - Project Folder name {project_folder_name}")

    # Return project folder name and path
    return {
        "exists": True,
        "name": project_folder_name,
        "path": project_folder_path
    }
    
def get_week_folder(data_week, project_id, rawdata_root):
    logger.debug(f"Getting Week folder for project id: {project_id}, data week: {data_week}")
    if data_week is None:
        logger.error(f"Missing 'data_week'. Expected a string or datetime. Provided: {data_week}")
        raise ValueError("Missing 'data_week'. Expected a string or datetime.")

    try:
        data_week_dt = pd.to_datetime(data_week)
    except Exception:
        logger.error(f"Invalid 'data_week'. Could not convert to datetime. Provided: {data_week}")
        raise ValueError("Invalid 'data_week'. Could not convert to datetime.")
    
    # Convert date to 'YYYY.MM.DD' and prepend 'WE '
    data_week_str = data_week_dt.strftime("%Y.%m.%d")
    week_folder_name = "WE " + data_week_str

    project_folder_info = get_project_folder(project_id, rawdata_root)
    if not project_folder_info["exists"]:
        logger.debug(f"Project-week folder does not exist.")
        return {
            "exists": None,
            "name": None,
            "path": None
        }
    
    project_folder_name = project_folder_info["name"]
    week_folder_path = os.path.join(rawdata_root, project_folder_name, week_folder_name)
    logger.debug(f"Returning project-week folder for: {project_id} - {week_folder_name}")
    return {
        "exists": bool(os.path.isdir(week_folder_path)),
        "name": week_folder_name,
        "path": week_folder_path
    }


# LIST HANDLING

def parse_file_list(val):
    logger.debug(f"Parsing list: {val}")
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return ast.literal_eval(val)
        except Exception as e:
            logger.error(f"Failed to parse string to list: {e} -> {val}")
            #print(f"[ERROR] Failed to parse string to list: {e} -> {val}")
    return []

def compare_files_list(prev, curr):
    logger.debug(f"Comparing file lists: {prev} - {curr}")
    prev_files = parse_file_list(prev)
    curr_files = parse_file_list(curr)
    
    new_files = []
    prev_hashes = set(f["hash"] for f in prev_files)
    for f in curr_files:
        if f["hash"] not in prev_hashes:
            logger.debug(f"Hash is different: {prev_files} - {curr_files}")
            #print(f"[DEBUG] {prev_files} VS {curr_files}")
            new_files.append(f)
    return new_files


# HASH UTILS

def hash_header(file_path):
    hasher = hashlib.md5()

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Hash_Header - File not found: {file_path}")

    if file_path.lower().endswith('.csv'):
        df = pd.read_csv(file_path, nrows=0, dtype=str)
    elif file_path.lower().endswith(('.xls', '.xlsx')):
        df = pd.read_excel(file_path, nrows=0, dtype=str)
    else:
        raise ValueError(f"Hash_Header - Unsupported file type: {file_path}")
    
    # Hash of sorted columns
    sorted_columns = sorted(df.columns.astype(str))
    header_string = '|'.join(sorted_columns).strip()
    md5_hash = hashlib.md5(header_string.encode('utf-8')).hexdigest()

    return md5_hash


def hash_file(file_path):
    hasher = hashlib.md5()
    
    # Filename
    filename = os.path.basename(file_path)
    hasher.update(filename.encode('utf-8'))
    
    # File size
    size = os.path.getsize(file_path)
    hasher.update(str(size).encode('utf-8'))

    # File modification time (mtime)
    mtime = int(os.path.getmtime(file_path))
    hasher.update(str(mtime).encode('utf-8'))

    # File chunks
    #with open(file_path, 'rb') as f:
    #    while chunk := f.read(8192):
    #        hasher.update(chunk)
        
    return hasher.hexdigest()


def hash_dir(folder_path, file_info_list=None):
    hasher = hashlib.md5()

    folder_name = os.path.basename(folder_path)
    parent_name = os.path.basename(os.path.dirname(folder_path))

    hasher.update(folder_name.encode("utf-8", errors="ignore"))
    hasher.update(parent_name.encode("utf-8", errors="ignore"))

    if file_info_list:
        hasher.update(str(len(file_info_list)).encode("utf-8"))

        for info in sorted(file_info_list, key=lambda x: x["filename"]):
            hasher.update(info["filename"].encode("utf-8", errors="ignore"))
            hasher.update(info["hash"].encode("utf-8", errors="ignore"))

    return hasher.hexdigest()



def hash_directory_fast(folder_path, is_active):
    hasher = hashlib.md5()
    base_folder = os.path.basename(folder_path)
    hasher.update(base_folder.encode("utf-8", errors="ignore"))
    hasher.update(str(is_active).encode("utf-8"))

    valid_ext = {".csv", ".xlsx", ".xls"}
    file_info = []

    for root, dirs, files in os.walk(folder_path):
        for filename in files:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in valid_ext:
                continue

            full_path = os.path.join(root, filename)
            relative_path = os.path.relpath(full_path, folder_path)

            try:
                size = os.path.getsize(full_path)
                mtime = os.path.getmtime(full_path)
                entry = f"{relative_path}:{size}:{mtime}"
                file_info.append(entry)
            except OSError:
                continue  # skip inaccessible files

    for entry in sorted(file_info):  # ensure consistent order
        hasher.update(entry.encode("utf-8", errors="ignore"))

    return hasher.hexdigest()


"""
def hash_directory_metadata(folder_path, project_is_active):
    try:
        hasher = hashlib.md5()

        # List all files
        files = [
            f for f in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, f))
        ]

        # Sort to ensure consistent hash
        files.sort()

        for fname in files:
            full_path = os.path.join(folder_path, fname)
            try:
                stat = os.stat(full_path)
                size = stat.st_size
                mtime = int(stat.st_mtime)

                # Use filename, size, mtime
                fingerprint = f"{fname}|{size}|{mtime}\n"
                hasher.update(fingerprint.encode("utf-8"))
            except Exception as e:
                # Log error but continue
                logger.warning(f"Error reading file {fname} for directory hash: {e}")
                continue

        hasher.update(f"ACTIVE={project_is_active}\n".encode("utf-8"))
        return hasher.hexdigest()

    except Exception as e:
        logger.error(f"Failed to hash directory metadata for {folder_path}: {e}")
        return "error_hash"
"""
################## NOT USED

# Reporting

def generate_rawdata_report(current_snapshot_df, project_df, savepath):
    
    summary_df = current_snapshot_df[
        ['project_id', 'project_name', 'data_week', 'file_number', 'valid_files_number']
    ].copy()

    summary_df['has_valid_data'] = summary_df['valid_files_number'] > 0

    summary_grouped = summary_df.groupby('project_id', as_index=False).agg(
        project_name=('project_name', 'first'),
        week_count=('data_week', 'count'),
        total_files=('file_number', 'sum'),
        total_valid_files=('valid_files_number', 'sum'),
        weeks_with_data=('has_valid_data', 'sum') 
    )

    top_valid_week_df = summary_df[summary_df['has_valid_data']].copy()
    top_valid_week_df = (
        top_valid_week_df.sort_values(['project_id', 'data_week'], ascending=[True, False])
        .drop_duplicates('project_id')
        [['project_id', 'data_week']]
        .rename(columns={'data_week': 'last_valid_data_week'})
    )

    summary_merged = summary_grouped.merge(
        project_df[['project_id', 'project_status', 'notes', 'data_type', 'project_start_date']],
        on='project_id',
        how='left'
    ).merge(
        top_valid_week_df,
        on='project_id',
        how='left'
    )

    
    summary_merged['data_coverage'] = summary_merged.apply(
        lambda row: row['weeks_with_data'] / row['week_count'] if row['week_count'] != 0 else 0,
        axis=1
    )

    summary_merged = summary_merged[[
        'project_id', 
        'project_name', 
        'project_start_date', 
        'last_valid_data_week', 
        'week_count', 
        'weeks_with_data', 
        'data_coverage', 
        'project_status', 
        'data_type', 
        'notes',
        'total_files',
        'total_valid_files'
    ]]

    current_date = pd.Timestamp.today()
    current_weekending = get_friday_of_week(current_date)
    summary_merged["current_week"] = current_weekending

    summary_merged.to_csv(savepath, index=False)


def extract_zip_file(folder_path):
    logging.debug(f"Extracting zip file: {folder_path}")
    for filename in os.listdir(folder_path):
        if not filename.lower().endswith(".zip"):
            continue

        zip_path = os.path.join(folder_path, filename)

        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(path=folder_path)

            os.remove(zip_path)
            print(f"✅ Extracted and removed: {filename}")

        except Exception as e:
            print(f"❌ Failed to extract {filename}: {e}")
