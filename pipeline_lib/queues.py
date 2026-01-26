import os
import time
import pandas as pd
import ast
from datetime import datetime, timezone

class FileLock:
    def __init__(self, path):
        self.lock_path = f"{path}.lock"

    def acquire(self, timeout=30):
        start_time = time.time()
        while os.path.exists(self.lock_path):
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout while waiting for lock on {self.lock_path}")
            time.sleep(0.1)
        with open(self.lock_path, 'w') as f:
            f.write('locked')

    def release(self):
        if os.path.exists(self.lock_path):
            os.remove(self.lock_path)


class SnapshotManager:
    def __init__(self, filepath):
        self.filepath = filepath
        self.lock = FileLock(filepath)
        self.columns = [
            'timestamp', 
            'snapshot_id', 
            'project_id', 
            'project_name', 
            'data_week', 
            'folder_hash', 
            'has_any_data', 
            'has_weekly_data', 
            'file_number', 
            'file_list', 
            'valid_files_number', 
            'valid_files_list'
        ]
    
    def _generate_timestamp(self):
        return datetime.now(timezone.utc).isoformat(timespec="seconds")
    
    def _get_last_id(self):
        if not os.path.exists(self.filepath):
            return None
        
        df = pd.read_csv(self.filepath, usecols=lambda c: c == "snapshot_id" or False)
        if "snapshot_id" not in df.columns or df.empty:
            return None
        
        max_id = pd.to_numeric(df["snapshot_id"], errors="coerce").max()
        return int(max_id) if pd.notna(max_id) else None

    def _generate_id(self):
        last_id = self._get_last_id()
        return last_id + 1 if last_id is not None else 1
        
    def generate_snapshot_id(self):
        self.lock.acquire()
        try:
            return self._generate_id()
        finally:
            self.lock.release()
    
    def push(self, snapshot_id, entry_dict):
        self.lock.acquire()
        try:
            file_exists = os.path.exists(self.filepath)

            row = {col: "" for col in self.columns}
            row.update(entry_dict)
            row["snapshot_id"] = snapshot_id
            row["timestamp"] = self._generate_timestamp()

            df = pd.DataFrame([row], columns=self.columns)
            df = df.astype(str)

            with open(self.filepath, 'a', newline='') as f:
                df.to_csv(f, header=not file_exists, index=False)
        finally:
            self.lock.release()
    
    def add_snapshot(self, df):
        self.lock.acquire()
        try:
            file_exists = os.path.exists(self.filepath)

            snapshot_id = self._generate_id()

            df = df.copy()
            for col in self.columns:
                if col not in df.columns:
                    df[col] = ""

            df["timestamp"] = self._generate_timestamp()
            df["snapshot_id"] = snapshot_id

            df = df[self.columns]
            df = df.astype(str)

            with open(self.filepath, 'a', newline='') as f:
                df.to_csv(f, header=not file_exists, index=False)

        finally:
            self.lock.release()
    
    def get_snapshot(self, snapshot_id):
        self.lock.acquire()
        try:
            if not os.path.exists(self.filepath):
                return pd.DataFrame(columns=self.columns)
            df = pd.read_csv(self.filepath, low_memory=False)
            if "snapshot_id" not in df.columns:
                return pd.DataFrame(columns=df.columns)
            df["snapshot_id"] = pd.to_numeric(df["snapshot_id"], errors="coerce")
            return df[df["snapshot_id"] == snapshot_id].copy()
        finally:
            self.lock.release()

    def get_snapshot_item(self, item):
        self.lock.acquire()
        try:
            if not os.path.exists(self.filepath):
                return pd.DataFrame(columns=self.columns)
            df = pd.read_csv(self.filepath)
            if "snapshot_id" not in df.columns:
                return pd.DataFrame(columns=df.columns)
            df["snapshot_id"] = pd.to_numeric(df["snapshot_id"], errors="coerce")
            return df[df["snapshot_id"] == snapshot_id].copy()
        finally:
            self.lock.release()

    def get_last_snapshot_no(self):
        return self._get_last_id()
        
    def get_previous_snapshot_no(self):
        last_iter = self.get_last_snapshot_no()
        if last_iter is None or last_iter <= 1:
            return None
        return last_iter - 1




class TransformationQueueManager:
    def __init__(self, filepath):
        self.filepath = filepath
        self.lock = FileLock(filepath)
        self.columns = [
            'item_id',
            'timestamp',    
            'snapshot_id', 
            'project_id', 
            'project_name', 
            'data_week',
            'filename',
            'transform_status',
            'output_filenames',
            'content_weeks',
            'olap_sync'
        ]

    def _get_last_id(self):
        if not os.path.exists(self.filepath):
            return None

        df = pd.read_csv(self.filepath, usecols=lambda c: c == "item_id" or False)
        if "item_id" not in df.columns or df.empty:
            return None

        max_id = pd.to_numeric(df["item_id"], errors="coerce").max()
        return int(max_id) if pd.notna(max_id) else None

    def _generate_id(self):
        last_id = self._get_last_id()
        return last_id + 1 if last_id is not None else 1
    
    def _generate_timestamp(self):
        return datetime.now(timezone.utc).isoformat(timespec="seconds")
    
    def push(self, record_dict):
        self.lock.acquire()
        try:
            file_exists = os.path.exists(self.filepath)

            for col in self.columns:
                if col not in record_dict:
                    record_dict[col] = ""

            record = {
                "item_id": self._generate_id(),
                "timestamp": self._generate_timestamp(),
                "snapshot_id": str(record_dict["snapshot_id"]),
                "project_id": str(record_dict["project_id"]),
                "project_name": str(record_dict["project_name"]),
                "data_week": str(record_dict["data_week"]),
                "filename": str(record_dict["filename"]),
                "transform_status": "enqueued",
                "transform_info": "",
                "output_filenames": "",
                "content_weeks": "",
                "olap_sync": ""
                #**{col: str(record_dict[col]) for col in self.required_columns},
            }

            df = pd.DataFrame([record])
            with open(self.filepath, "a", newline="") as f:
                df.to_csv(f, header=not file_exists, index=False)
            
            return record["item_id"]
        finally:
            self.lock.release()

    def append_df(self, df):
        for _, row in df.iterrows():
            self.push(row.to_dict())

    def count(self, status="enqueued"):
        self.lock.acquire()
        try:
            if not os.path.exists(self.filepath):
                return 0
            df = pd.read_csv(self.filepath, dtype=str)

            if status in {"enqueued", "processing", "failed"}:
                return (df["transform_status"] == status).sum()
            
            elif status == "olap_sync_ready":
                transformed_series = df["transform_status"].fillna("").str.strip().str.lower()
                olap_sync_series = df["olap_sync"].fillna("").str.strip().str.lower()
                condition = (transformed_series == "transformed") & (olap_sync_series == "")
                return condition.sum()
            
            else:
                raise ValueError(f"Unsupported status: {status}")
        finally:
            self.lock.release()
    
    def pop(self, mode="enqueued"):
        self.lock.acquire()
        try:
            if not os.path.exists(self.filepath):
                return None

            df = pd.read_csv(self.filepath, dtype=str)

            if mode == "enqueued":
                queue = df[df["transform_status"] == "enqueued"]

            elif mode == "olap_sync_ready":
                transformed_series = df["transform_status"].fillna("").str.strip().str.lower()
                olap_sync_series = df["olap_sync"].fillna("").str.strip().str.lower()
                condition = (transformed_series == "transformed") & (olap_sync_series == "")
                queue = df[condition]

            else:
                raise ValueError(f"Unsupported mode: {mode}")

            if queue.empty:
                return None
            
            queue = queue.copy()
            queue["item_id_int"] = queue["item_id"].astype(int)
            oldest = queue.sort_values("item_id_int").iloc[0]
            record_id = oldest["item_id"]

            if mode == "transform":
                df.loc[df["item_id"] == record_id, "transform_status"] = "processing"

            df.to_csv(self.filepath, index=False)

            return df[df["item_id"] == record_id].iloc[0].to_dict()

        finally:
            self.lock.release()


    def complete_transform(self, record_id, result, updates=None):
        self.lock.acquire()
        try:
            if not os.path.exists(self.filepath):
                raise FileNotFoundError("Queue file not found.")

            df = pd.read_csv(self.filepath, dtype=str)

            if record_id not in df["item_id"].values:
                raise ValueError(f"Item ID '{record_id}' not found.")

            df.loc[df["item_id"] == record_id, "transform_status"] = result

            if updates:
                for key, value in updates.items():
                    if key not in df.columns:
                        raise ValueError(f"Column '{key}' not found in the table.")
                    df.loc[df["item_id"] == record_id, key] = str(value)

            df.to_csv(self.filepath, index=False)
        finally:
            self.lock.release()

    
    def mark_olap_synced(self, record_id):
        self.lock.acquire()
        try:
            if not os.path.exists(self.filepath):
                return False

            df = pd.read_csv(self.filepath, dtype=str)
            if not (df["item_id"] == record_id).any():
                return False

            df.loc[df["item_id"] == record_id, "olap_sync"] = 'true'
            df.to_csv(self.filepath, index=False)
            return True
        finally:
            self.lock.release()
    
