import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
from time import sleep

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0]
STAGED_DIR = BASE_DIR / "data" / "staged"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Please set SUPABASE_URL and SUPABASE_KEY in your .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "air_quality_data"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    time TIMESTAMP,
    pm10 DOUBLE PRECISION,
    pm2_5 DOUBLE PRECISION,
    carbon_monoxide DOUBLE PRECISION,
    nitrogen_dioxide DOUBLE PRECISION,
    sulphur_dioxide DOUBLE PRECISION,
    ozone DOUBLE PRECISION,
    uv_index DOUBLE PRECISION,
    aqi_category TEXT,
    severity_score DOUBLE PRECISION,
    risk_flag TEXT,
    hour INTEGER
);
"""

def create_table_if_not_exists():
    """Try to create table using RPC. If RPC missing, print SQL."""
    try:
        print("[WORKING] Attempting to create table in Supabase (if permitted)...")
        supabase.rpc("execute_sql", {"query": CREATE_TABLE_SQL}).execute()
        print("[SUCCESS] Table check completed (RPC executed or table already exists).")
    except Exception as e:
        print(f"[WARNING] Could not create table via RPC: {e}")
        print("[INFO] Please run this SQL manually in Supabase if needed:")
        print(CREATE_TABLE_SQL)



def _read_staged_csv(staged_path: str) -> pd.DataFrame:
    df = pd.read_csv(staged_path)

    # Convert timestamp → ISO string
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce").astype(str)

    return df


def load_to_supabase(staged_csv_path: str, batch_size: int = 200):
    if not Path(staged_csv_path).exists():
        raise FileNotFoundError(f"Staged CSV not found at {staged_csv_path}")

    df = _read_staged_csv(staged_csv_path)

    # Fix column names to match table schema
    df = df.rename(
        columns={
            "AQI": "aqi_category",
            "severity": "severity_score",
            "risk": "risk_flag"
        }
    )

    # Convert NaN → NULL
    df = df.where(pd.notnull(df), None)

    total = len(df)
    print(f"[LOADING] Loading {total} rows into '{TABLE_NAME}' in batches of {batch_size} ...")

    records = df.to_dict(orient="records")
    inserted = 0

    # Batch insert
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        batch_number = (i // batch_size) + 1
        end = min(i + batch_size, total)

        retries = 0

        while retries <= 2:
            try:
                res = supabase.table(TABLE_NAME).insert(batch).execute()

                # Check for errors depending on Supabase client version
                if hasattr(res, "error") and res.error:
                    print(f"[WARNING] Batch {batch_number} error: {res.error}")
                else:
                    print(f"[SUCCESS] Inserted rows {i+1}-{end} of {total}")
                    inserted += len(batch)
                break

            except Exception as e:
                print(f"[WARNING] Exception in batch {batch_number}: {e}")
                retries += 1

                if retries <= 2:
                    print(f"[RETRY] Retrying batch {batch_number} in 3s... (attempt {retries}/2)")
                    sleep(3)
                else:
                    print(f"[ERROR] Batch {batch_number} failed after 2 retries.")
                    break

    print("[COMPLETE] Load complete.")
    print(f"[STATS] Total inserted: {inserted}/{total}")


if __name__ == "__main__":
    staged_files = sorted([
        str(p) for p in STAGED_DIR.glob("air_quality_transformed*.csv")
    ])

    if not staged_files:
        raise SystemExit("No air quality staged CSV found. Run transform.py first.")

    create_table_if_not_exists()
    load_to_supabase(staged_files[-1], batch_size=200)
