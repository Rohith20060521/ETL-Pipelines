# run_pipeline.py
import sys
from pathlib import Path
import subprocess

BASE_DIR = Path(__file__).resolve().parents[0]

# Paths to your ETL scripts
EXTRACT_SCRIPT = BASE_DIR / "extract.py"
TRANSFORM_SCRIPT = BASE_DIR / "transform.py"
LOAD_SCRIPT = BASE_DIR / "load.py"
ANALYSIS_SCRIPT = BASE_DIR / "etl_analysis.py"

def run_script(script_path: Path):
    """Run a Python script and exit if it fails."""
    print(f"[RUNNING] Running {script_path.name} ...")
    result = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"[ERROR] Script {script_path.name} failed:\n{result.stderr}")
        sys.exit(1)
    print(f"[SUCCESS] {script_path.name} completed successfully.\n")

if __name__ == "__main__":
    print("[INFO] Starting full ETL + Analysis pipeline...\n")
    
    # Step 1: Extract
    run_script(EXTRACT_SCRIPT)

    # Step 2: Transform
    run_script(TRANSFORM_SCRIPT)

    # Step 3: Load
    run_script(LOAD_SCRIPT)

    # Step 4: Analysis
    run_script(ANALYSIS_SCRIPT)

    print("[COMPLETE] Full ETL + Analysis pipeline finished successfully!")
