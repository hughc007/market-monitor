from datetime import datetime
import subprocess
import sys

SCRIPTS = [
    "ingest.py",
    "analysis.py",
    "signals.py",
    "charts.py",
    "desk_note.py",
]


def timestamp():
    return datetime.now().isoformat(sep=" ", timespec="seconds")


def run_script(script):
    print(f"[{timestamp()}] Starting {script}")
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[{timestamp()}] ERROR running {script}")
        print(result.stdout)
        print(result.stderr)
        raise SystemExit(result.returncode)
    print(f"[{timestamp()}] Completed {script}")


def main():
    print(f"[{timestamp()}] Starting full market monitor refresh")
    for script in SCRIPTS:
        run_script(script)
    print(f"[{timestamp()}] Market monitor pipeline complete")
    print("Outputs available in: outputs/charts/ and outputs/notes/")


if __name__ == "__main__":
    main()
