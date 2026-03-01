from flask import Flask, jsonify
import subprocess
import threading
import traceback
from datetime import datetime

app = Flask(__name__)

# Locks to ensure only one instance of each job runs at a time
build_lock = threading.Lock()
refresh_lock = threading.Lock()

def run_job_async(script_name, lock):
    """Run a script in the background with a lock."""
    # We acquire the lock with blocking=False so we fail immediately 
    # if it's already running, rather than queueing up.
    if not lock.acquire(blocking=False):
        print(f"[{datetime.now().isoformat()}] Job {script_name} is already running. Skipping.", flush=True)
        return False
        
    def _run():
        try:
            print(f"[{datetime.now().isoformat()}] Starting async job: {script_name}", flush=True)
            p = subprocess.run(["python", script_name], capture_output=True, text=True)
            if p.returncode == 0:
                print(f"[{datetime.now().isoformat()}] Job {script_name} finished successfully.\nSTDOUT: {p.stdout[-1000:]}", flush=True)
            else:
                print(f"[{datetime.now().isoformat()}] Job {script_name} FAILED (rc={p.returncode}).\nSTDERR: {p.stderr[-2000:]}", flush=True)
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] Exception in {script_name}:\n{traceback.format_exc()}", flush=True)
        finally:
            lock.release()
            print(f"[{datetime.now().isoformat()}] Lock released for {script_name}", flush=True)
            
    thread = threading.Thread(target=_run)
    # Daemon thread so it doesn't block the server shutting down if needed
    thread.daemon = True
    thread.start()
    return True

@app.get("/")
def health():
    return "OK", 200

@app.post("/run")
def run():
    started = run_job_async("build_pipeline.py", build_lock)
    if started:
        return jsonify({"status": "accepted", "message": "build_pipeline.py started in background"}), 202
    else:
        return jsonify({"status": "conflict", "message": "build_pipeline.py is already running"}), 409

@app.route("/refresh", methods=["POST"])
def refresh():
    started = run_job_async("refresh_stats.py", refresh_lock)
    if started:
        return jsonify({"status": "accepted", "message": "refresh_stats.py started in background"}), 202
    else:
        return jsonify({"status": "conflict", "message": "refresh_stats.py is already running"}), 409