from flask import Flask, jsonify
import subprocess

app = Flask(__name__)

@app.get("/")
def health():
    return "OK", 200

@app.post("/run")
def run():
    p = subprocess.run(["python", "build_pipeline.py"], capture_output=True, text=True)
    return jsonify({
        "ok": p.returncode == 0,
        "returncode": p.returncode,
        "stdout": p.stdout[-4000:],
        "stderr": p.stderr[-4000:],
    }), (200 if p.returncode == 0 else 500)