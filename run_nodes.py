import subprocess
import time
import sys

NUM_NODES = 10
BASE_PORT = 9000

processes = []

print("[STARTING NODES]")

p = subprocess.Popen(
    ["py", "node.py", "--port", str(BASE_PORT)],
    creationflags=subprocess.CREATE_NEW_CONSOLE
)
processes.append(p)

time.sleep(2)

for i in range(1, NUM_NODES):
    port = BASE_PORT + i
    p = subprocess.Popen(
        [
            "py", "node.py",
            "--port", str(port),
            "--bootstrap", f"127.0.0.1:{BASE_PORT}"
        ],
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )
    processes.append(p)
    time.sleep(1)

print("[ALL NODES STARTED]")
print("Press Ctrl+C to stop all nodes.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping all nodes...")
    for p in processes:
        p.terminate()