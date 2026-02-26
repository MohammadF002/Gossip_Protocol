import subprocess
import time
import os
import shutil
import sys
import atexit

N_VALUES = [10, 20, 50]
NUM_RUNS = 5

BASE_PORT = 9000
FORMATION_TIME = 8
GOSSIP_TIME = 10
NODE_SCRIPT = "node.py"

all_processes = []


def safe_terminate(processes):
    for p in processes:
        if p.poll() is None:
            p.terminate()

    deadline = time.time() + 5
    while time.time() < deadline:
        if all(p.poll() is not None for p in processes):
            return
        time.sleep(0.2)

    for p in processes:
        if p.poll() is None:
            p.kill()

def cleanup():
    safe_terminate(all_processes)

atexit.register(cleanup)

def run_experiment(N, seed, base_port):

    run_folder = f"logs/N_{N}_seed_{seed}"

    if os.path.exists(run_folder):
        shutil.rmtree(run_folder)

    os.makedirs(run_folder)

    processes = []

    print(f"\n[RUN] N={N} | seed={seed}")

    env = os.environ.copy()
    env["LOG_DIR"] = run_folder

    # bootstrap
    bootstrap = subprocess.Popen(
        ["py", NODE_SCRIPT,
         "--port", str(base_port),
         "--seed", str(seed)],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        env=env,
        text=True
    )

    processes.append(bootstrap)
    all_processes.append(bootstrap)

    time.sleep(2)

    for i in range(1, N):
        port = base_port + i
        p = subprocess.Popen(
            [sys.executable, NODE_SCRIPT,
             "--port", str(port),
             "--bootstrap", f"127.0.0.1:{base_port}",
             "--seed", str(seed + i)],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            env=env,
            text=True
        )
        processes.append(p)
        all_processes.append(p)
        time.sleep(0.3)

    print("⏳ Waiting for network formation...")
    time.sleep(FORMATION_TIME)


    print("🚀 Triggering gossip...")
    bootstrap.stdin.write("hello\n")
    bootstrap.stdin.flush()

    time.sleep(GOSSIP_TIME)

    safe_terminate(processes)
    time.sleep(2)


if __name__ == "__main__":
    for N in N_VALUES:
        for run in range(NUM_RUNS):
            seed = 100 + run
            base_port = 9000 + run * 200
            run_experiment(N, seed, base_port)
