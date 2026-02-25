import os
import glob
import sys
import math


COVERAGE_RATIO = 0.95


# =====================================
# Parsing Log Files
# =====================================

def parse_logs(log_dir):

    origin_time = None
    origin_msg_id = None

    recv_times = []
    sent_times = []

    log_files = glob.glob(os.path.join(log_dir, "log_*.txt"))

    if not log_files:
        raise RuntimeError("No log files found in directory")

    for filepath in log_files:

        try:
            with open(filepath, "r") as f:
                for line in f:

                    parts = line.strip().split()
                    if len(parts) < 2:
                        continue

                    tag = parts[0]

                    # ORIGIN msg_id timestamp
                    if tag == "ORIGIN" and len(parts) >= 3:
                        origin_msg_id = parts[1]
                        origin_time = float(parts[2])

                    # RECV msg_id timestamp
                    elif tag == "RECV" and len(parts) >= 3:
                        msg_id = parts[1]
                        ts = float(parts[2])

                        if origin_msg_id and msg_id == origin_msg_id:
                            recv_times.append(ts)

                    # SENT msg_id timestamp
                    elif tag == "SENT" and len(parts) >= 3:
                        ts = float(parts[-1])
                        sent_times.append(ts)

        except Exception:
            continue

    if origin_time is None:
        raise RuntimeError("No ORIGIN event found in logs")

    return origin_time, origin_msg_id, recv_times, sent_times


# =====================================
# Metrics Computation
# =====================================

def compute_metrics(log_dir, N):

    origin_time, msg_id, recv_times, sent_times = parse_logs(log_dir)

    recv_times.sort()

    required = math.ceil(COVERAGE_RATIO * N)

    if len(recv_times) < required:
        raise RuntimeError(
            f"Coverage not reached: only {len(recv_times)} of {N} nodes received the message"
        )

    convergence_timestamp = recv_times[required - 1]
    convergence_time = convergence_timestamp - origin_time

    overhead = sum(
        1 for t in sent_times
        if t <= convergence_timestamp
    )

    return convergence_time, overhead


# =====================================
# MAIN
# =====================================

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: python analyze.py <log_dir> <N>")
        sys.exit(1)

    log_dir = sys.argv[1]
    N = int(sys.argv[2])

    try:
        conv, over = compute_metrics(log_dir, N)
        print(f"{conv},{over}")

    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)