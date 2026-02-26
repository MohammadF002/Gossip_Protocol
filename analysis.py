import os
import glob
import statistics
import csv
import sys

COVERAGE_RATIO = 0.95


# --------------------------------------------------
# Parse one run folder
# --------------------------------------------------

def parse_run(run_folder):
    log_files = glob.glob(os.path.join(run_folder, "log_*.txt"))
    origin_time = None
    gossip_id = None
    recv_times = []
    sent_times = []

    for log_file in log_files:
        with open(log_file, "r") as f:
            for line in f:
                parts = line.strip().split()
                if not parts:
                    continue

                tag = parts[0]

                if tag == "ORIGIN":
                    gossip_id = parts[1]
                    origin_time = float(parts[2])

                elif tag == "RECV":
                    t = float(parts[2])
                    recv_times.append(t)


                elif tag == "SENT":
                    # IMPORTANT: count ALL sent messages
                    t = float(parts[2])
                    sent_times.append(t)

    return origin_time, gossip_id, recv_times, sent_times


# --------------------------------------------------
# Compute metrics for one run
# --------------------------------------------------

def compute_metrics(run_folder, N):
    origin_time, gossip_id, recv_times, sent_times = parse_run(run_folder)

    if origin_time is None or gossip_id is None:
        print(f"❌ ORIGIN not found in {run_folder}")
        return None

    if len(recv_times) == 0:
        print(f"❌ No RECV events in {run_folder}")
        return None

    required = int((N - 1) * COVERAGE_RATIO)

    if len(recv_times) < required:
        print(f"⚠️ 95% coverage not reached in {run_folder}")
        return None

    recv_times.sort()
    t_95 = recv_times[required - 1]

    convergence_time = t_95 - origin_time

    # Count ALL sent messages until t_95
    overhead = sum(1 for t in sent_times if t <= t_95)

    return convergence_time, overhead


# --------------------------------------------------
# Analyze all runs
# --------------------------------------------------

def analyze_all(base_logs_dir="logs"):
    per_run_results = []

    for folder in os.listdir(base_logs_dir):
        if not folder.startswith("N_"):
            continue

        parts = folder.split("_")
        N = int(parts[1])
        seed = int(parts[3])

        run_folder = os.path.join(base_logs_dir, folder)

        metrics = compute_metrics(run_folder, N)

        if metrics is None:
            continue

        convergence, overhead = metrics

        print(f"[OK] N={N} seed={seed} "
              f"convergence={convergence:.4f}s "
              f"overhead={overhead}")

        per_run_results.append((N, seed, convergence, overhead))

    return per_run_results


# --------------------------------------------------
# Aggregate results (mean + std)
# --------------------------------------------------

def aggregate_results(per_run_results):
    aggregated = {}

    for N, seed, convergence, overhead in per_run_results:
        if N not in aggregated:
            aggregated[N] = {"conv": [], "over": []}

        aggregated[N]["conv"].append(convergence)
        aggregated[N]["over"].append(overhead)

    summary = []

    for N in sorted(aggregated.keys()):
        conv_list = aggregated[N]["conv"]
        over_list = aggregated[N]["over"]

        conv_mean = statistics.mean(conv_list)
        conv_std = statistics.stdev(conv_list) if len(conv_list) > 1 else 0

        over_mean = statistics.mean(over_list)
        over_std = statistics.stdev(over_list) if len(over_list) > 1 else 0

        summary.append((N, conv_mean, conv_std, over_mean, over_std))

    return summary


# --------------------------------------------------
# Write CSV outputs
# --------------------------------------------------

def write_outputs(per_run_results, summary):
    # Detailed per-run results
    with open("results_detailed.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["N", "seed", "convergence", "overhead"])
        writer.writerows(per_run_results)

    # Aggregated summary
    with open("results_summary.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "N",
            "conv_mean",
            "conv_std",
            "overhead_mean",
            "overhead_std"
        ])
        writer.writerows(summary)


# --------------------------------------------------
# Main
# --------------------------------------------------

if __name__ == "__main__":
    base_dir = sys.argv[1] if len(sys.argv) > 1 else "logs"

    per_run_results = analyze_all(base_dir)
    summary = aggregate_results(per_run_results)

    write_outputs(per_run_results, summary)

    print("\n===== Aggregated Results =====")
    for row in summary:
        N, conv_mean, conv_std, over_mean, over_std = row
        print(f"N={N}")
        print(f"  Convergence: {conv_mean:.4f} ± {conv_std:.4f}")
        print(f"  Overhead:    {over_mean:.2f} ± {over_std:.2f}")
        print()

    print("✅ Analysis complete.")