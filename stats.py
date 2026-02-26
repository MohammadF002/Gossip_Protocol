import os
import time
import json
import glob
import threading


class StatsCollector:
    """
    جمع‌آورنده‌ی آمار که بر اساس لاگ تمام نودها
    (فایل‌های داخل پوشه‌ی logs/) معیارها را حساب می‌کند.

    ایده این است:
      - هر نود رویدادهای ORIGIN / RECV / SENT را در لاگ خودش می‌نویسد.
      - وقتی روی یک نود /report می‌زنیم، این کلاس تمام لاگ‌ها را می‌خواند
        و برای «آخرین پیام معمولی» معیارها را حساب می‌کند.
    """

    def __init__(self, node_id, mode):
        self.node_id = node_id
        self.mode = mode
        self.lock = threading.Lock()
        # آخرین msg_id که این نود دیده (برای حدس زدن پیام هدف)
        self.current_msg = None

    # این دو متد برای سازگاری با node.py هستند؛
    # فقط current_msg را به‌روز می‌کنیم تا بدانیم آخرین پیام چه بوده.
    def record_send(self):
        return

    def record_receive(self, msg_id):
        with self.lock:
            self.current_msg = msg_id

    # ----------------- تحلیل لاگ‌ها -----------------

    def _iter_log_files(self):
        log_dir = os.environ.get("LOG_DIR", "logs")
        pattern = os.path.join(log_dir, "log_*.txt")
        return sorted(glob.glob(pattern))

    def _parse_logs_for_msg(self, target_msg_id=None):
        """
        اگر target_msg_id None باشد، آخرین msg_id از روی ORIGIN ها انتخاب می‌شود.
        خروجی:
          dict با فیلدهای:
            msg_id, origin_time, recv_times: dict[file -> t], sent_count, total_nodes
        """
        log_files = self._iter_log_files()
        if not log_files:
            return None

        # مرحله ۱: اگر msg_id نداریم، از آخرین ORIGIN استفاده کن
        chosen_msg_id = target_msg_id
        if chosen_msg_id is None:
            last_origin_time = -1.0
            last_origin_id = None
            for path in log_files:
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        for line in f:
                            if not line.startswith("ORIGIN "):
                                continue
                            parts = line.strip().split()
                            if len(parts) != 3:
                                continue
                            _, mid, t_str = parts
                            try:
                                t_val = float(t_str)
                            except ValueError:
                                continue
                            if t_val >= last_origin_time:
                                last_origin_time = t_val
                                last_origin_id = mid
                except OSError:
                    continue
            chosen_msg_id = last_origin_id

        if not chosen_msg_id:
            return None

        # مرحله ۲: برای همان msg_id، تمام ORIGIN/RECV/SENT ها را استخراج کن
        origin_time = None
        recv_times = {}    # file -> t_first_recv
        sent_count = 0

        for path in log_files:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("ORIGIN "):
                            parts = line.strip().split()
                            if len(parts) != 3:
                                continue
                            _, mid, t_str = parts
                            if mid != chosen_msg_id:
                                continue
                            try:
                                t_val = float(t_str)
                            except ValueError:
                                continue
                            if origin_time is None or t_val < origin_time:
                                origin_time = t_val

                        elif line.startswith("RECV "):
                            parts = line.strip().split()
                            if len(parts) != 3:
                                continue
                            _, mid, t_str = parts
                            if mid != chosen_msg_id:
                                continue
                            try:
                                t_val = float(t_str)
                            except ValueError:
                                continue
                            # برای هر لاگ فقط اولین دریافت را می‌خواهیم
                            if path not in recv_times or t_val < recv_times[path]:
                                recv_times[path] = t_val

                        elif line.startswith("SENT "):
                            parts = line.strip().split()
                            if len(parts) != 3:
                                continue
                            _, mid, _ = parts
                            if mid == chosen_msg_id:
                                sent_count += 1
            except OSError:
                continue

        total_nodes = len(log_files)

        if origin_time is None:
            # پیام هدف در لاگ‌ها پیدا نشده
            return None

        return {
            "msg_id": chosen_msg_id,
            "origin_time": origin_time,
            "recv_times": recv_times,
            "sent_count": sent_count,
            "total_nodes": total_nodes,
        }

    def generate_report(self, total_nodes_override=None):
        with self.lock:
            info = self._parse_logs_for_msg(self.current_msg)
            if not info:
                return None

            msg_id = info["msg_id"]
            origin_time = info["origin_time"]
            recv_times = info["recv_times"]
            sent_count = info["sent_count"]
            total_nodes = info["total_nodes"]

            if total_nodes_override is not None:
                total_nodes = total_nodes_override

            nodes_reached = len(recv_times)
            delivery_ratio = nodes_reached / total_nodes if total_nodes > 0 else 0.0

            if recv_times:
                last_recv_time = max(recv_times.values())
                delay = last_recv_time - origin_time
            else:
                delay = 0.0

            return {
                "mode": self.mode,
                "msg_id": msg_id,
                "delivery_ratio": round(delivery_ratio, 4),
                "propagation_delay_sec": round(delay, 4),
                "total_messages_sent_for_msg": sent_count,
                "nodes_reached": nodes_reached,
                "total_nodes": total_nodes,
            }

    def save_report(self, total_nodes_override=None):
        report = self.generate_report(total_nodes_override)
        if not report:
            print("No data found in logs to build report.")
            return

        with open("results.json", "a", encoding="utf-8") as f:
            f.write(json.dumps(report) + "\n")

        print("\n=== TEST REPORT (from logs) ===")
        for k, v in report.items():
            print(f"{k}: {v}")
        print("================================\n")