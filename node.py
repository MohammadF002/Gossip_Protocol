import socket
import json
import uuid
import argparse
import threading
import time
import random
import sys
import os
import traceback
import hashlib
from typing import Dict
from stats import StatsCollector

class Config:
    def __init__(self, args):
        self.fanout = args.fanout
        self.ttl = args.ttl
        self.peer_limit = args.peer_limit
        self.ping_interval = args.ping_interval
        self.peer_timeout = args.peer_timeout
        self.pull_interval = args.pull_interval
        self.ihave_max_ids = args.ihave_max_ids
        self.pow_k = args.pow_k
        random.seed(args.seed)


class Peer:
    def __init__(self, peer_id: str, addr: str):
        self.peer_id = peer_id
        self.addr = addr
        self.last_seen = time.time()
        self.missed_pings = 0


class Node:

    def __init__(self, args):
        self.node_id = str(uuid.uuid4())
        self.addr = f"127.0.0.1:{args.port}"
        self.config = Config(args)

        self.peers: Dict[str, Peer] = {}
        self.seen = set()
        self.msg_store: Dict[str, dict] = {}

        self.peer_lock = threading.Lock()
        self.log_lock = threading.Lock()

        log_dir = os.environ.get("LOG_DIR", "logs")
        os.makedirs(log_dir, exist_ok=True)

        self.log_file = open(
            f"{log_dir}/log_{self.node_id}.txt",
            "w",
            buffering=1
        )
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", args.port))

        print(f"[START] {self.node_id} @ {self.addr}")

        self.stats = StatsCollector(
            self.node_id,
            "hybrid" if self.config.pull_interval != 0 else "push"
        )

        self.pow_info = self._compute_pow(self.config.pow_k) if self.config.pow_k > 0 else None

        threading.Thread(target=self.listen_loop, daemon=True).start()
        threading.Thread(target=self.ping_loop, daemon=True).start()
        threading.Thread(target=self.user_input_loop, daemon=True).start()
        if self.config.pull_interval > 0:
            threading.Thread(target=self.pull_loop, daemon=True).start()

        if args.bootstrap:
            time.sleep(1)
            self.bootstrap(args.bootstrap)



    def log(self, line: str):
        with self.log_lock:
            self.log_file.write(line + "\n")

    def _compute_pow(self, k: int):

        target_prefix = "0" * k
        nonce = 0
        node_bytes = self.node_id.encode("utf-8")
        start = time.time()

        while True:
            h = hashlib.sha256(node_bytes + str(nonce).encode("utf-8")).hexdigest()
            if h.startswith(target_prefix):
                duration = time.time() - start
                self.log(f"POW_FOUND k={k} nonce={nonce} time_s={duration:.3f} digest={h}")
                return {
                    "hash_alg": "sha256",
                    "difficulty_k": k,
                    "nonce": nonce,
                    "digest_hex": h,
                }
            nonce += 1

    def create_message(self, msg_type, payload, ttl=None):

        if ttl is None:
            ttl = self.config.ttl

        return {
            "version": 1,
            "msg_id": str(uuid.uuid4()),
            "msg_type": msg_type,
            "sender_id": self.node_id,
            "sender_addr": self.addr,
            "timestamp": time.time(),
            "ttl": ttl,
            "payload": payload
        }

    def send(self, msg, addr):

        try:
            if not addr or ":" not in addr:
                raise ValueError(f"Invalid address format: {addr}")

            ip, port = addr.split(":", 1)
            port = int(port)

        except Exception as e:
            self.log(f"ERROR INVALID_ADDRESS addr={addr} error={e}")
            return

        try:
            data = json.dumps(msg).encode()

        except Exception as e:
            self.log(f"ERROR JSON_ENCODE msg_id={msg.get('msg_id')} error={e}")
            return

        try:
            self.sock.sendto(data, (ip, port))
            self.stats.record_send()
            self.log(f"SENT {msg.get('msg_id')} {time.time()}")

        except OSError as e:
            self.log(
                f"ERROR SOCKET_SEND "
                f"msg_id={msg.get('msg_id')} "
                f"to={addr} "
                f"error={e}"
            )

        except Exception as e:
            self.log(
                f"ERROR UNKNOWN_SEND "
                f"msg_id={msg.get('msg_id')} "
                f"to={addr} "
                f"error={e}"
            )

    def listen_loop(self):

        while True:
            try:
                data, addr = self.sock.recvfrom(65536)

            except OSError as e:
                self.log(f"ERROR SOCKET_RECV {e}")
                continue

            try:
                msg = json.loads(data.decode())

            except json.JSONDecodeError as e:
                self.log(f"ERROR INVALID_JSON from {addr} {e}")
                continue

            except UnicodeDecodeError as e:
                self.log(f"ERROR INVALID_ENCODING from {addr} {e}")
                continue

            try:
                sender_id = msg.get("sender_id")
                sender_addr = msg.get("sender_addr")
                msg_type = msg.get("msg_type")

                if sender_id and sender_id != self.node_id and msg_type != "HELLO":
                    self.update_peer(sender_id, sender_addr)

                self.dispatch(msg)

            except Exception as e:
                self.log(f"ERROR DISPATCH {traceback.format_exc()}")

    def dispatch(self, msg):

        msg_type = msg.get("msg_type")

        if msg_type == "HELLO":
            self.handle_hello(msg)

        elif msg_type == "GET_PEERS":
            self.handle_get_peers(msg)

        elif msg_type == "PEERS_LIST":
            self.handle_peers_list(msg)

        elif msg_type == "PING":
            self.handle_ping(msg)

        elif msg_type == "PONG":
            # liveness already updated in listen_loop
            pass

        elif msg_type == "GOSSIP":
            self.handle_gossip(msg)

        elif msg_type == "IHAVE":
            self.handle_ihave(msg)

        elif msg_type == "IWANT":
            self.handle_iwant(msg)

    def update_peer(self, peer_id, addr):

        with self.peer_lock:
            if peer_id in self.peers:
                peer = self.peers[peer_id]
                peer.last_seen = time.time()
                peer.missed_pings = 0
                return

            if len(self.peers) >= self.config.peer_limit and self.peers:
                worst = max(
                    self.peers.values(),
                    key=lambda p: (p.missed_pings, -p.last_seen)
                )
                self.log(f"PEER_EVICT {worst.peer_id}")
                del self.peers[worst.peer_id]

            self.peers[peer_id] = Peer(peer_id, addr)
            print(f"[INFO] Added peer: node_id={peer_id}, addr={addr}")
            self.log(f"PEER_ADD {peer_id} {addr}")

    def bootstrap(self, bootstrap_addr):

        hello_payload = {
            "capabilities": ["udp", "json"],
        }
        if self.pow_info is not None:
            hello_payload["pow"] = self.pow_info

        hello = self.create_message("HELLO", hello_payload)
        self.send(hello, bootstrap_addr)

        get_peers = self.create_message("GET_PEERS", {})
        self.send(get_peers, bootstrap_addr)

    def handle_hello(self, msg):

        sender_id = msg.get("sender_id")
        sender_addr = msg.get("sender_addr")
        payload = msg.get("payload", {})
        pow_info = payload.get("pow")

        if self.config.pow_k <= 0:
            if sender_id and sender_id != self.node_id:
                self.update_peer(sender_id, sender_addr)
            return

        if not pow_info:
            self.log(f"HELLO_REJECT sender={sender_id} reason=missing_pow")
            return

        try:
            alg = pow_info.get("hash_alg")
            k_recv = int(pow_info.get("difficulty_k"))
            nonce = int(pow_info.get("nonce"))
            digest_hex = pow_info.get("digest_hex", "")
        except Exception:
            self.log(f"HELLO_REJECT sender={sender_id} reason=invalid_pow_fields")
            return

        if alg != "sha256" or k_recv != self.config.pow_k:
            self.log(f"HELLO_REJECT sender={sender_id} reason=alg_or_k_mismatch")
            return

        recomputed = hashlib.sha256(
            str(sender_id).encode("utf-8") + str(nonce).encode("utf-8")
        ).hexdigest()

        if recomputed != digest_hex or not digest_hex.startswith("0" * self.config.pow_k):
            self.log(f"HELLO_REJECT sender={sender_id} reason=bad_digest")
            return

        # PoW is valid: accept peer.
        if sender_id and sender_id != self.node_id:
            self.update_peer(sender_id, sender_addr)
            self.log(f"HELLO_ACCEPT sender={sender_id} addr={sender_addr}")

    def handle_get_peers(self, msg):

        peers_payload = [
            {"id": p.peer_id, "addr": p.addr}
            for p in self.peers.values()
        ]

        reply = self.create_message(
            "PEERS_LIST",
            {"peers": peers_payload}
        )

        self.send(reply, msg["sender_addr"])

    def handle_peers_list(self, msg):

        peers = msg["payload"].get("peers", [])

        for p in peers:
            if p["id"] == self.node_id:
                continue

            is_new = p["id"] not in self.peers
            self.update_peer(p["id"], p["addr"])

            if is_new:
                hello_payload = {
                    "capabilities": ["udp", "json"],
                }
                if self.pow_info is not None:
                    hello_payload["pow"] = self.pow_info
                hello = self.create_message("HELLO", hello_payload)
                self.send(hello, p["addr"])

    def handle_ping(self, msg):

        pong = self.create_message("PONG", {})
        self.send(pong, msg["sender_addr"])


    def handle_gossip(self, msg):

        msg_id = msg["msg_id"]
        sender_id = msg["sender_id"]
        sender_addr = msg["sender_addr"]
        message_content = msg["payload"]["data"]

        if msg_id in self.seen:
            return

        self.seen.add(msg_id)
        self.msg_store[msg_id] = msg

        recv_time = time.time()
        self.log(f"RECV {msg_id} {recv_time}")
        print (f"[INFO] Received a new gossip: "
               f"\n\tcontent={message_content}"
               f"\n\tmessage_id={msg_id}"
               f"\n\tsender_id={sender_id}"
               f"\n\tsender_addr={sender_addr}"
               f"at {time.time()}")
        ttl = msg["ttl"] - 1
        if ttl <= 0:
            return

        new_msg = msg.copy()
        new_msg["ttl"] = ttl
        new_msg["sender_id"] = self.node_id
        new_msg["sender_addr"] = self.addr
        self.stats.record_receive(msg["msg_id"])
        self.forward(new_msg, exclude_id=sender_id)

    def forward(self, msg, exclude_id=None):
        with self.peer_lock:
            peers = [
                p for pid, p in self.peers.items()
                if pid != exclude_id
            ]

        if not peers:
            return

        k = min(len(peers), self.config.fanout)
        selected = random.sample(peers, k)

        for p in selected:
            self.send(msg, p.addr)
            # print(f"[INFO] Forwarding message to a peer: "
            #       f"\n\tcontent={msg['payload']['data']}"
            #       f"\n\tdestination={p.addr}")


    def ping_loop(self):

        while True:
            time.sleep(self.config.ping_interval)

            now = time.time()

            with self.peer_lock:
                for pid in list(self.peers.keys()):
                    peer = self.peers[pid]

                    if now - peer.last_seen > self.config.peer_timeout:
                        print(f"[WARN] Peer removed due to timeout"
                              f"\n\tid={peer.peer_id}"
                              f"\n\taddr={peer.addr}")
                        self.log(f"PEER_TIMEOUT {peer.peer_id}")
                        del self.peers[pid]
                    else:
                        peer.missed_pings += 1
                        ping = self.create_message("PING", {})
                        self.send(ping, peer.addr)

    def pull_loop(self):

        while True:
            time.sleep(self.config.pull_interval)

            if not self.seen:
                continue

            with self.peer_lock:
                peers = list(self.peers.values())

            if not peers:
                continue

            ids_list = list(self.seen)
            max_ids = min(len(ids_list), self.config.ihave_max_ids)
            if max_ids <= 0:
                continue

            advertised_ids = random.sample(ids_list, max_ids)

            ihave_payload = {
                "ids": advertised_ids,
                "max_ids": self.config.ihave_max_ids,
            }
            ihave_msg = self.create_message("IHAVE", ihave_payload)

            k = min(len(peers), self.config.fanout)
            selected = random.sample(peers, k)
            for p in selected:
                self.send(ihave_msg, p.addr)

    def handle_ihave(self, msg):

        payload = msg.get("payload", {})
        ids = payload.get("ids", [])
        sender_addr = msg.get("sender_addr")

        missing = [mid for mid in ids if mid not in self.seen]
        if not missing:
            return

        iwant_payload = {"ids": missing}
        iwant_msg = self.create_message("IWANT", iwant_payload)
        self.send(iwant_msg, sender_addr)

    def handle_iwant(self, msg):

        payload = msg.get("payload", {})
        ids = payload.get("ids", [])
        sender_addr = msg.get("sender_addr")

        for mid in ids:
            original = self.msg_store.get(mid)
            if not original:
                continue
            self.send(original, sender_addr)

    def user_input_loop(self):

        while True:
            line = sys.stdin.readline().strip()
            if not line:
                continue
            if line == "/report":
                self.stats.save_report(total_nodes_override=10)
                continue
            msg = self.create_message("GOSSIP", {"data": line})
            self.seen.add(msg["msg_id"])
            self.msg_store[msg["msg_id"]] = msg
            self.stats.record_receive(msg["msg_id"])
            origin_time = time.time()
            self.log(f"ORIGIN {msg['msg_id']} {origin_time}")
            self.forward(msg)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--bootstrap", type=str)
    parser.add_argument("--fanout", type=int, default=3)
    parser.add_argument("--ttl", type=int, default=6)
    parser.add_argument("--peer-limit", type=int, default=20)
    parser.add_argument("--ping-interval", type=int, default=5)
    parser.add_argument("--peer-timeout", type=int, default=30)
    parser.add_argument("--pull-interval", type=int, default=0,
                        help="Interval (seconds) for Hybrid IHAVE/IWANT; 0 disables hybrid pull.")
    parser.add_argument("--ihave-max-ids", type=int, default=32,
                        help="Maximum number of msg_ids included in each IHAVE.")
    parser.add_argument("--pow-k", type=int, default=4,
                        help="Proof-of-Work difficulty (leading zero hex digits) for HELLO.")
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    Node(args)

    while True:
        time.sleep(1)