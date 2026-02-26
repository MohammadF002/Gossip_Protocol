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
from typing import Dict


# ==============================
# CONFIG
# ==============================

class Config:
    def __init__(self, args):
        self.fanout = args.fanout
        self.ttl = args.ttl
        self.peer_limit = args.peer_limit
        self.ping_interval = args.ping_interval
        self.peer_timeout = args.peer_timeout
        random.seed(args.seed)


# ==============================
# PEER
# ==============================

class Peer:
    def __init__(self, peer_id: str, addr: str):
        self.peer_id = peer_id
        self.addr = addr
        self.last_seen = time.time()


# ==============================
# NODE
# ==============================

class Node:

    # --------------------------
    # INIT
    # --------------------------

    def __init__(self, args):

        self.node_id = str(uuid.uuid4())
        self.addr = f"127.0.0.1:{args.port}"
        self.config = Config(args)

        self.peers: Dict[str, Peer] = {}
        self.seen = set()

        self.peer_lock = threading.Lock()
        self.log_lock = threading.Lock()

        # ---------- Logging ----------
        log_dir = os.environ.get("LOG_DIR", "logs")
        os.makedirs(log_dir, exist_ok=True)

        self.log_file = open(
            f"{log_dir}/log_{self.node_id}.txt",
            "w",
            buffering=1
        )
        # ---------- Socket ----------
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", args.port))

        print(f"[START] {self.node_id} @ {self.addr}")

        # ---------- Threads ----------
        threading.Thread(target=self.listen_loop, daemon=True).start()
        threading.Thread(target=self.ping_loop, daemon=True).start()
        threading.Thread(target=self.user_input_loop, daemon=True).start()

        # ---------- Bootstrap ----------
        if args.bootstrap:
            time.sleep(1)
            self.bootstrap(args.bootstrap)

    # ==============================
    # LOGGING
    # ==============================

    def log(self, line: str):
        with self.log_lock:
            self.log_file.write(line + "\n")

    # ==============================
    # MESSAGE CREATION
    # ==============================

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

    # ==============================
    # NETWORK SEND
    # ==============================

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

    # ==============================
    # LISTEN LOOP
    # ==============================

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

                if sender_id and sender_id != self.node_id:
                    self.update_peer(sender_id, sender_addr)

                self.dispatch(msg)

            except Exception as e:
                # This means YOUR CODE has a bug
                self.log(f"ERROR DISPATCH {traceback.format_exc()}")

    # ==============================
    # DISPATCH
    # ==============================

    def dispatch(self, msg):

        msg_type = msg.get("msg_type")

        if msg_type == "HELLO":
            pass

        elif msg_type == "GET_PEERS":
            self.handle_get_peers(msg)

        elif msg_type == "PEERS_LIST":
            self.handle_peers_list(msg)

        elif msg_type == "PING":
            self.handle_ping(msg)

        elif msg_type == "PONG":
            pass

        elif msg_type == "GOSSIP":
            self.handle_gossip(msg)

    # ==============================
    # PEER MANAGEMENT
    # ==============================

    def update_peer(self, peer_id, addr):

        with self.peer_lock:
            if peer_id in self.peers:
                self.peers[peer_id].last_seen = time.time()
                return

            if len(self.peers) >= self.config.peer_limit:
                oldest = min(
                    self.peers.values(),
                    key=lambda p: p.last_seen
                )
                del self.peers[oldest.peer_id]

            self.peers[peer_id] = Peer(peer_id, addr)

    def bootstrap(self, bootstrap_addr):

        hello = self.create_message("HELLO", {})
        self.send(hello, bootstrap_addr)

        get_peers = self.create_message("GET_PEERS", {})
        self.send(get_peers, bootstrap_addr)

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
            if p["id"] != self.node_id:
                self.update_peer(p["id"], p["addr"])

    def handle_ping(self, msg):

        pong = self.create_message("PONG", {})
        self.send(pong, msg["sender_addr"])

    # ==============================
    # GOSSIP
    # ==============================

    def handle_gossip(self, msg):

        msg_id = msg["msg_id"]

        if msg_id in self.seen:
            return

        self.seen.add(msg_id)

        recv_time = time.time()
        self.log(f"RECV {msg_id} {recv_time}")

        ttl = msg["ttl"] - 1
        if ttl <= 0:
            return

        new_msg = msg.copy()
        new_msg["ttl"] = ttl
        new_msg["sender_id"] = self.node_id
        new_msg["sender_addr"] = self.addr

        self.forward(new_msg)

    def forward(self, msg):
        with self.peer_lock:
            peers = list(self.peers.values())

        if not peers:
            return

        k = min(len(peers), self.config.fanout)
        selected = random.sample(peers, k)

        for p in selected:
            self.send(msg, p.addr)

    # ==============================
    # PERIODIC LOOPS
    # ==============================

    def ping_loop(self):

        while True:
            time.sleep(self.config.ping_interval)

            now = time.time()

            with self.peer_lock:
                for pid in list(self.peers.keys()):
                    peer = self.peers[pid]

                    if now - peer.last_seen > self.config.peer_timeout:
                        del self.peers[pid]
                    else:
                        ping = self.create_message("PING", {})
                        self.send(ping, peer.addr)

    def user_input_loop(self):

        while True:
            line = sys.stdin.readline().strip()
            if not line:
                continue
            msg = self.create_message("GOSSIP", {"data": line})
            self.seen.add(msg["msg_id"])
            origin_time = time.time()
            self.log(f"ORIGIN {msg['msg_id']} {origin_time}")
            self.forward(msg)


# ==============================
# MAIN
# ==============================

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--bootstrap", type=str)
    parser.add_argument("--fanout", type=int, default=3)
    parser.add_argument("--ttl", type=int, default=8)
    parser.add_argument("--peer-limit", type=int, default=20)
    parser.add_argument("--ping-interval", type=int, default=2)
    parser.add_argument("--peer-timeout", type=int, default=6)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    Node(args)

    while True:
        time.sleep(1)