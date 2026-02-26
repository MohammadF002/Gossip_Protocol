import hashlib
import time
import uuid

def find_pow(node_id, difficulty):
    prefix = "0" * difficulty
    nonce = 0
    start = time.time()

    while True:
        data = f"{node_id}{nonce}".encode()
        digest = hashlib.sha256(data).hexdigest()
        if digest.startswith(prefix):
            end = time.time()
            return nonce, digest, end - start
        nonce += 1

node_id = str(uuid.uuid4())

for k in [2,3,4,5]:
    nonce, digest, duration = find_pow(node_id, k)
    print(f"k={k} time={duration:.3f}s nonce={nonce}")