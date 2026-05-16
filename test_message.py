import argparse
import hashlib
import os
import random
import threading
import time
from typing import Optional

import httpx

KME_URL = os.getenv("KME_URL", "http://localhost:8000")
ALICE_URL = os.getenv("ALICE_URL", "http://localhost:8001")
QKDL_URL = os.getenv("QKDL_URL", "http://localhost:8003")

POLL_INTERVAL = 1.0
SESSION_TIMEOUT = 180

G = "\033[92m"
R = "\033[91m"
Y = "\033[93m"
C = "\033[96m"
B = "\033[1m"
D = "\033[0m"


def start_session(
    receiver_label: str,
    n_qubits: int,
    batch_size: int,
    loss_rate: float,
):
    r = httpx.post(
        f"{ALICE_URL}/start",
        params={
            "receiver_label": receiver_label,
            "n_qubits": n_qubits,
            "batch_size": batch_size,
            "loss_rate": loss_rate,
        },
        timeout=30.0,
    )

    r.raise_for_status()
    return r.json()


def poll_session(session_id: str, timeout: float = SESSION_TIMEOUT):
    end = time.time() + timeout

    with httpx.Client(timeout=10.0) as c:
        while time.time() < end:
            try:
                r = c.get(f"{KME_URL}/sessions/{session_id}")
                r.raise_for_status()

                d = r.json()
                status = d.get("status", "")

                print(
                    f"\rstatus={status:<10} "
                    f"delivered={d.get('n_delivered',0):<5} "
                    f"sifted={d.get('n_sifted',0):<5}",
                    end=""
                )

                if status in ("done", "aborted"):
                    print()
                    return d

            except Exception:
                pass

            time.sleep(POLL_INTERVAL)

    return {
        "session_id": session_id,
        "status": "timeout",
        "error_message": f"timeout after {timeout}s",
    }

def consume_key(session_id: str):
    try:
        r = httpx.post(
            f"{KME_URL}/sessions/{session_id}/consume-key",
            timeout=10.0,
        )
        r.raise_for_status()
        return r.json()

    except Exception as e:
        print(f"{R}consume failed: {e}{D}")
        return None


def key_bits_from_string(key: str):
    return [int(x) for x in key] if key else []


def _bytes_to_bits(data: bytes):
    return [(b >> s) & 1 for b in data for s in range(7, -1, -1)]


def _bits_to_bytes(bits):
    out = bytearray()

    for i in range(0, len(bits), 8):
        chunk = bits[i:i + 8]
        chunk += [0] * (8 - len(chunk))

        out.append(
            sum(b << (7 - j) for j, b in enumerate(chunk))
        )

    return bytes(out)


def xor_encrypt(message: str, key_bits):
    if not key_bits:
        raise ValueError("empty key")

    msg_bits = _bytes_to_bits(message.encode())

    enc_bits = [
        msg_bits[i] ^ key_bits[i % len(key_bits)]
        for i in range(len(msg_bits))
    ]

    return _bits_to_bytes(enc_bits)


def xor_decrypt(ciphertext: bytes, key_bits):
    bits = _bytes_to_bits(ciphertext)

    dec_bits = [
        bits[i] ^ key_bits[i % len(key_bits)]
        for i in range(len(bits))
    ]

    return _bits_to_bytes(dec_bits).decode(
        "utf-8",
        errors="replace"
    )


class ClassicalChannel:

    def __init__(self, session_id: str):
        self.session_id = session_id
        self._inbox = []
        self._lock = threading.Lock()
        self._event = threading.Event()

    def send(self, ciphertext: bytes):

        t0 = time.perf_counter()

        ok = self._send_via_qkdl(ciphertext)

        if not ok:
            self._local_send(ciphertext)

        return time.perf_counter() - t0

    def _send_via_qkdl(self, ciphertext: bytes):

        try:
            r = httpx.post(
                f"{QKDL_URL}/classical/send",
                json={
                    "session_id": self.session_id,
                    "payload_hex": ciphertext.hex(),
                },
                timeout=10.0,
            )

            r.raise_for_status()

            rr = httpx.get(
                f"{QKDL_URL}/classical/recv/{self.session_id}",
                timeout=5.0,
            )

            rr.raise_for_status()

            data = rr.json()

            if data.get("available"):

                payload = bytes.fromhex(data["payload_hex"])

                with self._lock:
                    self._inbox.append(payload)

                self._event.set()

                return True

        except Exception:
            pass

        return False

    def _local_send(self, ciphertext: bytes):

        time.sleep(random.uniform(0.001, 0.005))

        with self._lock:
            self._inbox.append(ciphertext)

        self._event.set()

    def recv(self, timeout: float = 5.0):

        if self._event.wait(timeout):

            with self._lock:

                if self._inbox:

                    payload = self._inbox.pop(0)

                    if not self._inbox:
                        self._event.clear()

                    return payload

        return None



def exchange(
    message: str,
    key_bits,
    channel: ClassicalChannel,
    verbose=True,
):

    try:
        cipher = xor_encrypt(message, key_bits)

    except Exception as e:

        if verbose:
            print(f"{R}encryption failed: {e}{D}")

        return False

    t0 = time.perf_counter()

    channel.send(cipher)

    payload = channel.recv(timeout=10.0)

    if payload is None:

        if verbose:
            print(f"{R}timeout waiting message{D}")

        return False

    decrypted = xor_decrypt(payload, key_bits).strip("\x00")

    ok = decrypted == message

    if verbose:

        print(f"\n{'OK' if ok else 'FAIL'}")
        print(f"msg : {message}")
        print(f"enc : {cipher.hex()[:64]}")
        print(f"dec : {decrypted}")
        print(f"lat : {(time.perf_counter()-t0)*1000:.2f} ms")

    return ok


def run_messages(messages, key_bits, session_id, verbose=True):

    channel = ClassicalChannel(session_id)

    ok = 0
    total = 0

    for msg in messages:

        total += 1

        if exchange(msg, key_bits, channel, verbose):
            ok += 1

    return ok, total

def print_summary(session, key_bits, ok, total, duration):

    print(f"\n{B}===== BB84 REPORT ====={D}")

    print("session_id :", session.get("session_id"))
    print("status     :", session.get("status"))
    print("qber       :", round(session.get("qber", 0.0) * 100, 4), "%")
    print("delivered  :", session.get("n_delivered"))
    print("sifted     :", session.get("n_sifted"))
    print("key_bits   :", len(key_bits))
    print("messages   :", f"{ok}/{total}")
    print("duration   :", round(duration, 3), "s")

    if session.get("error_message"):
        print("error      :", session["error_message"])


def parse_args():

    p = argparse.ArgumentParser()

    p.add_argument("--receiver", type=str, default="bob-1")

    p.add_argument("--n-qubits", type=int, default=200)

    p.add_argument("--batch-size", type=int, default=10)

    p.add_argument("--loss-rate", type=float, default=0.0)

    p.add_argument(
        "--messages",
        nargs="+",
        default=[
            "Hello",
            "Distributed BB84",
            "Quantum Key Distribution",
        ],
    )

    p.add_argument("--no-kme", action="store_true")

    return p.parse_args()


def main():

    args = parse_args()

    print(f"\n{C}Starting BB84 test...{D}\n")

    if args.no_kme:

        fake_key = hashlib.sha256(b"local").hexdigest()

        session = {
            "session_id": "local",
            "status": "done",
            "qber": 0.0,
            "n_sifted": 128,
        }

        key_bits = key_bits_from_string(
            bin(int(fake_key, 16))[2:]
        )

    else:

        start_data = start_session(
            receiver_label=args.receiver,
            n_qubits=args.n_qubits,
            batch_size=args.batch_size,
            loss_rate=args.loss_rate,
        )

        session_id = start_data["session_id"]

        print(f"{G}session created:{D} {session_id}\n")

        session = poll_session(session_id)

        if session.get("status") != "done":

            print(f"{R}session failed{D}")
            print(session)

            return

        key_bits = key_bits_from_string(
            session.get("key_final", "")
        )

        consume_key(session_id)

    print(f"\n{G}key ready:{D} {len(key_bits)} bits\n")

    t0 = time.perf_counter()

    ok, total = run_messages(
        args.messages,
        key_bits,
        session["session_id"],
    )

    duration = time.perf_counter() - t0

    print_summary(
        session,
        key_bits,
        ok,
        total,
        duration,
    )


if __name__ == "__main__":
    main()