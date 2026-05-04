import argparse
import hashlib
import os
import sys
import time
import threading
import random
from typing import Optional

import httpx

ORCH_URL=os.getenv("ORCH_URL","http://localhost:8000")
POLL_INTERVAL=1.0
SESSION_TIMEOUT=120

G="\033[92m"; R="\033[91m"; Y="\033[93m"
C="\033[96m"; B="\033[1m"; D="\033[0m"


# start bb84 session
def start_session(n_qubits:int,batch_size:int,loss_rate:float)->str:
    r=httpx.post(
        f"{ORCH_URL}/session/start",
        json={"n_qubits":n_qubits,"batch_size":batch_size,"loss_rate":loss_rate},
        timeout=30.0,
    )
    r.raise_for_status()
    return r.json()["session_id"]


# wait session done
def poll_session(session_id:str,timeout:float=SESSION_TIMEOUT)->dict:
    end=time.time()+timeout

    with httpx.Client(timeout=10.0) as c:
        while time.time()<end:
            try:
                d=c.get(f"{ORCH_URL}/session/{session_id}").json()
                if d.get("status","") in ("done","aborted"):
                    print(" "*60,end="\r")
                    return d
            except httpx.HTTPError:
                pass
            time.sleep(POLL_INTERVAL)

    return {"status":"timeout","error_message":f"timeout {timeout}s"}


# hash -> bits
def key_bits_from_hash(key_hash:str,n_bits:int)->list[int]:
    if not key_hash:
        return []

    ext=key_hash
    while len(ext)<n_bits//4+64:
        ext+=hashlib.sha256(ext.encode()).hexdigest()

    bits=[]
    for ch in ext:
        v=int(ch,16)
        for s in range(3,-1,-1):
            bits.append((v>>s)&1)
        if len(bits)>=n_bits:
            break

    return bits[:n_bits]


# bytes <-> bits
def _bytes_to_bits(data:bytes)->list[int]:
    return [(b>>s)&1 for b in data for s in range(7,-1,-1)]


def _bits_to_bytes(bits:list[int])->bytes:
    out=bytearray()
    for i in range(0,len(bits),8):
        c=bits[i:i+8]+[0]*(8-len(bits[i:i+8]))
        out.append(sum(b<<(7-j) for j,b in enumerate(c)))
    return bytes(out)


# xor enc
def xor_encrypt(message:str,key_bits:list[int])->bytes:
    if not key_bits:
        raise ValueError("no key")

    m=_bytes_to_bits(message.encode())
    return _bits_to_bytes([
        m[i]^key_bits[i%len(key_bits)]
        for i in range(len(m))
    ])


# xor dec
def xor_decrypt(data:bytes,key_bits:list[int])->str:
    b=_bytes_to_bits(data)
    return _bits_to_bytes([
        b[i]^key_bits[i%len(key_bits)]
        for i in range(len(b))
    ]).decode("utf-8",errors="replace")


QNS_URL=os.getenv("QNS_URL","http://localhost:8003")


# simple classical channel
class ClassicalChannel:
    def __init__(self,session_id:str,mode:str="direct"):
        self.session_id=session_id
        self.mode=mode
        self._inbox=[]
        self._lock=threading.Lock()
        self._received=threading.Event()

    def send(self,ciphertext:bytes,sender:str="Alice",receiver:str="Bob")->float:
        t0=time.perf_counter()

        if self.session_id and self.session_id!="local-simulation":
            ok=self._send_via_qns(ciphertext)
        else:
            ok=False

        if not ok:
            self._local_send(ciphertext)

        return time.perf_counter()-t0

    def _send_via_qns(self,ciphertext:bytes)->bool:
        try:
            r=httpx.post(
                f"{QNS_URL}/classical/send",
                json={
                    "session_id":self.session_id,
                    "payload_hex":ciphertext.hex(),
                    "mode":self.mode,
                },
                timeout=15.0,
            )
            r.raise_for_status()

            if not r.json().get("delivered"):
                return False

            rr=httpx.get(f"{QNS_URL}/classical/recv/{self.session_id}",timeout=5.0)
            rr.raise_for_status()
            d=rr.json()

            if d.get("available"):
                p=bytes.fromhex(d["payload_hex"])
                with self._lock:
                    self._inbox.append(p)
                self._received.set()
                return True

        except Exception:
            pass

        return False

    def _local_send(self,ciphertext:bytes)->None:
        time.sleep(random.uniform(0.0005,0.002))
        with self._lock:
            self._inbox.append(ciphertext)
        self._received.set()

    def recv(self,timeout:float=5.0)->Optional[bytes]:
        if self._received.wait(timeout=timeout):
            with self._lock:
                if self._inbox:
                    x=self._inbox.pop(0)
                    if not self._inbox:
                        self._received.clear()
                    return x
        return None


# one exchange
def exchange(message:str,key_bits:list[int],channel:ClassicalChannel,verbose:bool=True)->bool:
    need=len(message.encode())*8

    if verbose and len(key_bits)<need:
        print(f"{Y}key small, cycling{D}")

    t0=time.perf_counter()

    try:
        cipher=xor_encrypt(message,key_bits)
    except ValueError as e:
        if verbose:
            print(f"{R}enc fail {e}{D}")
        return False

    channel.send(cipher)
    payload=channel.recv(timeout=10.0)

    if payload is None:
        if verbose:
            print(f"{R}timeout msg{D}")
        return False

    dec=xor_decrypt(payload,key_bits).strip("\x00")
    ok=dec==message

    if verbose:
        print(f"\n{'OK' if ok else 'FAIL'} {message}")
        print(f"enc {cipher.hex()[:32]}")
        print(f"dec {dec}")
        print(f"lat {(time.perf_counter()-t0)*1000:.1f}ms")

    return ok


def run_messages(messages:list[str],key_bits:list[int],
                 session_id:str="local-simulation",
                 modes=("direct",),verbose:bool=True):

    ok=total=0

    for mode in modes:
        ch=ClassicalChannel(mode=mode,session_id=session_id)

        if verbose:
            print(f"\nmode {mode}")

        for m in messages:
            total+=1
            if exchange(m,key_bits,ch,verbose):
                ok+=1

    return ok,total


def print_summary(session:dict,key_bits:list[int],ok:int,total:int,duration:float):

    print("\n===== BB84 REPORT =====")
    print("session:",session.get("session_id","local"))
    print("qber:",session.get("qber",0)*100,"%")
    print("key:",len(key_bits))
    print("ok:",ok,"/",total)
    print("time:",round(duration,3),"s")


def parse_args():
    p=argparse.ArgumentParser()
    p.add_argument("--n-qubits",type=int,default=200)
    p.add_argument("--batch-size",type=int,default=10)
    p.add_argument("--loss-rate",type=float,default=0.0)
    p.add_argument("--messages",nargs="+",default=["Hello","BB84"])
    p.add_argument("--demo",action="store_true")
    p.add_argument("--no-orchestrator",action="store_true")
    return p.parse_args()

#after consuming key, it triggers consume of orch
def consume_key(session_id: str):
    try:
        r = httpx.post(f"{ORCH_URL}/session/{session_id}/consume-key", timeout=10.0)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"{R}consume failed: {e}{D}")
        return False

def main():
    args=parse_args()

    print("\nBB84 test start")

    if args.no_orchestrator: #cas local in case echec de l'orch service
        fake=hashlib.sha256(b"local").hexdigest()
        session={"session_id":"local","qber":0.0,"n_sifted":100}
        key_bits=key_bits_from_hash(fake,100)
    else:
        sid=start_session(args.n_qubits,args.batch_size,args.loss_rate)
        session=poll_session(sid)
        key_bits=key_bits_from_hash(session.get("key_final",""),session.get("n_sifted",0))
        if not args.no_orchestrator and session.get("session_id"):
            consume_key(session["session_id"])
            
    print("session ready, key:",len(key_bits))

    t0=time.perf_counter()

    ok,total=run_messages(args.messages,key_bits)

    print_summary(session,key_bits,ok,total,time.perf_counter()-t0)


if __name__=="__main__":
    main()