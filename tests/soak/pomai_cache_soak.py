#!/usr/bin/env python3
import argparse, random, socket, time, json

def cmd(*parts):
    out=[f"*{len(parts)}\r\n".encode()]
    for p in parts:
        b=str(p).encode()
        out.append(f"${len(b)}\r\n".encode()+b+b"\r\n")
    return b"".join(out)

def send(s,*parts):
    s.sendall(cmd(*parts))
    return s.recv(4096)

ap=argparse.ArgumentParser()
ap.add_argument('--port',type=int,default=6379)
ap.add_argument('--duration',type=int,default=180)
args=ap.parse_args()

s=socket.create_connection(('127.0.0.1',args.port))
start=time.time(); ops=0; hits=0
samples=[]
while time.time()-start<args.duration:
    k=f"k{random.randint(0,999)}"
    if ops%10==0:
        send(s,'CONFIG','SET','POLICY.CANARY_PCT',str((ops//100)%20))
    if random.random()<0.35:
        send(s,'SET',k,'x'*64,'EX','30')
    else:
        t=time.perf_counter_ns(); r=send(s,'GET',k); d=(time.perf_counter_ns()-t)/1000
        samples.append(d)
        if not r.startswith(b'$-1'): hits+=1
    ops+=1

p95=sorted(samples)[int(len(samples)*0.95)] if samples else 0
summary={'ops':ops,'duration_s':args.duration,'ops_per_s':ops/max(1,args.duration),'hit_rate':hits/max(1,len(samples)),'p95_us':p95}
print(json.dumps(summary,indent=2))
