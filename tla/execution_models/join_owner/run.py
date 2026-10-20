#!/usr/bin/env python3
"""Reproduce JOIN L0 with isolated TLC scratch and final action coverage."""
import json, os, pathlib, re, subprocess, tempfile, time
repo=pathlib.Path(__file__).resolve().parents[3]
level=pathlib.Path(__file__).resolve().parent/"L0_pending_activation"
models=level/"02_models"
evidence=level/"04_evidence"
evidence.mkdir(exist_ok=True)
results=[]
for cfg in sorted(models.glob("*.cfg")):
    if os.environ.get("TICKWIRE_JOIN_CFG") and cfg.stem!=os.environ["TICKWIRE_JOIN_CFG"]: continue
    with tempfile.TemporaryDirectory(prefix="tickwire-join-tlc-") as scratch:
        java=pathlib.Path(scratch)/"java"; java.mkdir()
        command=["java",f"-Djava.io.tmpdir={java}","-XX:+UseParallelGC","-Xmx2g","-cp",str(repo/"tla/tools/tla2tools.jar"),"tlc2.TLC","-workers","1","-coverage","1","-metadir",str(pathlib.Path(scratch)/"states"),"-config",cfg.name,"L0_JoinOwner.tla"]
        started=time.monotonic()
        with (evidence/(cfg.stem+".log")).open("w") as log:
            log.write("COMMAND: "+" ".join(command)+"\n"); log.flush()
            result=subprocess.run(command,cwd=models,stdout=log,stderr=subprocess.STDOUT)
        seconds=round(time.monotonic()-started,3)
    raw=(evidence/(cfg.stem+".log")).read_text()
    matches=re.findall(r"(\d+) states generated, (\d+) distinct states found, (\d+) states left on queue",raw)
    coverage=re.findall(r"^<([^>]+)>: (\d+):(\d+)$",raw,flags=re.M)
    # TLC prints intermediate coverage too; only the final block is the evidence.
    final=raw[raw.rfind("The coverage statistics at"):]
    coverage=re.findall(r"^<([^>]+)>: (\d+):(\d+)$",final,flags=re.M)
    row={"config":cfg.name,"returncode":result.returncode,"seconds":seconds,"counts":matches[-1] if matches else [],"depth":re.findall(r"depth of the complete state graph search is (\d+)",raw),"actions":[{"action":a,"distinct":int(d),"generated":int(g)} for a,d,g in coverage]}
    results.append(row)
    (evidence/"results-current.json").write_text(json.dumps(results,indent=2)+"\n")
    print(json.dumps(row),flush=True)
    if result.returncode or "Model checking completed. No error has been found." not in raw: raise SystemExit(1)
    if not coverage or any(int(g)==0 for _,_,g in coverage): raise SystemExit("missing action coverage")
