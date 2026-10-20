#!/usr/bin/env python3
"""Record full upstream byte hashes without vendoring the whole documentation site."""
import hashlib, json, pathlib, urllib.request
PIN='f4a9c0be68235f758b46f571f7313c63d1306b25'
BASE=f'https://raw.githubusercontent.com/binance/binance-spot-api-docs/{PIN}/'
files=['rest-api.md','web-socket-api.md','fix-api.md','faqs/api_key_types.md','errors.md']
records=[]
for name in files:
    with urllib.request.urlopen(BASE+name, timeout=30) as r:
        data=r.read()
    records.append({'path':name,'url':BASE+name,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)})
pathlib.Path(__file__).with_name('spot-source-index.json').write_text(json.dumps({'commit':PIN,'files':records},ensure_ascii=False,indent=2)+'\n')
