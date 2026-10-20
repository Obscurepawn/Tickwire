"""Check local documentation targets and diagram artifacts, not runtime behavior."""
from pathlib import Path
import re,json,urllib.parse,xml.etree.ElementTree as ET,hashlib
from PIL import Image
root=Path(__file__).resolve().parents[1]
errors=[];local_count=0;ref_count=0
files=[root/'README.md',*(root/'docs').rglob('*.md'),*(root/'specs').rglob('*.md'),*(root/'scripts').rglob('*.md')]
for p in files:
 s=p.read_text();text=re.sub(r'```.*?```','',s,flags=re.S)
 text=re.sub(r'(`+)(?!`)(.*?)\1(?!`)', '', text, flags=re.S)
 if sum(line.startswith('```') for line in s.splitlines())%2: errors.append(f'{p}: unmatched fence')
 definitions=dict(re.findall(r'^\[([^\]]+)\]:\s+(\S+)',text,re.M))
 for ref in re.findall(r'(?<!!)\[[^\]\n]*\]\[([^\]\n]+)\]',text):
  ref_count+=1
  if ref not in definitions:errors.append(f'{p}: undefined reference {ref}')
 for url in re.findall(r'!?\[[^\]\n]*\]\((<?[^\)\n]+>?)\)',text)+list(definitions.values()):
  url=url.strip('<>')
  if url.startswith(('http://','https://','mailto:','#')): continue
  url=urllib.parse.unquote(url.split('#')[0]);url=re.sub(r':\d+$','',url)
  dest=Path(url) if url.startswith('/') else p.parent/url
  local_count+=1
  if not dest.exists():errors.append(f'{p}: missing {dest}')
images=[]
for name in ['tickwire-architecture','order-state','recovery-sequence','process-topology','strategy-rollout','config-release']:
 ET.parse(root/f'docs/assets/{name}.svg')
 with Image.open(root/f'docs/assets/{name}.png') as im:im.verify()
 with Image.open(root/f'docs/assets/{name}.png') as im:images.append({'name':name,'size':im.size,'format':im.format})
delivery=json.loads((root/'docs/assets/tickwire-architecture.delivery.json').read_text())
visual=json.loads((root/'docs/assets/tickwire-architecture.visual-check.json').read_text())
assert delivery['ok'] and delivery['validation']['checksPassed']==9
assert visual['status']=='pass' and len(visual['containment']['viewports'])==4
for key,name in [('specification','tickwire-architecture.json'),('artifact','tickwire-architecture.html')]:
 data=(root/'docs/assets'/name).read_bytes()
 if hashlib.sha256(data).hexdigest()!=delivery[key]['sha256']:errors.append('receipt hash mismatch '+name)
 if len(data)!=delivery[key]['bytes']:errors.append('receipt byte mismatch '+name)
assert visual['artifact']['sha256']==delivery['artifact']['sha256']
manifest=json.loads((root/'docs/assets/diagram-assets.manifest.json').read_text())
manifest_checked=0
def check_manifest(value):
 global manifest_checked
 if isinstance(value,dict):
  if 'path' in value and 'sha256' in value:
   data=(root/'docs/assets'/value['path']).read_bytes()
   manifest_checked+=1
   if hashlib.sha256(data).hexdigest()!=value['sha256']:errors.append('manifest hash mismatch '+value['path'])
   if 'bytes' in value and len(data)!=value['bytes']:errors.append('manifest byte mismatch '+value['path'])
  for item in value.values():check_manifest(item)
 elif isinstance(value,list):
  for item in value:check_manifest(item)
check_manifest(manifest)
verification=(root/'docs/verification-plan.md').read_text()
test_counts={}
for prefix in ['U','I']:
 actual=re.findall(r'^\| ('+prefix+r'-\d+) \|',verification,re.M)
 expected=[f'{prefix}-{i:02d}' for i in range(1,len(actual)+1)]
 if sorted(actual)!=sorted(expected):errors.append('test case inventory mismatch '+prefix)
 test_counts[prefix]=len(actual)
for relative in ['docs/design/binance-ultra-low-latency.md','docs/design/shared-memory-ipc.md','docs/design/strategy-lifecycle.md','docs/verification-plan.md']:
 data=(root/relative).read_text()
 for term in ['tickwire-trader','首版先','未来双向','初期只支持']:
  if term in data:errors.append('obsolete architecture text '+relative+': '+term)
result={'manifest_assets':manifest_checked,'test_plan_cases':sum(test_counts.values()),'test_plan_groups':test_counts,'markdown_files':len(files),'local_links':local_count,'reference_usages':ref_count,'images':images,'archify_receipts':'hashes and sizes match; 9 checks; 4 viewports','errors':errors}
print(json.dumps(result,ensure_ascii=False,indent=2))
raise SystemExit(bool(errors))
