"""Check Markdown consistency for docs that live in the repository.

Scope after the docs/ directory was removed:
- README.md, AGENTS.md, every AGENTS.md under crates/, and all *.md under specs/
  and scripts/.
- For each file: fenced code blocks are balanced, reference-style links have
  definitions, and local (non-http) links point at existing files.

This script deliberately no longer checks the deleted docs/ assets
(diagram PNG/SVG hashes, verification plan numbering, design-doc wording).
It checks documentation structure only, not runtime behavior.
"""
from pathlib import Path
import re
import urllib.parse

root = Path(__file__).resolve().parents[1]
errors = []
local_count = 0
ref_count = 0

files = [root / 'README.md', root / 'AGENTS.md']
files += sorted((root / 'crates').glob('*/AGENTS.md'))
for folder in ['specs', 'scripts']:
    base = root / folder
    if base.exists():
        files += sorted(base.rglob('*.md'))

for p in files:
    s = p.read_text(encoding='utf-8')
    text = re.sub(r'```.*?```', '', s, flags=re.S)
    text = re.sub(r'(`+)(?!`)(.*?)\1(?!`)', '', text, flags=re.S)
    if sum(line.startswith('```') for line in s.splitlines()) % 2:
        errors.append(f'{p}: unmatched fence')
    definitions = dict(re.findall(r'^\[([^\]]+)\]:\s+(\S+)', text, re.M))
    for ref in re.findall(r'(?<!!)\[[^\]\n]*\]\[([^\]\n]+)\]', text):
        ref_count += 1
        if ref not in definitions:
            errors.append(f'{p}: undefined reference {ref}')
    for url in re.findall(r'!?\[[^\]\n]*\]\((<?[^\)\n]+>?)\)', text) + list(definitions.values()):
        url = url.strip('<>')
        if url.startswith(('http://', 'https://', 'mailto:', '#')):
            continue
        url = urllib.parse.unquote(url.split('#')[0])
        url = re.sub(r':\d+$', '', url)
        if not url:
            continue
        dest = Path(url) if url.startswith('/') else p.parent / url
        local_count += 1
        if not dest.exists():
            errors.append(f'{p}: missing {dest}')

result = {
    'markdown_files': len(files),
    'local_links': local_count,
    'reference_usages': ref_count,
    'errors': errors,
}
import json
print(json.dumps(result, ensure_ascii=False, indent=2))
raise SystemExit(bool(errors))
