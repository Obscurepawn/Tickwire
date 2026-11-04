"""Check English project guides, specification prose, and local Markdown links."""
import os
from pathlib import Path
import re
import urllib.parse

root = Path(__file__).resolve().parents[1]
errors = []
local_count = 0
ref_count = 0

files = []
for folder, directories, names in os.walk(root):
    directories[:] = [name for name in directories
                      if name not in {'.git', '.tools', 'target', '__pycache__'}]
    base = Path(folder)
    parts = base.relative_to(root).parts
    for name in names:
        if name in {'README.md', 'AGENTS.md'} or (
            parts and parts[0] in {'crates', 'specs', 'scripts'} and name.endswith('.md')
        ):
            files.append(base / name)

for p in sorted(files):
    s = p.read_text(encoding='utf-8')
    text = re.sub(r'```.*?```', '', s, flags=re.S)
    text = re.sub(r'(`+)(?!`)(.*?)\1(?!`)', '', text, flags=re.S)
    if re.search(r'[\u3400-\u9fff]', text):
        errors.append(f'{p}: untranslated prose')
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
