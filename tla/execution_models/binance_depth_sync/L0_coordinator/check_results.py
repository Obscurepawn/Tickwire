#!/usr/bin/env python3
"""Summarize only completed TLC runs; reject missing/dead named actions."""
from pathlib import Path
import hashlib
import json
import re
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(ROOT / 'scripts'))
from tlc_coverage import final_counts

model = HERE / '02_models/L0_DepthSync.tla'
source = model.read_text()
next_text = source.split('Next == ', 1)[1].split('\nSpec ==', 1)[0]
expected = set(re.findall(r'\b[A-Z][A-Za-z]+\b', next_text))
rows = []
for name in ('L0_DepthSync', 'L0_UsdM', 'L0_SpotBuffered', 'L0_UsdMBuffered'):
    cfg = HERE / '02_models' / (name + '.cfg')
    log = HERE / 'runs' / (name + '.log')
    data = log.read_text()
    assert '>>> elapsed:' in data and '(exit 0)' in data, name
    # Shared parser rejects unfinished runs and ignores intermediate statistics.
    final_counts(data, 'L0_DepthSync')
    final_coverage = data.rsplit('The coverage statistics at ', 1)[1]
    counts = re.findall(r'^([\d,]+) states generated, ([\d,]+) distinct states found, 0 states left on queue\.', data, re.M)
    assert len(counts) == 1, (name, counts)
    generated, distinct = (int(x.replace(',', '')) for x in counts[0])
    actions = {}
    for action, first, count in re.findall(r'^<([A-Za-z]+) line .*?>: (\d+):(\d+)$', final_coverage, re.M):
        if action != 'Init':
            assert action not in actions, (name, action)
            actions[action] = {'distinct': int(first), 'generated': int(count)}
    assert set(actions) == expected, (name, expected - set(actions), set(actions) - expected)
    assert all(v['generated'] > 0 for v in actions.values()), (name, actions)
    rows.append({
        'configuration': name, 'verdict': 'PASS', 'generated': generated,
        'distinct': distinct, 'depth': int(re.search(r'The depth of the complete state graph search is (\d+)\.', data)[1]),
        'tlc_elapsed': re.search(r'Finished in (.+?) at ', data)[1],
        'runner_seconds': int(re.search(r'>>> elapsed: (\d+)s \(exit 0\)', data)[1]),
        'actions': actions, 'cfg_sha256': hashlib.sha256(cfg.read_bytes()).hexdigest(),
        'log_sha256': hashlib.sha256(log.read_bytes()).hexdigest()
    })
result = {
    'model_sha256': hashlib.sha256(model.read_bytes()).hexdigest(),
    'jar_sha256': hashlib.sha256((ROOT / 'tla/tools/tla2tools.jar').read_bytes()).hexdigest(),
    'coverage_parser_sha256': hashlib.sha256((ROOT / 'scripts/tlc_coverage.py').read_bytes()).hexdigest(),
    'workers': 1, 'coverage_minutes': 1, 'invariants': 7, 'safety_properties': 6,
    'named_next_actions': len(expected), 'results': rows,
}
(HERE / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
for row in rows:
    print(f"{row['configuration']}: PASS; {row['generated']} generated / {row['distinct']} distinct; depth {row['depth']}; {row['tlc_elapsed']}; {len(row['actions'])} / {len(expected)} actions generated > 0; delayed={row['actions']['SnapshotDelayed']['generated']}")
