import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile

source = Path(sys.argv[1]).resolve() if len(sys.argv) == 2 else Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('verify_observation', source / 'verify-runtime-observation-acceptance.py')
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)
cases = []

def encode(value):
    return json.dumps(value, ensure_ascii=False, indent=2).encode() + b'\n'

def write_artifact(base, document, name, value):
    raw = value if isinstance(value, bytes) else encode(value)
    (base / name).write_bytes(raw)
    document['artifacts'][name]['bytes'] = len(raw)
    document['artifacts'][name]['sha256'] = hashlib.sha256(raw).hexdigest()

def app_change(base, document, change):
    app = document['app_observation']
    metadata = json.loads((base / app['files']['metadata.json']).read_bytes())
    def artifact(name):
        return json.loads((base / app['files'][name]).read_bytes())
    def save(name, value):
        write_artifact(base, document, app['files'][name], value)
        metadata['artifact_sha256'][name] = document['artifacts'][app['files'][name]]['sha256']
    change(metadata, artifact, save)
    write_artifact(base, document, app['files']['metadata.json'], metadata)
    files = app['files']
    document['app_observation'] = {k:v for k,v in metadata.items() if k != 'artifacts'}
    document['app_observation']['files'] = files

def run(name, mutate, expected):
    with tempfile.TemporaryDirectory(prefix='tw-observation-negative-') as tmp:
        base = Path(tmp)
        shutil.copytree(source / 'runtime-observation-acceptance', base / 'runtime-observation-acceptance')
        for file in ['runtime-observation-acceptance.json', 'runtime-observation-source-sha256.json', 'verify-runtime-observation-acceptance.py']:
            shutil.copyfile(source / file, base / file)
        document = json.loads((base / 'runtime-observation-acceptance.json').read_bytes())
        mutate(base, document)
        (base / 'runtime-observation-acceptance.json').write_bytes(encode(document))
        try:
            verifier.verify(base)
        except Exception as error:
            message = f'{type(error).__name__}: {error}'
            assert expected in message, (name, expected, message)
            cases.append({'case':name, 'status':'rejected', 'reason':message})
        else:
            raise AssertionError(f'{name}: accepted mutation')

def corrupt_bytes(base, document):
    path = base / document['host_process_tests']['files']['host/process.log']
    path.write_bytes(path.read_bytes() + b'corruption\n')
run('artifact byte change', corrupt_bytes, 'artifact digest')

def bad_signature(base, document):
    path = document['scenarios'][0]['signed_receipts'][0]['file']
    value = json.loads((base / path).read_bytes())
    value['signature'][0] ^= 1
    write_artifact(base, document, path, value)
run('signature changed with envelope rehashed', bad_signature, 'InvalidSignature')

def fee_open(base, document):
    path = document['scenarios'][0]['files']['account-report.json']
    value = json.loads((base / path).read_bytes())
    value['orders'][0]['fee_reconciled'] = False
    write_artifact(base, document, path, value)
run('fee closure removed with envelope rehashed', fee_open, 'Spot order or fees not closed')

def corrupt_input(base, document):
    path = document['scenarios'][1]['files']['export/input/segment-00000000000000000001.wal']
    value = bytearray((base / path).read_bytes())
    offset = value.index(b'TWAI0001\x04')
    value[offset + 25] ^= 1
    write_artifact(base, document, path, bytes(value))
run('input gap altered with artifact rehashed', corrupt_input, 'input frame CRC')

def source_count(base, document):
    document['source_binding']['file_count'] += 1
run('source count mismatch', source_count, 'source input count')

def repeated_challenge(metadata, artifact, save):
    first = artifact('outcomes/outcome-01000000000000000000000000000000.json')
    name = 'outcomes/outcome-02000000000000000000000000000000.json'
    second = artifact(name)
    challenge = first['observation']['challenge']
    second['observation']['challenge'] = challenge
    metadata['observations'][1]['challenge'] = bytes(challenge).hex()
    save(name, second)
run('two fresh queries reuse challenge after coherent rehash', lambda b,d:app_change(b,d,repeated_challenge), 'reused fresh challenge or READY')

def expired_observation(metadata, artifact, save):
    name = 'outcomes/outcome-02000000000000000000000000000000.json'
    value = artifact(name)
    value['observation']['observed_mono_ns'] = value['request']['valid_until_mono_ns'] + 1
    save(name, value)
run('fresh observation after original deadline', lambda b,d:app_change(b,d,expired_observation), 'fresh original deadline')

def duplicate_object(metadata, artifact, save):
    name = 'outcomes/outcome-02000000000000000000000000000000.json'
    value = artifact(name)
    value['observation']['objects'].append(value['observation']['objects'][0])
    metadata['observations'][1]['objects'] += 1
    save(name, value)
run('duplicate recovery root after coherent rehash', lambda b,d:app_change(b,d,duplicate_object), 'fresh objects must be ordered and unique')

def historical_live(metadata, artifact, save):
    value = artifact('outcomes/historical.json')
    value['historical'] = False
    save('outcomes/historical.json', value)
run('historical result presented as fresh', lambda b,d:app_change(b,d,historical_live), 'historical result changed original fact or time')

def premature_mirror(metadata, artifact, save):
    metadata['mirror_before_stop']['prefix']['sequence'] -= 1
run('premature stop before actual mirror prefix', lambda b,d:app_change(b,d,premature_mirror), 'mirror was not durably caught up before stop')

def unlisted(base, document):
    (base / 'runtime-observation-acceptance/unlisted.txt').write_text('extra')
run('unlisted archive object', unlisted, 'unlisted or missing bundle file')

print(json.dumps({'status':'verified','verifier_sha256':hashlib.sha256((source/'verify-runtime-observation-acceptance.py').read_bytes()).hexdigest(),'negative_cases':len(cases),'cases':cases},ensure_ascii=False,indent=2))
