"""Independent Python integer oracle for the exact base-1000 TLA helper."""
from pathlib import Path
import argparse
import json
import shutil
base=Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser()
parser.add_argument('--output-dir',required=True,type=Path)
args=parser.parse_args()
out=args.output_dir.resolve()
out.mkdir(parents=True,exist_ok=True)
maximum=2**64-1
cases=[]
def w(n):
    assert 0 <= n < 1000**10
    return '<<'+','.join(str(n//1000**i%1000) for i in range(10))+'>>'
def add(name,expression,expected):
    cases.append({'name':name,'expression':expression,'expected_decimal':str(expected), 'expected_digits':w(expected)})
add('MAX_plus_one',f'Add({w(maximum)},One)',maximum+1)
add('MAX_round_trip',f'Sub({w(maximum+1)},One)',maximum)
add('borrow_across_second',f'Sub(Second,One)',10**9-1)
add('carry_across_nine_limbs',f'Add({w(10**27-1)},One)',10**27)
add('half_odd_MAX',f'Half({w(maximum)})',maximum//2)
add('midpoint_near_MAX',f'Mid({w(maximum-17)},{w(maximum)})',maximum-9)
for n in [0,1,999,1000,1001,10**9-1,10**9,10**9+1,maximum]:
 for d,k in [(0,1),(1,10**9),(10**6,1000)]:
    delta=0 if not d else (n+k-1)//k
    add(f'up_{n}_{d}',f'Up({w(n)},{d})',n+delta)
    add(f'down_{n}_{d}',f'Down({w(n)},{d})',n-delta)
for n in [10**6-1,10**6,10**6+1,maximum]:
 for k in [1,2]:add(f'quantized_{n}_{k}',f'Quantized({w(n)},{k})',n//1000**k*1000**k)
for n in [maximum//10**6,maximum//10**6+1]:add(f'ms_product_{n}',f'Shift({w(n)},2)',n*10**6)
for d in [0,1,10**6]:
 for rate in [-1,0,1]:add(f'truth_{d}_{rate}',f'AdvanceTruth({w(maximum)},{d},{rate})',maximum*(10**9+rate*d))
module='------------------------- MODULE ClockWordsCheck -------------------------\nEXTENDS ClockWords\nVARIABLE done\nInit == done=FALSE\nNext == /\\ ~done /\\ done\'=TRUE\nSpec == Init /\\ [][Next]_done\nArithmeticOK ==\n'
module+='\n'.join(' /\\ '+c['expression']+' = '+c['expected_digits'] for c in cases)
module+='\n /\\ Lt(Max64,Add(Max64,One))\n /\\ ~Le(Add(Max64,One),Max64)\n /\\ Sub(Add(Max64,One),Max64)=One\n /\\ Shift(Drop(Max64,2),2)=Quantized(Max64,2)\n=============================================================================\n'
assert module==(base/'04_evidence/ClockWordsCheck.tla.txt').read_text()
assert cases==json.loads((base/'04_evidence/word-vectors.json').read_text())
helpers=(base/'04_evidence/ClockWords.tla.txt').read_text()
body=helpers.split('EXTENDS Integers, Sequences, FiniteSets, TLC\n',1)[1].rsplit('=============================================================================',1)[0]
checks=module.split('ArithmeticOK ==\n',1)[1].rsplit('=============================================================================',1)[0]
main=(base/'02_models/L0_Calibration.tla').read_text()
assert body in main
assert 'ArithmeticOK ==\n'+checks in main
(out/'ClockWordsCheck.tla').write_text(module)
(out/'ClockWordsCheck.cfg').write_text('SPECIFICATION Spec\nCHECK_DEADLOCK FALSE\nINVARIANT ArithmeticOK\n')
shutil.copyfile(base/'04_evidence/ClockWords.tla.txt',out/'ClockWords.tla')
(out/'word-vectors.json').write_text(json.dumps(cases,indent=2)+'\n')
repo=base.parents[3]
(out/'config.env').write_text(f'REPO_ROOT={repo}\nTLA_TOOLS_JAR={repo}/tla/tools/tla2tools.jar\nTLC_WORKERS=1\nTLC_TIMEOUT=0\n')
print(len(cases),'independent Python cases and 4 order/identity checks')
print('Archived oracle matches; helper and ArithmeticOK are identical to embedded main-module text.')
print('Self-test module:',out/'ClockWordsCheck.tla')
