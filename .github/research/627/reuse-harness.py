#!/usr/bin/env python3
"""Research-only compiler/setup reuse harness for oxide-batch #627."""
from __future__ import annotations
import argparse, hashlib, importlib.util, json, os, platform, subprocess, sys, tarfile, time, unittest
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

SCHEMA='oxide-batch-reuse/v1'; SEED_CMD=['cargo','test','--workspace','--all-features','--lib','--no-run']
RESEARCH=Path('.github/research/627'); WORKFLOW=Path('.github/workflows/research-627-integration-reuse.yml')

def fail(msg): raise ValueError(msg)
def run(cmd,cwd=None,capture=False): return subprocess.run(cmd,cwd=cwd,check=True,text=True,capture_output=capture)
def git(root,*args): return run(['git','-C',str(root),*args],capture=True).stdout.strip()
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):
    x=json.loads(path.read_text());
    if not isinstance(x,dict): fail(f'{path}: expected JSON object')
    return x
def now(): return datetime.now(timezone.utc).isoformat()

def sharder(root):
    p=root/'.github/scripts/run-integration-shard.py'; spec=importlib.util.spec_from_file_location('sharder627',p)
    if not p.is_file() or spec is None or spec.loader is None: fail('production sharder unavailable')
    m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    if getattr(m,'SHARD_COUNT',None)!=4: fail('production SHARD_COUNT must be 4')
    return m

def inventory(root):
    m=sharder(root); old=Path.cwd()
    try: os.chdir(root); targets=m.load_targets(); parts=m.partition(targets)
    finally: os.chdir(old)
    flat=[x for p in parts for x in p]; counts=Counter(flat)
    if len(parts)!=4 or any(not p for p in parts) or set(flat)!=set(targets) or any(v!=1 for v in counts.values()): fail('production partition is not exact one-to-one cover')
    return targets,parts

def harness_identity(root):
    names=git(root,'ls-files','--',str(RESEARCH),str(WORKFLOW)).splitlines(); names=sorted(x for x in names if x)
    if str(WORKFLOW) not in names or not any(x.startswith(str(RESEARCH)+'/') for x in names): fail('tracked research harness incomplete')
    files={x:sha(root/x) for x in names}; h=hashlib.sha256()
    for x in names: h.update(x.encode()+b'\0'+files[x].encode()+b'\n')
    return files,h.hexdigest()

def plan(a):
    root=a.harness_root.resolve(); target=a.target_root.resolve(); head=a.head_sha; target_sha=a.target_sha
    if len(head)!=40 or len(target_sha)!=40: fail('head/target SHA must be 40 hex')
    base=git(root,'merge-base',head,'origin/main'); run(['git','-C',str(root),'merge-base','--is-ancestor',base,'origin/main'])
    changed=[x for x in git(root,'diff','--name-only',base,head).splitlines() if x]
    if any(x!=str(WORKFLOW) and not x.startswith(str(RESEARCH)+'/') for x in changed): fail(f'non-research delta: {changed}')
    if git(target,'rev-parse','HEAD')!=target_sha: fail('target checkout mismatch')
    run(['git','-C',str(root),'merge-base','--is-ancestor',target_sha,'origin/main'])
    if git(target,'status','--porcelain','--untracked-files=no'): fail('target checkout dirty')
    targets,parts=inventory(target); files,digest=harness_identity(root)
    out={'schema':SCHEMA,'kind':'plan','harness_base_sha':base,'harness_head_sha':head,'harness_logic_files':files,'harness_logic_sha256':digest,'target_sha':target_sha,'target_count':len(targets),'target_digest_sha256':hashlib.sha256(('\n'.join(targets)+'\n').encode()).hexdigest(),'targets':targets,'partitions':parts,'partition_sizes':[len(x) for x in parts],'toolchain_sha256':sha(target/'rust-toolchain.toml'),'cargo_lock_sha256':sha(target/'Cargo.lock'),'workspace_root_name':'target-src','seed_command':SEED_CMD,'worker_command':'python3 .github/scripts/run-integration-shard.py <index> 4'}
    a.out.write_text(json.dumps(out,indent=2,sort_keys=True)+'\n');
    if a.github_output:
        with a.github_output.open('a') as f: f.write(f"target_sha={target_sha}\ntarget_count={len(targets)}\nharness_logic_digest={digest}\n")

def same(plan,other):
    for k in ('harness_head_sha','harness_logic_sha256','target_sha','target_count','target_digest_sha256','toolchain_sha256','cargo_lock_sha256'):
        if other.get(k)!=plan.get(k): fail(f'{k} mismatch')

def seed(a):
    p=load(a.plan); root=a.target_root.resolve()
    if p.get('schema')!=SCHEMA or p.get('kind')!='plan' or git(root,'rev-parse','HEAD')!=p['target_sha']: fail('seed identity mismatch')
    if os.getenv('RUNNER_ARCH')!='ARM64' or platform.machine()!='aarch64': fail('ARM64 required')
    run(['sccache','--zero-stats']); started=now(); t=time.monotonic(); run(SEED_CMD,cwd=root); elapsed=round(time.monotonic()-t,3)
    if git(root,'status','--porcelain','--untracked-files=no') or not (root/'target').is_dir(): fail('seed modified source or target missing')
    a.archive.parent.mkdir(parents=True,exist_ok=True); run(['tar','--zstd','-cf',str(a.archive.resolve()),'-C',str(root.parent),root.name])
    stats=json.loads(run(['sccache','--show-stats','--stats-format=json'],capture=True).stdout)
    m={**{k:p[k] for k in ('harness_head_sha','harness_logic_sha256','target_sha','target_count','target_digest_sha256','toolchain_sha256','cargo_lock_sha256')},'schema':SCHEMA,'kind':'seed','status':'success','seed_command':SEED_CMD,'seed_seconds':elapsed,'started_at':started,'completed_at':now(),'archive_sha256':sha(a.archive),'archive_bytes':a.archive.stat().st_size,'archive_name':a.archive.name,'sccache_stats':stats,'cache':{k:os.getenv(k) for k in ('RESEARCH_CACHE_KEY','RESEARCH_CACHE_MATCHED_KEY','RESEARCH_CACHE_HIT')}}
    a.manifest.write_text(json.dumps(m,indent=2,sort_keys=True)+'\n')

def safe_members(names,root):
    pre=root+'/'
    if not names: fail('empty archive')
    for raw in names:
        n=raw.rstrip('/'); q=PurePosixPath(n)
        if q.is_absolute() or '..' in q.parts or (n!=root and not n.startswith(pre)): fail(f'unsafe archive member: {raw}')

def extract(a):
    p,m=load(a.plan),load(a.manifest); same(p,m)
    if m.get('kind')!='seed' or m.get('status')!='success' or sha(a.archive)!=m.get('archive_sha256') or a.archive.stat().st_size!=m.get('archive_bytes'): fail('seed artifact integrity mismatch')
    root=a.target_root.resolve()
    if root.exists() or root.name!=p['workspace_root_name']: fail('target root preexists or wrong name')
    names=run(['tar','--zstd','-tf',str(a.archive)],capture=True).stdout.splitlines(); safe_members(names,root.name)
    run(['tar','--zstd','-xf',str(a.archive),'-C',str(root.parent)])
    if git(root,'rev-parse','HEAD')!=p['target_sha'] or git(root,'status','--porcelain','--untracked-files=no'): fail('extracted source identity mismatch')
    if sha(root/'rust-toolchain.toml')!=p['toolchain_sha256'] or sha(root/'Cargo.lock')!=p['cargo_lock_sha256'] or not (root/'target').is_dir(): fail('extracted build identity mismatch')
    a.marker.write_text(json.dumps({'schema':SCHEMA,'kind':'prepared','target_sha':p['target_sha'],'archive_sha256':m['archive_sha256']})+'\n')

def worker(a):
    p,m,r=load(a.plan),load(a.manifest),load(a.marker); same(p,m)
    if a.shard not in range(4) or r.get('target_sha')!=p['target_sha'] or r.get('archive_sha256')!=m['archive_sha256']: fail('prepared worker identity mismatch')
    root=a.target_root.resolve(); targets,parts=inventory(root)
    if targets!=p['targets'] or parts!=p['partitions']: fail('worker inventory/partition mismatch')
    run(['sccache','--zero-stats']); t=time.monotonic(); proc=subprocess.run(['python3','.github/scripts/run-integration-shard.py',str(a.shard),'4'],cwd=root); elapsed=round(time.monotonic()-t,3)
    stats=json.loads(run(['sccache','--show-stats','--stats-format=json'],capture=True).stdout)
    e={'schema':SCHEMA,'kind':'worker','status':'success' if proc.returncode==0 else 'failed','exit_code':proc.returncode,'shard_index':a.shard,'selected_targets':parts[a.shard],'all_targets':targets,'execution_seconds':elapsed,'seed_archive_sha256':m['archive_sha256'],'sccache_stats':stats,**{k:p[k] for k in ('harness_head_sha','harness_logic_sha256','target_sha','target_count','target_digest_sha256')}}
    a.out.write_text(json.dumps(e,indent=2,sort_keys=True)+'\n'); return proc.returncode

def validate(plan,seed,items):
    same(plan,seed)
    if len(items)!=4: fail('expected four workers')
    by={}
    for x in items:
        same(plan,x); i=x.get('shard_index')
        if x.get('kind')!='worker' or x.get('status')!='success' or x.get('exit_code')!=0 or i not in range(4) or i in by or x.get('selected_targets')!=plan['partitions'][i] or not isinstance(x.get('sccache_stats'),dict): fail('invalid worker evidence')
        by[i]=x
    flat=[n for i in range(4) for n in by[i]['selected_targets']]; c=Counter(flat)
    if set(flat)!=set(plan['targets']) or any(v!=1 for v in c.values()): fail('worker evidence not exact cover')
    times=[float(by[i]['execution_seconds']) for i in range(4)]
    return {'execution_seconds':times,'execution_critical_seconds':max(times),'execution_sum_seconds':round(sum(times),3)}

def summarize(a):
    p,m=load(a.plan),load(a.manifest); items=[load(x) for x in sorted(a.evidence_dir.glob('evidence-reuse-*.json'))]; out={'schema':SCHEMA,'kind':'summary','valid':False}
    try: out.update({'valid':True,'target_sha':p['target_sha'],'target_count':p['target_count'],'target_digest_sha256':p['target_digest_sha256'],'partition_sizes':p['partition_sizes'],'harness_logic_sha256':p['harness_logic_sha256'],'seed':{'seed_seconds':m['seed_seconds'],'archive_bytes':m['archive_bytes'],'archive_sha256':m['archive_sha256'],'sccache_stats':m['sccache_stats']},'execution':validate(p,m,items),'worker_cache':[{'shard_index':x['shard_index'],'sccache_stats':x['sccache_stats']} for x in sorted(items,key=lambda z:z['shard_index'])]})
    except Exception as e: out['error']=str(e)
    a.out.write_text(json.dumps(out,indent=2,sort_keys=True)+'\n'); return 0 if out['valid'] else 1

class Tests(unittest.TestCase):
    def test_archive(self):
        safe_members(['target-src/','target-src/target/a'],'target-src')
        for x in [['../x'],['target-src/../../x'],['other/x'],['/x']]:
            with self.assertRaises(ValueError): safe_members(x,'target-src')
    def test_seed_scope(self): self.assertEqual(SEED_CMD,['cargo','test','--workspace','--all-features','--lib','--no-run']); self.assertNotIn('--tests',SEED_CMD)
    def test_manual_workflow(self):
        w=(Path(__file__).resolve().parents[2]/'workflows'/WORKFLOW.name).read_text(); self.assertIn('workflow_dispatch:',w); self.assertNotIn('pull_request:',w); self.assertNotIn('\n  push:',w)

def selftest(_): return 0 if unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests)).wasSuccessful() else 1

def parser():
    p=argparse.ArgumentParser(); s=p.add_subparsers(dest='cmd',required=True)
    q=s.add_parser('plan'); q.add_argument('--harness-root',type=Path,required=True); q.add_argument('--target-root',type=Path,required=True); q.add_argument('--head-sha',required=True); q.add_argument('--target-sha',required=True); q.add_argument('--out',type=Path,required=True); q.add_argument('--github-output',type=Path); q.set_defaults(f=plan)
    q=s.add_parser('seed'); q.add_argument('--plan',type=Path,required=True); q.add_argument('--target-root',type=Path,required=True); q.add_argument('--archive',type=Path,required=True); q.add_argument('--manifest',type=Path,required=True); q.set_defaults(f=seed)
    q=s.add_parser('extract'); q.add_argument('--plan',type=Path,required=True); q.add_argument('--manifest',type=Path,required=True); q.add_argument('--archive',type=Path,required=True); q.add_argument('--target-root',type=Path,required=True); q.add_argument('--marker',type=Path,required=True); q.set_defaults(f=extract)
    q=s.add_parser('run'); q.add_argument('--plan',type=Path,required=True); q.add_argument('--manifest',type=Path,required=True); q.add_argument('--marker',type=Path,required=True); q.add_argument('--target-root',type=Path,required=True); q.add_argument('--shard',type=int,required=True); q.add_argument('--out',type=Path,required=True); q.set_defaults(f=worker)
    q=s.add_parser('summarize'); q.add_argument('--plan',type=Path,required=True); q.add_argument('--manifest',type=Path,required=True); q.add_argument('--evidence-dir',type=Path,required=True); q.add_argument('--out',type=Path,required=True); q.set_defaults(f=summarize)
    q=s.add_parser('self-test'); q.set_defaults(f=selftest); return p

def main():
    try: a=parser().parse_args(); code=a.f(a)
    except Exception as e: print(f'::error::{e}'); code=1
    raise SystemExit(code or 0)
if __name__=='__main__': main()
