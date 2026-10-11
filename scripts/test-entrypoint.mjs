import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

function fixture(run) {
  const dir=mkdtempSync(join(tmpdir(),'easy-proxies-entrypoint-'));
  try {
    const configDir=join(dir,'config'); mkdirSync(configDir);
    const binary=join(dir,'fake-proxy'); const argsFile=join(dir,'args');
    writeFileSync(binary,'#!/bin/sh\nprintf "%s\\n" "$@" > "$FIXTURE_ARGS"\n',{mode:0o755});
    const example=join(dir,'example.yaml'); writeFileSync(example,'nodes_file: nodes.txt\n');
    const env={...process.env,EASY_PROXIES_CONFIG_DIR:configDir,EASY_PROXIES_EXAMPLE_CONFIG:example,EASY_PROXIES_BINARY:binary,FIXTURE_ARGS:argsFile};
    run({dir,configDir,example,argsFile,env,exec:(args=[])=>spawnSync('sh',['entrypoint.sh',...args],{env,encoding:'utf8'})});
  } finally { rmSync(dir,{recursive:true,force:true}); }
}
test('entrypoint creates private defaults and execs selected config',()=>fixture(f=>{
  const r=f.exec(); assert.equal(r.status,0,r.stderr); assert.equal(readFileSync(f.argsFile,'utf8'),`--config\n${f.configDir}/config.yaml\n`);
  assert.ok(existsSync(join(f.configDir,'nodes.txt'))); assert.equal(statSync(join(f.configDir,'config.yaml')).mode & 0o777,0o600);
}));
for(const form of ['--config','-config','--config=','-config=']) test(`custom config bypasses unrelated mounts (${form})`,()=>fixture(f=>{
  mkdirSync(join(f.configDir,'config.yaml')); const custom=join(f.dir,'custom.yaml'); writeFileSync(custom,'fixture: true\n');
  const args=form.endsWith('=') ? [form+custom] : [form,custom]; const r=f.exec(args);
  assert.equal(r.status,0,r.stderr); assert.equal(readFileSync(f.argsFile,'utf8'),args.join('\n')+'\n'); assert.ok(!existsSync(join(f.configDir,'nodes.txt')));
}));
test('directory bind mount fails with actionable error',()=>fixture(f=>{ mkdirSync(join(f.configDir,'config.yaml')); const r=f.exec(); assert.equal(r.status,1); assert.match(r.stderr,/directory, not a file/); assert.ok(!existsSync(f.argsFile)); }));
test('failed config copy never starts proxy',()=>fixture(f=>{ f.env.EASY_PROXIES_EXAMPLE_CONFIG=join(f.dir,'missing'); const r=f.exec(); assert.equal(r.status,1); assert.match(r.stderr,/Failed to create/); assert.ok(!existsSync(f.argsFile)); }));
test('directory nodes mount fails before exec',()=>fixture(f=>{ mkdirSync(join(f.configDir,'nodes.txt')); const r=f.exec(); assert.equal(r.status,1); assert.match(r.stderr,/nodes.txt is a directory/); assert.ok(!existsSync(f.argsFile)); }));
test('missing or empty --config value fails',()=>fixture(f=>{ for(const args of [['--config'],['--config='],['--config',join(f.dir,'missing')]]) { const r=f.exec(args); assert.equal(r.status,1); assert.ok(!existsSync(f.argsFile)); } }));
test('help does not require valid mounts',()=>fixture(f=>{ mkdirSync(join(f.configDir,'config.yaml')); assert.equal(f.exec(['--help']).status,0); }));
