// Pure local DOM/timer/API fixtures: no browser dependency or network access.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
const html=readFileSync('internal/monitor/assets/index.html','utf8');
const scripts=[...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m=>m[1]);
for (const script of scripts) new vm.Script(script);

async function fixture() {
  const elements=new Map(),listeners=new Map(),timers=new Map(),intervals=new Map(),storage=new Map(); let nextID=0;
  const document={hidden:false,activeElement:null,contains:()=>true,addEventListener:(type,fn)=>listeners.set(type,fn),querySelectorAll:()=>[],querySelector:()=>element('fake-nav'),body:{classList:{contains:()=>false,toggle:()=>{}}}};
  function element(id) {
    if(!elements.has(id)) {
      const classes=new Set(id==='dashboardTab'?['active']:[]);
      const el={id,value:'',textContent:'',innerHTML:'',hidden:false,style:{},dataset:{},checked:false,disabled:false,scrollTop:0,scrollHeight:10,options:[],classList:{contains:c=>classes.has(c),add:c=>classes.add(c),remove:c=>classes.delete(c),toggle(c,on){on?classes.add(c):classes.delete(c);}},addEventListener(){},setAttribute(){},removeAttribute(){},getClientRects:()=>[1],focus:()=>document.activeElement=el,querySelector:()=>element('fake-input'),querySelectorAll:()=>[],remove(){},appendChild(){},add(opt){this.options.push(opt);}};
      elements.set(id,el);
    }
    return elements.get(id);
  }
  document.getElementById=element; document.documentElement=element('root');
  element('nodeStatusFilter').value='all'; element('nodeSort').value='default';
  const context={document,Response,AbortController,ReadableStream,TextDecoder,URL,Uint32Array,crypto:globalThis.crypto,console,navigator:{},Option:class{constructor(text,value){this.textContent=text;this.value=value;this.dataset={};}remove(){}},localStorage:{getItem:k=>storage.get(k)??null,setItem:(k,v)=>storage.set(k,v),removeItem:k=>storage.delete(k)},setTimeout:(fn,ms)=>{const id=++nextID;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id),setInterval:(fn,ms)=>{const id=++nextID;intervals.set(id,{fn,ms});return id;},clearInterval:id=>intervals.delete(id),addEventListener:(type,fn)=>listeners.set(type,fn),matchMedia:()=>({matches:false,addEventListener(){}}),getComputedStyle:()=>({getPropertyValue:()=> '#fff'}),confirm:()=>true,fetch:async path=>new Response(JSON.stringify(path==='/api/auth'?{no_password:true,authenticated:true}:{nodes:[]}),{headers:{'Content-Type':'application/json'}}),EventSource:class{static opened=[];constructor(url){this.url=url;this.closed=false;this.constructor.opened.push(this);}close(){this.closed=true;}}};
  context.window=context; vm.createContext(context); for(const script of scripts)vm.runInContext(script,context);
  await new Promise(resolve=>setImmediate(resolve)); await new Promise(resolve=>setImmediate(resolve));
  vm.runInContext('stopAutoRefresh(); stopTrafficSSE(); authReady=true;',context); timers.clear(); intervals.clear();
  return {context,element,timers,intervals,listeners,run:code=>vm.runInContext(code,context)};
}
const nodes=()=>Array.from({length:10000},(_,i)=>({tag:`node-${i}`,name:`Fixture ${i}`,uri:'http://fixture.invalid:1',region:'us',source:'inline',initial_check_done:true,available:true,last_latency_ms:500-i%500,success_count:2,failure_count:1,timeline:Array.from({length:60},()=>({success:true,latency_ms:5}))}));
const rows=el=>(el.innerHTML.match(/<tr>/g)||[]).length;

test('10k config and diagnostics render only 300 rows; timeline is bounded',async()=>{
 const f=await fixture(); f.context.fixtureNodes=nodes(); f.run('configNodes=fixtureNodes; window._debugNodes=fixtureNodes; renderConfigNodes(); renderDebugNodes();');
 assert.equal(rows(f.element('configNodesTableBody')),300); assert.equal(rows(f.element('debugNodesTableBody')),300); assert.equal((f.element('debugNodesTableBody').innerHTML.match(/tl-dot /g)||[]).length,9000);
 f.run('showMoreConfigNodes(); showMoreDebugNodes();'); assert.equal(rows(f.element('configNodesTableBody')),600); assert.equal(rows(f.element('debugNodesTableBody')),600);
 f.element('configNodeSearch').value='Fixture 9999';f.run('renderConfigNodes(true)');assert.equal(rows(f.element('configNodesTableBody')),1);
 f.element('debugNodeSearch').value='does not exist';f.run('renderDebugNodes(true)');assert.equal(rows(f.element('debugNodesTableBody')),0);assert.equal(f.element('debugNodesEmpty').hidden,false);
});
test('latency sort does not mutate cache and prioritizes measured healthy nodes',async()=>{
 const f=await fixture();f.context.fixtureNodes=[{tag:'unknown',available:true,initial_check_done:true,last_latency_ms:-1},{tag:'slow',available:true,initial_check_done:true,last_latency_ms:90},{tag:'fast',available:true,initial_check_done:true,last_latency_ms:2}];f.element('nodeSort').value='latency';f.run('allNodesCache=fixtureNodes; applyNodeFilters()');assert.ok(f.element('nodesTableBody').innerHTML.indexOf('fast')<f.element('nodesTableBody').innerHTML.indexOf('slow'));assert.equal(f.run('allNodesCache[0].tag'),'unknown');
});
test('SSE retries stop on visibility/auth/tab changes and stale callbacks',async()=>{
 const f=await fixture();f.run('connectTrafficSSE()');const source=f.run('trafficSource');source.onerror();assert.equal(f.timers.size,1);const retry=[...f.timers.values()][0];f.run('invalidateAuth()');assert.equal(f.timers.size,0);await retry.fn();assert.equal(f.run('trafficSource'),null);
 f.run('authReady=true; connectTrafficSSE()');const second=f.run('trafficSource');f.context.document.hidden=true;f.listeners.get('visibilitychange')();assert.equal(second.closed,true);assert.equal(f.run('trafficSource'),null);second.onmessage({data:'{"up":100}'});assert.equal(f.run('trafficDataUp.length'),0);
});
test('traffic history remains bounded and hidden chart is not updated',async()=>{
 const f=await fixture();f.run('connectTrafficSSE()');const source=f.run('trafficSource');for(let i=0;i<100;i++)source.onmessage({data:'{"up":1,"down":2}'});assert.equal(f.run('trafficTime.length'),60);assert.equal(f.element('trafficStatus').textContent,'实时');f.element('dashboardTab').classList.remove('active');source.onmessage({data:'{"up":999}'});assert.equal(f.run('trafficDataUp.at(-1)'),1);
});
test('log polling has one request in flight and suppresses hidden-page work',async()=>{
 const f=await fixture();f.element('dashboardTab').classList.remove('active');f.element('logsTab').classList.add('active');let calls=0,resolve;f.context.fetch=()=>{calls++;return new Promise(r=>resolve=r);};const first=f.run('pollLogs()');await f.run('pollLogs()');assert.equal(calls,1);resolve(new Response('{"logs":"fixture"}'));await first;assert.equal(f.element('consoleLogs').value,'fixture');f.context.document.hidden=true;await f.run('pollLogs()');assert.equal(calls,1);
});
test('finite request body reads include timeout; streams explicitly opt out',async()=>{
 const f=await fixture();f.context.fetch=(_,options)=>new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new Error('aborted'))));const request=f.run('apiFetch("/fixture",{timeoutMs:42})');const timer=[...f.timers.values()].find(t=>t.ms===42);timer.fn();await assert.rejects(request,/取消或超时/);assert.equal(f.run('activeRequests.size'),0);
 f.context.fetch=async()=>new Response('data: fixture\n\n');const stream=await f.run('apiFetch("/api/nodes/probe-all",{timeoutMs:0})');assert.equal(await stream.text(),'data: fixture\n\n');assert.equal(f.run('activeRequests.size'),0);
});
test('pool mode matches backend sequential spelling',()=>{assert.match(html,/<option value="sequential">/);assert.doesNotMatch(html,/<option value="round-robin">/);});

test('response-body timeout survives receiving headers',async()=>{
 const f=await fixture();
 f.context.fetch=async(_,options)=>new Response(new ReadableStream({start(controller){options.signal.addEventListener('abort',()=>controller.error(new Error('body aborted')));}}));
 const request=f.run('apiFetch("/fixture",{timeoutMs:43})');await new Promise(resolve=>setImmediate(resolve));
 const timer=[...f.timers.values()].find(t=>t.ms===43);assert.ok(timer);timer.fn();await assert.rejects(request,/取消或超时/);assert.equal(f.run('activeRequests.size'),0);
});
test('log request aborts on leaving log view',async()=>{
 const f=await fixture();f.element('dashboardTab').classList.remove('active');f.element('logsTab').classList.add('active');let signal;
 f.context.fetch=(_,options)=>new Promise((resolve,reject)=>{signal=options.signal;signal.addEventListener('abort',()=>reject(new Error('aborted')));});
 const request=f.run('pollLogs()');f.run('stopLogPolling()');assert.equal(signal.aborted,true);await request;assert.equal(f.run('logsLoading'),false);assert.equal(f.element('logStatus').textContent,'');
});

test('HTTP 200 busy SSE surfaces the backend reason and clears probe loading',async()=>{
 const f=await fixture(),toasts=[];
 f.context.showToast=(message,type)=>toasts.push({message,type});f.context.refresh=()=>{};
 f.context.fetch=async()=>new Response('data: {"type":"error","message":"批量探测已在进行中，请稍候"}\n\n',{headers:{'Content-Type':'text/event-stream'}});
 await f.run('probeAllNodes()');
 assert.deepEqual(toasts,[{message:'批量探测已在进行中，请稍候',type:'error'}]);assert.equal(f.run('isProbing'),false);assert.equal(f.element('probeProgress').classList.contains('show'),false);assert.equal(f.run('activeRequests.size'),0);
});

test('disabled GeoIP zero interval loads a valid default without blocking settings saves',async()=>{
 const f=await fixture();let interval='0s';
 f.element('settingSubInterval').options=[{value:'1h',dataset:{}},{value:'12h',dataset:{}}];
 f.context.fetch=async(path)=>new Response(JSON.stringify(path==='/api/settings'?{pool:{mode:'sequential'},geoip:{enabled:false,auto_update_interval:interval}}:{enabled:false,interval:'12h',subscriptions:[]}));
 await f.run('loadSettingsPage()');assert.equal(f.run('settingsLoaded'),true);assert.equal(f.element('settingGeoIPUpdateInterval').value,'24h');
 interval='6h';await f.run('loadSettingsPage()');assert.equal(f.element('settingGeoIPUpdateInterval').value,'6h');
});
