// Optional real-browser smoke/performance test. All APIs are local fixtures;
// every external URL is blocked. Install Playwright, or set PLAYWRIGHT_MODULE.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
const require=createRequire(import.meta.url);
const { chromium }=require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const browser=await chromium.launch({headless:true,...(process.env.CHROME_CHANNEL?{channel:process.env.CHROME_CHANNEL}:{})});
try {
 const page=await browser.newPage({viewport:{width:1440,height:950}});
 const errors=[]; page.on('pageerror',e=>errors.push(e.message));
 const nodes=Array.from({length:10000},(_,i)=>({tag:`node-${i}`,name:`Fixture ${i}`,uri:'http://fixture.invalid:1',port:24000+i,region:'us',initial_check_done:true,available:true,last_latency_ms:i%500+1,source:'inline',success_count:2,failure_count:1,timeline:Array.from({length:60},()=>({success:true,latency_ms:5}))}));
 let loggedIn=true,reloads=0,subWrites=0,subscriptionInterval='12h0m0s';
 await page.addInitScript(()=>{
  window.__trafficSources=[]; window.__hiddenFlag=false;
  Object.defineProperty(document,'hidden',{get:()=>window.__hiddenFlag});
  window.EventSource=class { constructor(url){this.url=url;this.closed=false;window.__trafficSources.push(this);}close(){this.closed=true;} };
 });
 await page.route('**/*',route=>{
  const request=route.request(),u=new URL(request.url());
  if(u.hostname!=='127.0.0.1') {
   if(u.hostname==='cdn.jsdelivr.net' && process.env.ECHARTS_FIXTURE) return route.fulfill({contentType:'application/javascript',body:readFileSync(process.env.ECHARTS_FIXTURE,'utf8')});
   return route.abort();
  }
  if(u.pathname==='/')return route.fulfill({contentType:'text/html',body:readFileSync('internal/monitor/assets/index.html','utf8')});
  if(u.pathname==='/api/auth') {
   if(request.method()==='POST') loggedIn=true;
   if(request.method()==='DELETE') loggedIn=false;
   return route.fulfill({json:{no_password:false,authenticated:loggedIn,message:'fixture'}});
  }
  if(!loggedIn)return route.fulfill({status:401,json:{error:'fixture login required'}});
  if(u.pathname==='/api/nodes'||u.pathname==='/api/nodes/config')return route.fulfill({json:{nodes,total_nodes:nodes.length}});
  if(u.pathname==='/api/debug')return route.fulfill({json:{nodes,total_calls:30000,total_success:20000,success_rate:66.7}});
  if(u.pathname==='/api/settings') {
   if(request.method()==='PUT') { loggedIn=false; return route.fulfill({json:{auth_changed:true,need_reload:true}}); }
   return route.fulfill({json:{mode:'pool',pool:{mode:'sequential'},listener:{address:'127.0.0.1',port:2323,username:'fixture',password:'fixture'},management:{password:'old-fixture'},sticky:{session_enabled:true}}});
  }
  if(u.pathname==='/api/subscription/config') { if(request.method()==='PUT') subWrites++; return route.fulfill({json:{enabled:false,interval:subscriptionInterval,subscriptions:[]}}); }
  if(u.pathname==='/api/reload') reloads++;
  if(u.pathname==='/api/logs') return route.fulfill({json:{logs:'local fixture logs'}});
  return route.fulfill({json:{}});
 });
 await page.goto('http://127.0.0.1:45454/'); await page.waitForSelector('#nodesTableBody tr');
 assert.equal(await page.locator('#nodesTableBody tr').count(),300);
 await page.selectOption('#nodeSort','latency');
 assert.match(await page.locator('#nodesTableBody tr').first().innerText(),/1ms/);
 let start=Date.now();await page.evaluate(()=>switchTab('manage'));await page.waitForSelector('#configNodesTableBody tr');const manageMs=Date.now()-start;
 assert.equal(await page.locator('#configNodesTableBody tr').count(),300);
 await page.fill('#configNodeSearch','Fixture 9999');assert.equal(await page.locator('#configNodesTableBody tr').count(),1);
 start=Date.now();await page.evaluate(()=>switchTab('debug'));await page.waitForSelector('#debugNodesTableBody tr');const debugMs=Date.now()-start;
 assert.equal(await page.locator('#debugNodesTableBody tr').count(),300);
 const domNodes=await page.locator('*').count(); assert.ok(domNodes<30000,`DOM nodes ${domNodes}`);
 if(process.env.ECHARTS_FIXTURE) {
  assert.equal(await page.evaluate(()=>successRateChartInst.getOption().series[0].data[0].value),66.7);
  await page.evaluate(()=>toggleTheme());assert.equal(await page.evaluate(()=>successRateChartInst.getOption().series[0].data[0].value),66.7);
 }
 await page.evaluate(()=>switchTab('dashboard'));
 await page.evaluate(()=>{ __trafficSources.at(-1).onerror(); invalidateAuth(); });
 const opened=await page.evaluate(()=>__trafficSources.length);await page.waitForTimeout(1300);assert.equal(await page.evaluate(()=>__trafficSources.length),opened);
 await page.evaluate(()=>{authReady=true;connectTrafficSSE();__hiddenFlag=true;document.dispatchEvent(new Event('visibilitychange'));});
 assert.equal(await page.evaluate(()=>trafficSource),null);
 await page.evaluate(()=>{__hiddenFlag=false;document.dispatchEvent(new Event('visibilitychange'));});
 if(process.env.ECHARTS_FIXTURE) {
  await page.evaluate(()=>{allNodesCache=[];updateDashboardCharts();});
  assert.equal(await page.evaluate(()=>regionChartInst.getOption().series[0].data.length),0);
  assert.equal(await page.evaluate(()=>latencyChartInst.getOption().series[0].data.length),0);
 }
 await page.evaluate(()=>switchTab('settings'));await page.waitForFunction(()=>settingsLoaded);
 assert.equal(await page.inputValue('#settingPoolMode'),'sequential');assert.equal(await page.inputValue('#settingSubInterval'),'12h');
 subscriptionInterval='1h30m0s';await page.evaluate(()=>loadSettingsPage());assert.equal(await page.inputValue('#settingSubInterval'),'1h30m0s');
 subscriptionInterval='12h0m0s';await page.evaluate(()=>loadSettingsPage());assert.equal(await page.inputValue('#settingSubInterval'),'12h');
 await page.fill('#settingMgmtPassword','new-fixture');await page.check('#settingSubEnabled');
 await page.evaluate(()=>handleSettingsSave({preventDefault(){},target:document.querySelector('#settingsTab form')}));
 assert.equal(await page.evaluate(()=>authReady),false);assert.equal(reloads,0);assert.equal(subWrites,0);assert.ok(await page.locator('#loginOverlay').evaluate(el=>el.classList.contains('show')));
 await page.fill('#password','new-fixture');await page.evaluate(()=>handleLogin({preventDefault(){},target:document.querySelector('#loginOverlay form')}));
 assert.equal(await page.evaluate(()=>authReady),true);assert.equal(await page.isChecked('#settingSubEnabled'),true);
 for(const width of [375,768,1024,1440]) {
  await page.setViewportSize({width,height:950});await page.evaluate(()=>switchTab('manage'));
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),`overflow at ${width}`);
 }
 await page.evaluate(()=>logout());assert.equal(await page.evaluate(()=>authReady),false);assert.equal(await page.evaluate(()=>trafficSource),null);
 assert.equal(await page.inputValue('#settingMgmtPassword'),''); assert.equal(await page.inputValue('#settingListenerPass'),''); assert.equal(await page.locator('#configNodesTableBody tr').count(),0);
 assert.deepEqual(errors,[]);
 console.log(JSON.stringify({fixtureNodes:10000,manageRows:300,debugRows:300,manageMs,debugMs,domNodes,baselineDOM:826022,domReduction:(100-domNodes/826022*100).toFixed(1)+'%',viewports:[375,768,1024,1440],authChange:'no dependent writes after session revocation',errors}));
} finally { await browser.close(); }
