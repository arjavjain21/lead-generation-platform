const fs = require('fs');
const html = fs.readFileSync('/var/www/lead-generation-platform/frontend/index.html', 'utf8');
const segStart = html.lastIndexOf('// DATA EXPLORER (#/explorer)');
const anchor = html.indexOf('window.explorerState = {', segStart);
const scriptEnd = html.indexOf('</script>', anchor);
let tail = html.slice(anchor, scriptEnd);
const cut = tail.lastIndexOf('}); // End DOMContentLoaded');
let js = tail.slice(0, cut);
const escFn = html.match(/function escHtml[\s\S]*?\n        \}/)[0];
const consts = [];
['EX_MULTI_DEFS','EX_MULTI_LABELS','EX_TEXT_FILTERS','EX_FACET_DIMS','EX_LIMIT'].forEach(n => {
  const re = new RegExp('(?:const|let|var) ' + n + ' = [\\s\\S]*?;\\n');
  const m = html.match(re);
  if (m && !js.includes(n + ' =')) consts.push(m[0].trim());
});
js = 'let token = "t";\n' + consts.join('\n') + '\n' + escFn + '\n' + js;
const elements = {};
function el(id){ if(!elements[id]) elements[id]={id,value:'',checked:false,textContent:'',innerHTML:'',style:{},dataset:{},listeners:{},addEventListener(t,f){this.listeners[t]=f;},classList:{add(){},remove(){},toggle(){},contains:()=>false},disabled:false,closest(){return null;},focus(){},appendChild(){}}; return elements[id]; }
global.window = global;
global.document = { getElementById:(id)=>el(id), querySelector:(s)=>el(String(s).replace(/\W/g,'_')), querySelectorAll:()=>[], addEventListener:()=>{}, body: el('body') };
global.addEventListener = () => {};
global.localStorage = { getItem:()=>'t', setItem(){}, removeItem(){} };
const bodies = [];
global.fetch = async (url, opts) => {
  if (opts && opts.body) bodies.push({ url, body: JSON.parse(opts.body) });
  if (url.includes('/people/count')) return { ok:true, status:200, json: async () => ({count:5,approximate:false}) };
  if (url.includes('/people/facets')) return { ok:true, status:200, json: async () => ({ facets:{seniority:[{value:'vp',count:1}]}, source:'summary', as_of:'x' }) };
  if (url.includes('/people/search')) return { ok:true, status:200, json: async () => ({people:[],next_cursor:null}) };
  return { ok:true, status:200, json: async () => ({}) };
};
const timeouts = [];
const waits = [];
global.setTimeout = (f, ms) => { if (ms === 50) { waits.push(f); return 1; } timeouts.push(f); return 1; };
global.setInterval = () => 1;
global.location = { hash:'', pathname:'/', origin:'https://x', href:'https://x/' };
global.confirm = () => true;
let failures = [];
function check(n, c){ console.log((c?'PASS':'FAIL')+' | '+n); if(!c) failures.push(n); }
(async () => {
  process.on('uncaughtException', (e) => { console.error('UNCAUGHT', e.message); process.exitCode = 1; });
  try { (0, eval)(js); } catch (e) { console.error('EVAL CRASH:', e.message); process.exitCode = 1; return; }
  explorerWriteFilters({ seniority: ['vp'], industry: ['Software Development'], has_email: true });
  try { (typeof explorerRefresh === 'function') ? await explorerRefresh(false) : timeouts.splice(0).forEach(f => f()); } catch(e) { console.error('refresh err', e.message); }
  waits.splice(0).forEach(f => f()); // flush microtask waits deterministically
  console.log('bodies captured:', bodies.map(b => b.url).join(', ') || 'NONE');
  const count = bodies.find(b => b.url.includes('/people/count'));
  const facets = bodies.find(b => b.url.includes('/people/facets'));
  const search = bodies.find(b => b.url.includes('/people/search'));
  check('count body nests filters', !!count && !!count.body.filters && JSON.stringify(count.body.filters.seniority)==='["vp"]');
  check('facets body nests filters', !!facets && !!facets.body.filters && facets.body.filters.industry[0]==='Software Development');
  check('search body nests filters', !!search && !!search.body.filters && search.body.filters.has_email===true);
  check('limit stays top-level, no flat leak', !!search && search.body.limit && !search.body.seniority);
  console.log(failures.length ? 'FAILURES: '+failures.join(', ') : 'ALL PAYLOAD-SHAPE CHECKS PASSED');
  process.exitCode = failures.length ? 1 : 0;
})();
