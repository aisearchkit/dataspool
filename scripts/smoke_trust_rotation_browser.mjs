const [base, token, expectedActor, activePortFile] = process.argv.slice(2);
if (!base || !token || !expectedActor || !activePortFile) {
  throw new Error('usage: node smoke_trust_rotation_browser.mjs BASE TOKEN ACTOR DEVTOOLS_ACTIVE_PORT_FILE');
}
const fs = await import('node:fs/promises');
const { readFile } = fs;
const activePort = (await readFile(activePortFile, 'utf8')).trim().split(/\s+/)[0];
const tabs = await fetch(`http://127.0.0.1:${activePort}/json/list`).then(r => r.json());
let tab = tabs.find(item => item.type === 'page' && item.url === 'about:blank');
if (!tab) {
  const response = await fetch(`http://127.0.0.1:${activePort}/json/new?${encodeURIComponent(`${base}/admin/trust-rotations`)}`, { method: 'PUT' });
  if (!response.ok) throw new Error(`Chrome could not open the operator UI: HTTP ${response.status}`);
  tab = await response.json();
} else {
  const opened = await fetch(`http://127.0.0.1:${activePort}/json/new?${encodeURIComponent(`${base}/admin/trust-rotations`)}`, { method: 'PUT' });
  if (!opened.ok) throw new Error(`Chrome could not open the operator UI: HTTP ${opened.status}`);
  tab = await opened.json();
}
const socket = new WebSocket(tab.webSocketDebuggerUrl);
let nextId = 0;
const pending = new Map();
socket.addEventListener('message', event => {
  const message = JSON.parse(event.data);
  if (message.id && pending.has(message.id)) {
    const { resolve, reject } = pending.get(message.id);
    pending.delete(message.id);
    if (message.error) reject(new Error(message.error.message));
    else resolve(message.result);
  }
});
await new Promise((resolve, reject) => {
  socket.addEventListener('open', resolve, { once: true });
  socket.addEventListener('error', reject, { once: true });
});
const command = (method, params = {}) => new Promise((resolve, reject) => {
  const id = ++nextId;
  pending.set(id, { resolve, reject });
  socket.send(JSON.stringify({ id, method, params }));
});
const evaluate = async expression => {
  const result = await command('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
  if (result.exceptionDetails) throw new Error(result.exceptionDetails.text || 'browser JavaScript evaluation failed');
  return result.result?.value;
};
try {
  await command('Page.enable');
  await command('Runtime.enable');
  await command('Page.navigate', { url: `${base}/admin/trust-rotations` });
  let loaded = false;
  for (let i = 0; i < 80; i++) {
    loaded = await evaluate("document.readyState === 'complete' && !!document.querySelector('#connect')").catch(() => false);
    if (loaded) break;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  if (!loaded) throw new Error('operator UI did not finish loading in Chrome');
  await evaluate(`document.querySelector('#token').value = ${JSON.stringify(token)}; document.querySelector('#connect').click(); true`);
  let connected = false;
  let lastState = '';
  for (let i = 0; i < 120; i++) {
    lastState = await evaluate("JSON.stringify({connection:document.querySelector('#connection-label')?.textContent, page:document.querySelector('#page-info')?.textContent, message:document.querySelector('#message')?.textContent})");
    const state = JSON.parse(lastState);
    if (state.connection?.includes(`Connected · ${expectedActor}`) && state.page?.includes('0 records')) {
      connected = true;
      break;
    }
    if (state.connection === 'Connection failed') throw new Error(`operator UI authentication failed: ${state.message}`);
    await new Promise(resolve => setTimeout(resolve, 150));
  }
  if (!connected) throw new Error(`operator UI did not reach authenticated empty-list state: ${lastState}`);
  const rendered = await evaluate("JSON.stringify({actor:document.querySelector('#connection-label').textContent, active:document.querySelector('#stat-active').textContent, empty:document.querySelector('#rotation-rows').textContent.trim()})");
  const state = JSON.parse(rendered);
  if (state.actor !== `Connected · ${expectedActor}` || state.active !== '0' || !state.empty.includes('No rotations')) {
    throw new Error(`unexpected authenticated UI state: ${rendered}`);
  }
  console.log(`PASS: Chrome authenticated as ${expectedActor}, loaded operator registry, and rendered the empty state`);
} finally {
  socket.close();
}
