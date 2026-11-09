// Throwaway real-model probe. Isolated OpenCode state; never edits user config.
import { spawn } from 'node:child_process';
import { promises as fs } from 'node:fs';
import { createServer } from 'node:net';
import { randomBytes } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root = path.dirname(fileURLToPath(import.meta.url));
const modelArg = process.argv.find(a => a.startsWith('--model='))?.slice('--model='.length);
const reportName = modelArg ? `live-${modelArg.replace(/[^a-zA-Z0-9_-]/g, '-')}-report.json` : 'live-report.json';
const state = path.join(root, 'state/live');
const cwd = path.join(root, 'workspace/live');
await fs.mkdir(cwd, { recursive: true });
const env = { ...process.env };
for (const key of Object.keys(env)) if (/^OPENCODE_|^XDG_/.test(key)) delete env[key];
const password = randomBytes(24).toString('hex');
Object.assign(env, {
  XDG_CONFIG_HOME: path.join(state, 'config'), XDG_DATA_HOME: path.join(state, 'data'),
  XDG_CACHE_HOME: path.join(state, 'cache'), XDG_STATE_HOME: path.join(state, 'runtime'),
  OPENCODE_SERVER_PASSWORD: password, OPENCODE_DISABLE_AUTOUPDATE: 'true',
  OPENCODE_DISABLE_CLAUDE_CODE: 'true',
  OPENCODE_CONFIG_CONTENT: JSON.stringify({ permission: { '*': 'deny' } }),
});
const listener = createServer();
await new Promise(r => listener.listen(0, '127.0.0.1', r));
const port = listener.address().port;
await new Promise(r => listener.close(r));
const base = `http://127.0.0.1:${port}`;
const authorization = 'Basic ' + Buffer.from('opencode:' + password).toString('base64');
const child = spawn(path.join(root, 'vendor/package/bin/opencode.exe'), ['serve', '--hostname', '127.0.0.1', '--port', String(port)],
  { env, cwd, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
let serverLog = '';
child.stdout.on('data', b => { serverLog += b; }); child.stderr.on('data', b => { serverLog += b; });
const sleep = ms => new Promise(r => setTimeout(r, ms));
const report = { started: new Date().toISOString(), modelCalls: [], isolatedState: true, toolsDenied: true };
async function api(route, method = 'GET', body, timeout = 15000, signal) {
  const response = await fetch(base + route, { method, headers: { Authorization: authorization, 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body), signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(timeout)]) : AbortSignal.timeout(timeout) });
  const raw = await response.text(); let value; try { value = JSON.parse(raw); } catch { value = raw.slice(0, 1000); }
  if (!response.ok) throw new Error(`${method} ${route}: ${response.status} ${JSON.stringify(value).slice(0, 1000)}`);
  return value;
}
try {
  let healthy = false;
  for (let i = 0; i < 60; i++) { await sleep(500); try { report.health = await api('/global/health'); healthy = true; break; } catch { if (child.exitCode !== null) break; } }
  if (!healthy) throw new Error('Live server did not start: ' + serverLog.slice(-1000));
  const providers = await api('/provider', 'GET', undefined, 45000);
  const freeModels = providers.all.filter(p => p.id === 'opencode' && providers.connected.includes(p.id)).flatMap(p => Object.values(p.models ?? {}).filter(m => m.cost?.input === 0 && m.cost?.output === 0)
    .map(m => ({ providerID: p.id, modelID: m.id, name: m.name })));
  report.availableFreeModels = freeModels;
  console.log(JSON.stringify({ freeModels, connectedProviders: providers.connected }));
  if (process.argv.includes('--discover')) { /* discovery only */ }
  else {
    const selected = modelArg ? freeModels.find(m => m.modelID === modelArg) :
      freeModels.find(m => m.providerID === 'opencode' && /free/.test(m.modelID)) ?? freeModels.find(m => m.providerID === 'opencode');
    if (!selected) throw new Error('No unauthenticated zero-cost OpenCode model available; need user-selected provider credentials.');
    report.selectedModel = selected;
    const session = await api('/session', 'POST', { title: 'Central real inference probe' });
    report.sessionID = session.id;
    const nonce = randomBytes(8).toString('hex');
    const prompt = `This is a minimal API integration test. Do not use tools or read files. Reply with exactly one JSON object: {"probe":"central-live","nonce":"${nonce}","sum":42}. No Markdown.`;
    const started = Date.now();
    const attempt = { status: 'submitted', selectedModel: selected, submitted: new Date().toISOString() };
    report.modelCalls.push(attempt);
    const quotaAbort = new AbortController();
    const guard = setInterval(async () => {
      const log = await fs.readFile(path.join(state, 'data/opencode/log/opencode.log'), 'utf8').catch(() => '');
      if (log.split('\n').some(line => line.includes(session.id) && line.includes('Free usage limit reached'))) {
        report.providerError = 'Free usage limit reached';
        quotaAbort.abort(new Error('Provider quota exhausted; stopped without waiting for retries'));
      }
    }, 500);
    let result;
    try { result = await api(`/session/${session.id}/message`, 'POST', {
      model: { providerID: selected.providerID, modelID: selected.modelID },
      parts: [{ type: 'text', text: prompt }],
    }, 120000, quotaAbort.signal); } finally { clearInterval(guard); }
    const output = (result.parts ?? []).filter(p => p.type === 'text').map(p => p.text).join('\n');
    let parsed; try { parsed = JSON.parse(output); } catch {}
    const passed = parsed?.probe === 'central-live' && parsed?.nonce === nonce && parsed?.sum === 42 && !result.info?.error;
    Object.assign(attempt, { status: passed ? 'succeeded' : 'failed', passed, elapsedMs: Date.now() - started, output,
      error: result.info?.error, actualModel: result.info?.modelID, actualProvider: result.info?.providerID,
      tokens: result.info?.tokens, reportedCost: result.info?.cost,
      toolCalls: (result.parts ?? []).filter(p => p.type === 'tool').length });
    console.log(JSON.stringify(report.modelCalls.at(-1)));
    if (!passed) process.exitCode = 1;
  }
} catch (error) {
  report.error = error.message;
  if (report.modelCalls.at(-1)?.status === 'submitted') Object.assign(report.modelCalls.at(-1), { status: 'failed', transportError: error.message });
  const log = await fs.readFile(path.join(state, 'data/opencode/log/opencode.log'), 'utf8').catch(() => '');
  if (report.sessionID && log.split('\n').some(line => line.includes(report.sessionID) && line.includes('Free usage limit reached'))) report.providerError = 'Free usage limit reached';
  console.error(error.message); process.exitCode = 1;
}
finally {
  if (child.exitCode === null) await new Promise(resolve => {
    const killer = spawn('taskkill', ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true });
    killer.on('exit', resolve); killer.on('error', resolve);
  });
  report.finished = new Date().toISOString();
  await fs.mkdir(path.join(root, 'results'), { recursive: true });
  await fs.writeFile(path.join(root, 'results', reportName), JSON.stringify(report, null, 2));
}
