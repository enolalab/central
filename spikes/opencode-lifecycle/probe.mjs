// Throwaway protocol spike, not Central product code. No model calls or credentials.
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';

const root = path.dirname(fileURLToPath(import.meta.url));
const workspace = path.join(root, 'workspace');
const results = path.join(root, 'results');
const state = path.join(root, 'state');
await Promise.all([workspace, results, state].map(p => fs.mkdir(p, { recursive: true })));
const binary = path.join(root, 'vendor/package/bin/opencode.exe');
const password = randomBytes(24).toString('hex');
const auth = 'Basic ' + Buffer.from('opencode:' + password).toString('base64');
const env = { ...process.env };
for (const key of Object.keys(env)) {
  if (/API_KEY|TOKEN|SECRET|PASSWORD|^OPENCODE_|^XDG_/.test(key)) delete env[key];
}
Object.assign(env, {
  XDG_CONFIG_HOME: path.join(state, 'config'),
  XDG_DATA_HOME: path.join(state, 'data'),
  XDG_CACHE_HOME: path.join(state, 'cache'),
  XDG_STATE_HOME: path.join(state, 'runtime'),
  OPENCODE_SERVER_PASSWORD: password,
  OPENCODE_DISABLE_MODELS_FETCH: 'true',
  OPENCODE_DISABLE_AUTOUPDATE: 'true',
  OPENCODE_DISABLE_CLAUDE_CODE: 'true',
  OPENCODE_CONFIG_CONTENT: JSON.stringify({
    model: 'probe/probe', enabled_providers: ['probe'],
    provider: { probe: { npm: '@ai-sdk/openai-compatible', name: 'Local probe (no model)',
      options: { baseURL: 'http://127.0.0.1:1/v1', apiKey: 'probe-not-a-secret' },
      models: { probe: { name: 'Protocol probe', limit: { context: 4096, output: 128 } } } } },
  }),
});
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const report = { version: '1.19.5', started: new Date().toISOString(), modelCalls: false, checks: [] };
let child, base, generation = 0;
function check(name, passed, evidence) {
  report.checks.push({ name, passed, evidence });
  console.log(JSON.stringify({ name, passed, evidence }));
}
async function request(route, method = 'GET', body) {
  const r = await fetch(base + route, { method, headers: { Authorization: auth, 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(15000) });
  const raw = await r.text();
  let data; try { data = JSON.parse(raw); } catch { data = raw.slice(0, 500); }
  if (!r.ok) throw new Error(`${method} ${route}: ${r.status} ${JSON.stringify(data).slice(0, 800)}`);
  return data;
}
async function start() {
  const socket = createServer();
  await new Promise(resolve => socket.listen(0, '127.0.0.1', resolve));
  const port = socket.address().port;
  await new Promise(resolve => socket.close(resolve));
  base = `http://127.0.0.1:${port}`;
  child = spawn(binary, ['serve', '--hostname', '127.0.0.1', '--port', String(port)], {
    cwd: workspace, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
  });
  let log = '';
  const logPath = path.join(results, `server-${++generation}.log`);
  child.stdout.on('data', b => { log += b; });
  child.stderr.on('data', b => { log += b; });
  child.on('error', e => { log += e.message; });
  for (let i = 0; i < 60; i++) {
    await sleep(500);
    try { const health = await request('/global/health'); await fs.writeFile(logPath, log); return health; }
    catch { if (child.exitCode !== null) break; }
  }
  await fs.writeFile(logPath, log);
  throw new Error('Server did not become healthy: ' + log.slice(-1500));
}
async function crash() {
  if (!child || child.exitCode !== null) return;
  const victim = child;
  await new Promise((resolve, reject) => {
    const killer = spawn('taskkill', ['/PID', String(victim.pid), '/T', '/F'], { windowsHide: true });
    killer.on('error', reject); killer.on('exit', resolve);
  });
  await Promise.race([new Promise(resolve => victim.once('exit', resolve)), sleep(1500)]);
  child = undefined;
}
async function listen(lastEventID) {
  const controller = new AbortController();
  const headers = { Authorization: auth };
  if (lastEventID) headers['Last-Event-ID'] = lastEventID;
  const response = await fetch(base + '/event', { headers, signal: controller.signal });
  if (!response.ok) throw new Error('SSE status ' + response.status);
  const packets = [];
  const done = (async () => {
    let buffer = '';
    try {
      for await (const chunk of response.body) {
        buffer += new TextDecoder().decode(chunk);
        buffer = buffer.replaceAll('\r\n', '\n');
        let idx;
        while ((idx = buffer.indexOf('\n\n')) >= 0) {
          const raw = buffer.slice(0, idx); buffer = buffer.slice(idx + 2);
          const text = raw.split('\n').filter(x => x.startsWith('data:')).map(x => x.slice(5).trim()).join('\n');
          try { packets.push({ id: raw.match(/^id:\s*(.*)$/m)?.[1] ?? null, data: JSON.parse(text) }); } catch {}
        }
      }
    } catch (error) { if (!controller.signal.aborted) throw error; }
  })();
  return { packets, close: async () => { controller.abort(); await done; } };
}
try {
  check('server-start', true, await start());
  const schema = await request('/doc');
  await fs.writeFile(path.join(results, 'openapi.json'), JSON.stringify(schema, null, 2));
  const events = await listen();
  const session = await request('/session', 'POST', { title: 'Central lifecycle probe' });
  check('session-create', typeof session.id === 'string', { id: session.id });
  await request(`/session/${session.id}/message`, 'POST', {
    noReply: true, parts: [{ type: 'text', text: 'CENTRAL_CHECKPOINT: saved before forced restart; no model response requested.' }],
  });
  await sleep(500);
  check('sse-session-event', events.packets.some(x => x.data.type === 'session.created'), {
    types: [...new Set(events.packets.map(x => x.data.type))], ids: events.packets.map(x => x.id),
  });
  await events.close();
  const gap = await request('/session', 'POST', { title: 'Created while subscriber disconnected' });
  const reconnected = await listen('central-probe-missing-event-id');
  await sleep(1000);
  check('sse-replays-gap-within-1s', reconnected.packets.some(x => x.data.properties?.info?.id === gap.id), {
    types: reconnected.packets.map(x => x.data.type), gapID: gap.id,
    caveat: 'Bounded observation with synthetic Last-Event-ID; not proof of all replay behavior.',
  });
  await reconnected.close();
  check('gap-recoverable-by-query', (await request('/session')).some(x => x.id === gap.id), {});
  await crash();
  await start();
  const restored = await request(`/session/${session.id}`);
  const messages = await request(`/session/${session.id}/message`);
  check('session-and-message-survive-forced-restart', restored.id === session.id && JSON.stringify(messages).includes('CENTRAL_CHECKPOINT'), {
    messageCount: messages.length, status: await request('/session/status'),
    caveat: 'Persisted idle session; does not establish interrupted model/tool continuation.',
  });
  const helper = path.join(workspace, 'long-task.cjs');
  await fs.writeFile(helper, `const fs = require('node:fs'); fs.writeFileSync('started.txt', String(process.pid)); setTimeout(() => { fs.writeFileSync('finished.txt', 'done'); }, 12000);\n`);
  for (const name of ['started.txt', 'finished.txt']) {
    await fs.unlink(path.join(workspace, name)).catch(e => { if (e.code !== 'ENOENT') throw e; });
  }
  // OpenCode selected PowerShell on this Windows host: quoted executable needs &.
  const command = `& '${process.execPath.replaceAll("'", "''")}' '${helper.replaceAll("'", "''")}'`;
  const shellPromise = request(`/session/${session.id}/shell`, 'POST', { agent: 'build', model: { providerID: 'probe', modelID: 'probe' }, command })
    .then(value => ({ ok: true, value }), error => ({ ok: false, error: error.message }));
  let started = false;
  for (let i = 0; i < 40; i++) {
    try { await fs.access(path.join(workspace, 'started.txt')); started = true; break; } catch {}
    await sleep(100);
  }
  if (started) {
    const response = await request(`/session/${session.id}/abort`, 'POST');
    const result = await shellPromise;
    await sleep(13000);
    let finished = false; try { await fs.access(path.join(workspace, 'finished.txt')); finished = true; } catch {}
    const pid = Number(await fs.readFile(path.join(workspace, 'started.txt'), 'utf8'));
    let alive = false; try { process.kill(pid, 0); alive = true; } catch {}
    check('abort-stops-controlled-shell-child', !finished && !alive, { abortResponse: response, finishedMarker: finished, childAlive: alive, shellResponseOK: result.ok });
  } else {
    await request(`/session/${session.id}/abort`, 'POST');
    check('abort-stops-controlled-shell-child', false, { reason: 'Shell did not create start marker', result: await shellPromise });
  }
  // Kill just the server, not its tree: distinguish daemon failure from abort.
  const interrupted = await request('/session', 'POST', { title: 'Interrupted in-flight shell' });
  const crashHelper = path.join(workspace, 'crash-task.cjs');
  for (const name of ['crash-started.txt', 'crash-finished.txt']) {
    await fs.unlink(path.join(workspace, name)).catch(e => { if (e.code !== 'ENOENT') throw e; });
  }
  await fs.writeFile(crashHelper, `const fs = require('node:fs'); fs.writeFileSync('crash-started.txt', String(process.pid)); setTimeout(() => fs.writeFileSync('crash-finished.txt', 'done'), 10000);\n`);
  const inflight = request(`/session/${interrupted.id}/shell`, 'POST', {
    agent: 'build', model: { providerID: 'probe', modelID: 'probe' },
    command: `& '${process.execPath.replaceAll("'", "''")}' '${crashHelper.replaceAll("'", "''")}'`,
  }).then(() => 'response received', error => error.message);
  let activePID;
  for (let i = 0; i < 40; i++) {
    try { activePID = Number(await fs.readFile(path.join(workspace, 'crash-started.txt'), 'utf8')); break; } catch {}
    await sleep(100);
  }
  if (!activePID) throw new Error('In-flight crash test did not start helper');
  child.kill('SIGKILL');
  await sleep(300);
  child = undefined;
  await start();
  const recoveredMessages = await request(`/session/${interrupted.id}/message`);
  const toolStates = recoveredMessages.flatMap(m => m.parts ?? []).filter(p => p.type === 'tool').map(p => p.state.status);
  const statuses = await request('/session/status');
  await sleep(11000);
  let crashFinished = false; try { await fs.access(path.join(workspace, 'crash-finished.txt')); crashFinished = true; } catch {}
  let helperAlive = false; try { process.kill(activePID, 0); helperAlive = true; } catch {}
  check('inflight-crash-reconciliation-observed', true, {
    recoveredToolStates: toolStates, reportedStatus: statuses[interrupted.id] ?? 'absent',
    sideEffectCompletedAfterServerDeath: crashFinished, helperAlive, transportOutcome: await inflight,
    caveat: 'Observation only; success means evidence collected, not automatic continuation or correctness.',
  });
  if (helperAlive) process.kill(activePID, 'SIGKILL');
} catch (error) {
  report.error = error.stack;
  console.error(error.stack);
  process.exitCode = 1;
} finally {
  await crash();
  report.finished = new Date().toISOString();
  await fs.writeFile(path.join(results, 'report.json'), JSON.stringify(report, null, 2));
}
