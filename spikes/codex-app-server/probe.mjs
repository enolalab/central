// Disposable client protocol investigation. Not Central or a production adapter.
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes, createHash } from 'node:crypto';
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const WebSocket = require('./vendor/ws/package');
const root = path.dirname(fileURLToPath(import.meta.url));
const runID = new Date().toISOString().replace(/[:.]/g, '-');
const results = path.join(root, 'results', runID);
const cwd = path.join(root, 'workspace', runID);
await Promise.all([results, cwd].map(p => fs.mkdir(p, { recursive: true })));
const sleep = ms => new Promise(r => setTimeout(r, ms));
const nonce = randomBytes(8).toString('hex');
const helper = path.join(cwd, 'approval-helper.cjs');
await fs.writeFile(helper, `require('node:fs').writeFileSync('approved.txt', '${nonce}');console.log('APPROVAL_PROBE_DONE');\n`);
const token = randomBytes(32).toString('hex');
const listener = createServer();
await new Promise(r => listener.listen(0, '127.0.0.1', r));
const port = listener.address().port;
await new Promise(r => listener.close(r));
const url = `ws://127.0.0.1:${port}`;
const env = { ...process.env };
for (const key of Object.keys(env)) if (/^CODEX_/.test(key) && key !== 'CODEX_HOME') delete env[key];
const child = spawn(env.CENTRAL_CODEX_BINARY ?? 'codex', ['app-server', '--listen', url,
  '--ws-auth', 'capability-token', '--ws-token-sha256', createHash('sha256').update(token).digest('hex'),
  '-c', 'windows.sandbox="elevated"', '-c', 'features.multi_agent=false',
  '-c', 'project_doc_max_bytes=0', '-c', 'skills.max_context_tokens=1', '-c', 'skills.include_instructions=false',
  '-c', 'mcp_servers={}', '-c', 'analytics.enabled=false'], {
  cwd, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
});
let serverLog = '';
child.stdout.on('data', b => { serverLog += b; }); child.stderr.on('data', b => { serverLog += b; });
const report = { started: new Date().toISOString(), runID, model: 'gpt-6-luna', effort: 'low',
  transport: 'loopback WebSocket, capability-token auth', checks: [], cases: [] };
const traces = [], clients = [];
let nextID = 100;
class RPC {
  constructor(label) { this.label = label; this.messages = []; this.pending = new Map(); this.waiters = []; }
  async connect() {
    this.ws = new WebSocket(url, { headers: { Authorization: `Bearer ${token}` } });
    this.ws.on('message', data => {
      const m = JSON.parse(data.toString()); this.messages.push(m); traces.push({ client: this.label, direction: 'received', message: m });
      if (m.id !== undefined && !m.method) {
        const p = this.pending.get(m.id); if (p) { this.pending.delete(m.id); clearTimeout(p.timer); m.error ? p.reject(new Error(JSON.stringify(m.error))) : p.resolve(m.result); }
      }
      for (const w of [...this.waiters]) if (w.predicate(m)) { clearTimeout(w.timer); this.waiters.splice(this.waiters.indexOf(w), 1); w.resolve(m); }
    });
    await new Promise((resolve, reject) => { const timer = setTimeout(() => reject(new Error('WS connect timeout')), 10000); this.ws.once('open', () => { clearTimeout(timer); resolve(); }); this.ws.once('error', e => { clearTimeout(timer); reject(e); }); });
    this.ws.on('error', () => {});
    clients.push(this);
    const init = await this.call('initialize', { clientInfo: { name: 'central_protocol_spike', title: 'Central protocol spike', version: '0.0.1' }, capabilities: { experimentalApi: true } });
    this.send({ method: 'initialized', params: {} });
    report.serverInfo ??= init;
    return this;
  }
  send(m) { traces.push({ client: this.label, direction: 'sent', message: m }); this.ws.send(JSON.stringify(m)); }
  call(method, params, timeout = 20000) {
    const id = nextID++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`${method} timed out`)); }, timeout);
      this.pending.set(id, { resolve, reject, timer }); this.send({ id, method, params });
    });
  }
  wait(predicate, timeout = 60000) {
    const existing = this.messages.find(predicate); if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const w = { predicate, resolve, timer: setTimeout(() => { this.waiters.splice(this.waiters.indexOf(w), 1); reject(new Error(`Event wait timed out on ${this.label}`)); }, timeout) };
      this.waiters.push(w);
    });
  }
  async close() {
    if (this.ws.readyState === WebSocket.CLOSED) return;
    const done = new Promise(r => this.ws.once('close', r)); this.ws.close(); await Promise.race([done, sleep(1000)]);
    this.ws.terminate();
  }
}
async function thread(client, policy) {
  return client.call('thread/start', { model: report.model, cwd, sandbox: 'workspace-write', approvalPolicy: policy,
    approvalsReviewer: 'user', ephemeral: false, experimentalRawEvents: false,
    config: { model_reasoning_effort: 'low' } });
}
async function reconnectRequest(oldClient, request, threadID, method, observation) {
  observation.beforeDisconnect = (await oldClient.call('thread/read', { threadId: threadID, includeTurns: false })).thread.status;
  await oldClient.close();
  await sleep(500);
  const replacement = await new RPC(oldClient.label + '-reconnected').connect();
  const resumed = await replacement.call('thread/resume', { threadId: threadID, excludeTurns: true });
  observation.afterReconnect = resumed.thread.status;
  observation.sameThreadID = resumed.thread.id === threadID;
  let replay;
  try { replay = await replacement.wait(m => m.method === method && m.params?.threadId === threadID, 3000); } catch {}
  observation.requestReplayed = Boolean(replay);
  observation.originalRequestID = request.id;
  observation.replayedRequestID = replay?.id;
  console.log(JSON.stringify({ stage: 'reconnected', case: observation.kind, ...observation }));
  return { client: replacement, request: replay ?? request };
}
try {
  let healthy = false;
  for (let i = 0; i < 60; i++) { await sleep(250); try { const r = await fetch(`http://127.0.0.1:${port}/readyz`, { signal: AbortSignal.timeout(1000) }); if (r.ok) { healthy = true; break; } } catch {} if (child.exitCode !== null) break; }
  if (!healthy) throw new Error('App server failed to start: ' + serverLog.slice(-1500));
  const first = await new RPC('approval').connect();
  const { thread: approvalThread } = await thread(first, 'untrusted');
  report.approvalThreadID = approvalThread.id;
  const command = `& '${process.execPath.replaceAll("'", "''")}' '${helper.replaceAll("'", "''")}'`;
  const { turn: approvalTurn } = await first.call('turn/start', { threadId: approvalThread.id, effort: 'low',
    input: [{ type: 'text', text: `Disposable approval protocol test. Skip skills/delegation. Run exactly this harmless local command once, then reply DONE:\n${command}`, text_elements: [] }] });
  const approvalRequest = await first.wait(m => m.method === 'item/commandExecution/requestApproval' && m.params?.threadId === approvalThread.id);
  const approvalCase = { kind: 'command-approval', turnID: approvalTurn.id, initialMethod: approvalRequest.method };
  report.cases.push(approvalCase);
  const markerBefore = await fs.readFile(path.join(cwd, 'approved.txt'), 'utf8').catch(() => null);
  approvalCase.noSideEffectBeforeApproval = markerBefore === null;
  const replacement = await reconnectRequest(first, approvalRequest, approvalThread.id, approvalRequest.method, approvalCase);
  const preview = replacement.request.params?.command ?? '';
  if (!preview.includes('approval-helper.cjs') || /[;|]/.test(preview) || replacement.request.params?.cwd !== cwd) throw new Error('Unexpected command approval; refusing to accept');
  replacement.client.send({ id: replacement.request.id, result: { decision: 'accept' } });
  const approvalDone = await replacement.client.wait(m => m.method === 'turn/completed' && m.params?.turn?.id === approvalTurn.id, 60000);
  approvalCase.finalTurnStatus = approvalDone.params.turn.status;
  approvalCase.markerMatches = await fs.readFile(path.join(cwd, 'approved.txt'), 'utf8').catch(() => null) === nonce;
  approvalCase.resolutionObserved = replacement.client.messages.some(m => m.method === 'serverRequest/resolved');
  approvalCase.usage = replacement.client.messages.findLast(m => m.method === 'thread/tokenUsage/updated' && m.params?.threadId === approvalThread.id)?.params.tokenUsage;
  approvalCase.passed = approvalCase.noSideEffectBeforeApproval && approvalCase.markerMatches && approvalCase.finalTurnStatus === 'completed' && approvalCase.requestReplayed && approvalCase.sameThreadID && approvalCase.resolutionObserved;
  console.log(JSON.stringify(approvalCase));
  const questionClient = await new RPC('question').connect();
  const { thread: questionThread } = await thread(questionClient, 'on-request');
  report.questionThreadID = questionThread.id;
  const { turn: questionTurn } = await questionClient.call('turn/start', { threadId: questionThread.id,
    collaborationMode: { mode: 'plan', settings: { model: report.model, reasoning_effort: 'low', developer_instructions: 'For this disposable test ask the requested structured question using request_user_input; after the answer, return the selected label. Do not inspect files, use shell, load skills, or delegate.' } },
    input: [{ type: 'text', text: 'Use request_user_input now to ask which test label to use: Alpha or Beta. Do not choose for me. After my answer reply only with the chosen label.', text_elements: [] }] });
  const questionRequest = await questionClient.wait(m => m.method === 'item/tool/requestUserInput' && m.params?.threadId === questionThread.id);
  const questionCase = { kind: 'user-question', turnID: questionTurn.id, initialMethod: questionRequest.method, questions: questionRequest.params.questions };
  report.cases.push(questionCase);
  const qReplacement = await reconnectRequest(questionClient, questionRequest, questionThread.id, questionRequest.method, questionCase);
  const questions = qReplacement.request.params.questions;
  qReplacement.client.send({ id: qReplacement.request.id, result: { answers: Object.fromEntries(questions.map(q => [q.id, { answers: ['Alpha'] }])) } });
  const questionDone = await qReplacement.client.wait(m => m.method === 'turn/completed' && m.params?.turn?.id === questionTurn.id, 60000);
  questionCase.finalTurnStatus = questionDone.params.turn.status;
  questionCase.resolutionObserved = qReplacement.client.messages.some(m => m.method === 'serverRequest/resolved');
  const finalMessages = qReplacement.client.messages.filter(m => m.method === 'item/completed' && m.params?.item?.type === 'agentMessage').map(m => m.params.item.text);
  questionCase.finalMessages = finalMessages;
  questionCase.usage = qReplacement.client.messages.findLast(m => m.method === 'thread/tokenUsage/updated' && m.params?.threadId === questionThread.id)?.params.tokenUsage;
  questionCase.passed = questionCase.finalTurnStatus === 'completed' && finalMessages.some(s => /Alpha/i.test(s)) && questionCase.requestReplayed && questionCase.sameThreadID && questionCase.resolutionObserved;
  console.log(JSON.stringify(questionCase));
  report.passed = report.cases.every(c => c.passed);
  if (!report.passed) process.exitCode = 1;
} catch (error) { report.error = error.stack; report.passed = false; console.error(error.message); process.exitCode = 1; }
finally {
  for (const c of clients) await c.close().catch(() => {});
  if (child.exitCode === null) await new Promise(resolve => {
    const killer = spawn('taskkill', ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true }); killer.on('exit', resolve); killer.on('error', resolve);
  });
  report.finished = new Date().toISOString();
  await fs.writeFile(path.join(results, 'report.json'), JSON.stringify(report, null, 2));
  await fs.writeFile(path.join(results, 'rpc.jsonl'), traces.map(m => JSON.stringify(m)).join('\n') + '\n');
  await fs.writeFile(path.join(results, 'server.log'), serverLog);
  console.log(JSON.stringify({ passed: report.passed, reportPath: path.join(results, 'report.json') }));
}
