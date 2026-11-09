// Disposable integration spike, not Central's Rust implementation.
import { spawn } from 'node:child_process';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
const root = path.dirname(fileURLToPath(import.meta.url));
const workspace = path.join(root, 'workspace');
const results = path.join(root, 'results');
await Promise.all([workspace, results].map(p => fs.mkdir(p, { recursive: true })));
const binary = process.env.CENTRAL_CODEX_BINARY ?? 'codex';
const model = 'gpt-6.1-sol';
const nonce = randomBytes(8).toString('hex');
const schemaPath = path.join(results, 'schema.json');
await fs.writeFile(schemaPath, JSON.stringify({ type: 'object', properties: {
  probe: { type: 'string' }, nonce: { type: 'string' }, sum: { type: 'integer' },
}, required: ['probe', 'nonce', 'sum'], additionalProperties: false }, null, 2));
const env = { ...process.env };
// Avoid attaching the child CLI to this desktop chat's execution context.
for (const key of Object.keys(env)) if (/^CODEX_/.test(key) && key !== 'CODEX_HOME') delete env[key];
const common = ['--ignore-user-config', '--ignore-rules', '--skip-git-repo-check',
  '--json', '--model', model, '-c', 'model_reasoning_effort="medium"',
  '-c', 'features.multi_agent=false', '--output-schema', schemaPath];
const report = { started: new Date().toISOString(), model, effort: 'medium',
  authentication: 'existing ChatGPT login; no credential copied', calls: [] };
async function call(label, args, prompt) {
  const started = Date.now();
  const answerPath = path.join(results, `${label}-answer.json`);
  const child = spawn(binary, [...args, '-o', answerPath, '-'], {
    cwd: workspace, env, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'],
  });
  let stdout = '', stderr = '', timedOut = false;
  child.stdout.on('data', b => { stdout += b; });
  child.stderr.on('data', b => { stderr += b; });
  child.stdin.on('error', () => {});
  child.stdin.end(prompt);
  const timer = setTimeout(() => {
    timedOut = true;
    const killer = spawn('taskkill', ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true });
    killer.on('error', () => {});
  }, 120000);
  const exit = await new Promise((resolve, reject) => { child.on('error', reject); child.on('exit', (code, signal) => resolve({ code, signal })); }).finally(() => clearTimeout(timer));
  await fs.writeFile(path.join(results, `${label}-events.jsonl`), stdout);
  await fs.writeFile(path.join(results, `${label}-stderr.log`), stderr);
  const events = stdout.split(/\r?\n/).filter(Boolean).map(line => { try { return JSON.parse(line); } catch { return { type: 'unparsed', text: line }; } });
  let answer; try { answer = JSON.parse(await fs.readFile(answerPath, 'utf8')); } catch {}
  const tools = events.filter(e => /item\./.test(e.type) && e.item && !['agent_message', 'reasoning'].includes(e.item.type));
  const entry = { label, ...exit, timedOut, elapsedMs: Date.now() - started,
    threadID: events.find(e => e.type === 'thread.started')?.thread_id,
    eventTypes: [...new Set(events.map(e => e.type))],
    usage: events.findLast(e => e.type === 'turn.completed')?.usage,
    answer, toolItems: tools.length,
    errors: events.filter(e => e.type === 'error' || e.type === 'turn.failed'),
    passed: exit.code === 0 && answer?.probe === 'central-live' && answer?.nonce === nonce && answer?.sum === 42 && tools.length === 0,
  };
  report.calls.push(entry);
  console.log(JSON.stringify(entry));
  return entry;
}
try {
  const first = await call('initial', ['exec', '--sandbox', 'read-only', ...common],
    `This is a disposable minimal protocol probe. For this probe do not read files, load skills, run any tools or delegate. Reply only with a JSON object: {"probe":"central-live","nonce":"${nonce}","sum":42}. Remember the nonce in this conversation.`);
  if (!first.passed || !first.threadID) throw new Error('Initial model call did not satisfy the probe; inspect saved events.');
  const second = await call('resume', ['exec', '--sandbox', 'read-only', 'resume', ...common, first.threadID],
    'Continue this same protocol probe. Do not use tools, files, skills or delegation. Return the same JSON object as before, including the original nonce from conversation history.');
  if (!second.passed || second.threadID !== first.threadID) throw new Error('Session resume did not satisfy the probe.');
  report.passed = true;
} catch (error) { report.passed = false; report.error = error.message; console.error(error.message); process.exitCode = 1; }
finally {
  report.finished = new Date().toISOString();
  await fs.writeFile(path.join(results, 'report.json'), JSON.stringify(report, null, 2));
}
