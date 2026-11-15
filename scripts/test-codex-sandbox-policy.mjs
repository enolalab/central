#!/usr/bin/env node
// Inspect the effective app-server sandbox without starting a model turn.
// Pass the native Codex executable, not a shell wrapper (.cmd/.ps1).
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { resolve } from 'node:path';

const values = new Map();
for (let i = 2; i < process.argv.length; i += 2) {
  const key = process.argv[i];
  if (!['--binary', '--cwd', '--model', '--windows-sandbox'].includes(key) ||
      values.has(key) || !process.argv[i + 1]) {
    throw new Error('Usage: node test-codex-sandbox-policy.mjs --binary <native-exe> --cwd <fixture-dir> [--model <model>] [--windows-sandbox elevated|unelevated]');
  }
  values.set(key, process.argv[i + 1]);
}
if (!values.has('--binary') || !values.has('--cwd')) throw new Error('Both --binary and --cwd are required.');
const mode = values.get('--windows-sandbox');
if (mode && !['elevated', 'unelevated'].includes(mode)) throw new Error('Invalid Windows sandbox mode.');
const model = values.get('--model') || 'gpt-6-luna';
const cwd = resolve(values.get('--cwd'));
const args = mode ? ['-c', `windows.sandbox="${mode}"`, 'app-server'] : ['app-server'];
const child = spawn(values.get('--binary'), args, { cwd, shell: false, stdio: ['pipe', 'pipe', 'pipe'] });
let finished = false;
const timer = setTimeout(() => finish({ error: 'Timed out waiting for app-server response.' }, 1), 30000);
function finish(result, code) {
  if (finished) return;
  finished = true;
  clearTimeout(timer);
  console.log(JSON.stringify({ ...result, model_turns: 0 }));
  child.kill();
  process.exitCode = code;
}
function send(message) { child.stdin.write(`${JSON.stringify(message)}\n`); }
child.stderr.resume(); // Do not publish process stderr, which may contain personal data.
child.on('error', () => finish({ error: 'Could not start native Codex executable.' }, 1));
child.on('exit', code => { if (!finished) finish({ error: 'App-server exited early.', exit_code: code }, 1); });
child.stdin.on('error', () => finish({ error: 'App-server stdin closed.' }, 1));
createInterface({ input: child.stdout }).on('line', line => {
  let message;
  try { message = JSON.parse(line); } catch { return; }
  if (message.id === 1) {
    if (message.error || !message.result) return finish({ error: 'Initialize failed.' }, 1);
    send({ method: 'initialized', params: {} });
    send({ id: 2, method: 'thread/start', params: {
      cwd, model, sandbox: 'workspace-write', approvalPolicy: 'on-request',
      ephemeral: true, allowProviderModelFallback: false,
      config: { model_reasoning_effort: 'low' },
    } });
  } else if (message.id === 2) {
    if (message.error || !message.result) return finish({ error: 'thread/start failed.' }, 1);
    const actual = message.result.sandbox?.type ?? null;
    const observedModel = message.result.model ?? null;
    const passed = actual === 'workspaceWrite' && observedModel === model;
    finish({ requested_model: model, observed_model: observedModel,
      windows_sandbox_override: mode || 'current', effective_sandbox: actual,
      cli_version: message.result.thread?.cliVersion ?? null, passed }, passed ? 0 : 1);
  }
});
send({ id: 1, method: 'initialize', params: {
  clientInfo: { name: 'central-sandbox-preflight', title: 'Central sandbox preflight', version: '0.1' },
  capabilities: { experimentalApi: true },
} });
