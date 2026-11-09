// Throwaway crash/reconciliation probe. Two real calls, Luna/low; no product code.
import { spawn } from 'node:child_process';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes } from 'node:crypto';
const root = path.dirname(fileURLToPath(import.meta.url));
const runID = new Date().toISOString().replace(/[:.]/g, '-');
const workspace = path.join(root, 'workspace', 'crash-' + runID);
const results = path.join(root, 'results', 'crash-' + runID);
await Promise.all([workspace, results].map(p => fs.mkdir(p, { recursive: true })));
const nonce = randomBytes(8).toString('hex');
await fs.writeFile(path.join(workspace, 'input.json'), JSON.stringify({ nonce }));
const helper = path.join(workspace, 'worker.cjs');
await fs.writeFile(helper, `const fs=require('node:fs');
const input=JSON.parse(fs.readFileSync('input.json'));
const read=()=>{try{return JSON.parse(fs.readFileSync('checkpoint.json'))}catch{return null}};
const save=p=>fs.writeFileSync('checkpoint.json',JSON.stringify({nonce:input.nonce,phase:p,pid:process.pid}));
const log=s=>fs.appendFileSync('actions.log',s+'\\n');
const finish=()=>{log('FINISH');save('done');console.log(JSON.stringify(read()))};
const mode=process.argv[2];
if(mode==='inspect'){const checkpoint=read();let alive=false;if(checkpoint?.phase==='started'){try{process.kill(checkpoint.pid,0);alive=true}catch{}}console.log(JSON.stringify({checkpoint,alive,actions:fs.existsSync('actions.log')?fs.readFileSync('actions.log','utf8'):''}))}
else if(mode==='run'){log('START');save('started');console.log('CHECKPOINT_SAVED');setTimeout(finish,20000)}
else if(mode==='finish'){const c=read();if(c?.phase==='done')console.log(JSON.stringify(c));else{if(c?.phase==='started'){try{process.kill(c.pid,0);throw Error('Old worker alive; do not duplicate')}catch(e){if(e.message.includes('alive'))throw e}}log('RECOVER');finish()}}
else throw Error('Unknown mode');
`);
const env = { ...process.env };
for (const key of Object.keys(env)) if (/^CODEX_/.test(key) && key !== 'CODEX_HOME') delete env[key];
const binary = env.CENTRAL_CODEX_BINARY ?? 'codex';
const report = { started: new Date().toISOString(), model: 'gpt-6-luna', effort: 'low', runID, calls: [] };
const common = ['--ignore-user-config', '--ignore-rules', '--skip-git-repo-check', '--json', '-m', report.model,
  '-c', 'model_reasoning_effort="low"', '-c', 'features.multi_agent=false', '-c', 'project_doc_max_bytes=0',
  '-c', 'windows.sandbox="elevated"', '-c', 'skills.max_context_tokens=1', '-c', 'skills.include_instructions=false'];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const command = mode => `& '${process.execPath.replaceAll("'", "''")}' '${helper.replaceAll("'", "''")}' ${mode}`;
const readCheckpoint = async () => JSON.parse(await fs.readFile(path.join(workspace, 'checkpoint.json'), 'utf8'));
function launch(label, args, prompt) {
  const child = spawn(binary, [...args, '-o', path.join(results, label + '-answer.txt'), '-'], {
    cwd: workspace, env, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'],
  });
  let stdout = '', stderr = '';
  child.stdout.on('data', b => { stdout += b; }); child.stderr.on('data', b => { stderr += b; });
  child.stdin.on('error', () => {}); child.stdin.end(prompt);
  const done = new Promise((resolve, reject) => { child.on('error', reject); child.on('exit', (code, signal) => resolve({ code, signal })); });
  const timer = setTimeout(() => {
    const killer = spawn('taskkill', ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true });
    killer.on('error', () => {});
  }, 120000);
  async function collect() {
    let exit; try { exit = await done; } finally { clearTimeout(timer); }
    await fs.writeFile(path.join(results, label + '-events.jsonl'), stdout);
    await fs.writeFile(path.join(results, label + '-stderr.log'), stderr);
    const events = stdout.split(/\r?\n/).filter(Boolean).map(line => { try { return JSON.parse(line); } catch { return { type: 'unparsed' }; } });
    const entry = { label, ...exit, threadID: events.find(e => e.type === 'thread.started')?.thread_id,
      eventTypes: [...new Set(events.map(e => e.type))],
      usage: events.findLast(e => e.type === 'turn.completed')?.usage ?? null,
      commandEvents: events.filter(e => e.item?.type === 'command_execution'),
      errors: events.filter(e => ['error', 'turn.failed'].includes(e.type)),
      answer: await fs.readFile(path.join(results, label + '-answer.txt'), 'utf8').catch(() => null),
    };
    report.calls.push(entry); console.log(JSON.stringify({ label, ...exit, threadID: entry.threadID, usage: entry.usage, errors: entry.errors }));
    return entry;
  }
  return { child, done, collect };
}
let active;
try {
  active = launch('interrupted', ['--ask-for-approval', 'never', 'exec', '--sandbox', 'workspace-write', ...common],
    `Disposable crash test. Skip skills and delegation. Execute exactly this PowerShell command once, wait for completion, then reply DONE. Do not inspect anything else:\n${command('run')}`);
  let checkpoint;
  for (let i = 0; i < 900; i++) {
    try { checkpoint = await readCheckpoint(); if (checkpoint.phase === 'started') break; } catch {}
    if (active.child.exitCode !== null) break;
    await sleep(100);
  }
  if (checkpoint?.phase !== 'started') {
    if (active.child.exitCode === null) active.child.kill('SIGKILL');
    await active.collect(); active = undefined;
    throw new Error('No in-flight checkpoint observed; not a successful crash test. Inspect events/stderr.');
  }
  report.beforeCrash = checkpoint;
  active.child.kill('SIGKILL'); // Kill only this CLI, deliberately not its process tree.
  const interrupted = await active.collect(); active = undefined;
  if (!interrupted.threadID) throw new Error('No session ID captured before crash');
  let workerAlive = false; try { process.kill(checkpoint.pid, 0); workerAlive = true; } catch {}
  report.immediatelyAfterCrash = { workerAlive, checkpoint: await readCheckpoint() };
  console.log(JSON.stringify({ phase: 'crashed-during-tool', workerAlive, checkpointPID: checkpoint.pid }));
  // Reconcile external work before issuing a second agent call.
  await sleep(22000);
  workerAlive = false; try { process.kill(checkpoint.pid, 0); workerAlive = true; } catch {}
  report.beforeResume = { workerAlive, checkpoint: await readCheckpoint(), actions: await fs.readFile(path.join(workspace, 'actions.log'), 'utf8') };
  if (workerAlive) throw new Error('Old worker still alive; refuse duplicate attempt');
  active = launch('resumed', ['--ask-for-approval', 'never', 'exec', '--sandbox', 'workspace-write', 'resume', ...common, interrupted.threadID],
    `The previous CLI was force-killed during the tool. Skip skills/delegation. First inspect actual state with:\n${command('inspect')}\nIf phase is done, do NOT rerun. If started and alive=false, recover only with:\n${command('finish')}\nNever run the original command again. Reply with a short JSON containing nonce, phase, and whether you recovered.`);
  const resumed = await active.collect(); active = undefined;
  const final = await readCheckpoint();
  const actions = (await fs.readFile(path.join(workspace, 'actions.log'), 'utf8')).trim().split('\n');
  report.final = { checkpoint: final, actions };
  report.passed = resumed.code === 0 && resumed.threadID === interrupted.threadID &&
    final.nonce === nonce && final.phase === 'done' && actions.filter(x => x === 'START').length === 1 && actions.filter(x => x === 'FINISH').length === 1;
  if (!report.passed) throw new Error('Resume/side-effect checks failed');
} catch (error) { report.passed = false; report.error = error.message; console.error(error.message); process.exitCode = 1; }
finally {
  if (active?.child.exitCode === null) { active.child.kill('SIGKILL'); await active.collect().catch(() => {}); }
  try {
    const c = await readCheckpoint();
    if (c.phase !== 'done') { try { process.kill(c.pid, 0); process.kill(c.pid, 'SIGKILL'); } catch {} }
  } catch {}
  report.finished = new Date().toISOString();
  await fs.writeFile(path.join(results, 'report.json'), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ passed: report.passed, reportPath: path.join(results, 'report.json') }));
}
