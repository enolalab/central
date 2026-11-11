// Independent browser client sketch for the Central public v1 API.
// fetch + a manual stream reader: never EventSource (it cannot send an
// Authorization header, and tokens must never go in the URL).
//
// The token lives in this module's memory only: it is never written to
// localStorage, never logged, and never interpolated into a URL.
// The page's origin must be in the owner's origins.json allowlist or
// every call (reads and streams) is 403.

const DAEMON = 'http://127.0.0.1:7878';

// Memory-only credential. Callers set it from a paste-once prompt.
let bearerToken = null;

export function setToken(token) {
  bearerToken = token;
}

export function clearToken() {
  bearerToken = null;
}

function authHeaders(extra = {}) {
  if (!bearerToken) throw new Error('no token set: call setToken() first');
  return { Authorization: `Bearer ${bearerToken}`, ...extra };
}

async function api(method, path, body) {
  const response = await fetch(`${DAEMON}${path}`, {
    method,
    headers: {
      ...authHeaders({ 'Content-Type': 'application/json' }),
      ...(body === undefined ? {} : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) {
    let code = `http-${response.status}`;
    try {
      code = JSON.parse(text).error.code;
    } catch {
      /* keep the http code */
    }
    throw new Error(`${method} ${path}: ${code}`);
  }
  return JSON.parse(text);
}

export async function discover() {
  const meta = await api('GET', '/v1/meta');
  if (meta.api !== 'v1') throw new Error(`daemon API mismatch: ${meta.api}`);
  return meta;
}

export async function snapshot(runId) {
  return api('GET', `/v1/runs/${runId}`);
}

export async function eventsAfter(runId, after) {
  return api('GET', `/v1/runs/${runId}/events?after=${after}`);
}

// Stream events with durable-seq resume. Yields parsed EventViewV1
// objects; throws a resync_required error carrying the snapshot the
// caller must refetch when the daemon rotated instances.
export async function* streamEvents(runId, after, knownEpoch) {
  const params = new URLSearchParams({ after: String(after) });
  if (knownEpoch) params.set('known_epoch', knownEpoch);
  const response = await fetch(`${DAEMON}/v1/runs/${runId}/events/stream?${params}`, {
    headers: authHeaders({ Accept: 'text/event-stream' }),
  });
  if (!response.ok) {
    const text = await response.text();
    let code = `http-${response.status}`;
    try {
      code = JSON.parse(text).error.code;
    } catch {
      /* keep the http code */
    }
    if (code === 'resync_required') {
      const fresh = await snapshot(runId);
      throw Object.assign(new Error('resync_required: refetch the snapshot'), { snapshot: fresh });
    }
    throw new Error(`stream: ${code}`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let eventId = null;
  let eventName = null;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return;
    buffer += decoder.decode(value, { stream: true });
    let boundary;
    while ((boundary = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, boundary).trimEnd();
      buffer = buffer.slice(boundary + 1);
      if (line === '') {
        eventId = null;
        eventName = null;
        continue;
      }
      if (line.startsWith('id:')) eventId = line.slice(3).trim();
      else if (line.startsWith('event:')) eventName = line.slice(6).trim();
      else if (line.startsWith('data:')) {
        const data = line.slice(5).trim();
        if (eventName === 'resync_required') {
          const fresh = await snapshot(runId);
          throw Object.assign(new Error('resync_required: refetch the snapshot'), { snapshot: fresh });
        }
        try {
          yield { id: eventId, event: eventName, data: JSON.parse(data) };
        } catch {
          /* keepalive comments and non-JSON frames are ignored */
        }
      }
    }
  }
}
