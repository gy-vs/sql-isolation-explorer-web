'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const { Engine, ISOLATION_LEVELS } = require('./src/engine');
const { Scheduler } = require('./src/scheduler');
const { Store } = require('./src/store');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data', 'state.json');
const PUBLIC = path.join(__dirname, 'public');

const store = new Store(DATA_FILE);
const live = new Map(); // id -> { engine, scheduler }

function getSession(record) {
  let s = live.get(record.id);
  if (!s) {
    const engine = record.engine ? Engine.fromJSON(record.engine) : new Engine(record.isolation);
    const scheduler = record.scheduler
      ? Scheduler.restore(engine, record.scheduler)
      : new Scheduler(engine, record.ops);
    s = { engine, scheduler };
    live.set(record.id, s);
  }
  return s;
}

function persist(record) {
  const s = live.get(record.id);
  if (s) {
    record.engine = s.engine.toJSON();
    record.scheduler = s.scheduler.toJSON();
  }
  store.save();
}

function stateFor(record) {
  const s = getSession(record);
  return {
    id: record.id,
    name: record.name,
    isolation: record.isolation,
    createdAt: record.createdAt,
    engine: s.engine.toJSON(),
    scheduler: s.scheduler.toJSON(),
    finished: s.scheduler.finished,
    history: record.history,
  };
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let b = '';
    req.on('data', (c) => (b += c));
    req.on('end', () => {
      try {
        resolve(b ? JSON.parse(b) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

function send(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(body);
}

const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  try {
    // ------------------------------------------------------------- API
    if (p === '/api/schedules' && req.method === 'GET') {
      return send(res, 200, {
        levels: ISOLATION_LEVELS,
        schedules: Object.values(store.data.schedules).map((r) => ({
          id: r.id,
          name: r.name,
          isolation: r.isolation,
          createdAt: r.createdAt,
          steps: r.scheduler ? r.scheduler.cursor : 0,
          ops: r.ops.length,
          runs: r.history.length,
        })),
      });
    }

    if (p === '/api/schedules' && req.method === 'POST') {
      const body = await readBody(req);
      if (!ISOLATION_LEVELS.includes(body.isolation)) {
        return send(res, 400, { error: `isolation must be one of ${ISOLATION_LEVELS.join(', ')}` });
      }
      if (!Array.isArray(body.ops)) return send(res, 400, { error: 'ops must be an array' });
      const record = {
        id: crypto.randomBytes(4).toString('hex'),
        name: body.name || 'untitled',
        isolation: body.isolation,
        ops: body.ops,
        createdAt: new Date().toISOString(),
        engine: null,
        scheduler: null,
        history: [],
      };
      store.data.schedules[record.id] = record;
      persist(record);
      return send(res, 201, stateFor(record));
    }

    const m = p.match(/^\/api\/schedules\/([a-f0-9]+)(\/(step|run|reset|abort|ops))?$/);
    if (m) {
      const record = store.data.schedules[m[1]];
      if (!record) return send(res, 404, { error: 'schedule not found' });
      const action = m[3];

      if (req.method === 'GET' && !action) return send(res, 200, stateFor(record));

      if (req.method === 'DELETE' && !action) {
        delete store.data.schedules[record.id];
        live.delete(record.id);
        store.save();
        return send(res, 200, { ok: true });
      }

      if (req.method === 'POST' && action === 'step') {
        const s = getSession(record);
        const r = s.scheduler.step();
        persist(record);
        return send(res, 200, { result: r, state: stateFor(record) });
      }

      if (req.method === 'POST' && action === 'run') {
        const s = getSession(record);
        const r = s.scheduler.run();
        persist(record);
        return send(res, 200, { executed: r.length, state: stateFor(record) });
      }

      if (req.method === 'POST' && action === 'reset') {
        const s = getSession(record);
        record.history.push({
          finishedAt: new Date().toISOString(),
          events: s.engine.events,
          anomalies: s.engine.anomalies,
          edges: s.engine.edges,
          clock: s.engine.clock,
        });
        live.delete(record.id);
        record.engine = null;
        record.scheduler = null;
        persist(record);
        return send(res, 200, stateFor(record));
      }

      if (req.method === 'POST' && action === 'abort') {
        const body = await readBody(req);
        const s = getSession(record);
        s.engine.rollback(body.txn);
        persist(record);
        return send(res, 200, stateFor(record));
      }

      if (req.method === 'POST' && action === 'ops') {
        const body = await readBody(req);
        if (!Array.isArray(body.ops)) return send(res, 400, { error: 'ops must be an array' });
        const s = getSession(record);
        const base = s.scheduler.ops.length;
        body.ops.forEach((o, i) => s.scheduler.ops.push({ id: base + i, status: 'pending', ...o }));
        record.ops = s.scheduler.ops.map(({ id, status, result, ...rest }) => rest);
        persist(record);
        return send(res, 200, stateFor(record));
      }
    }

    // ------------------------------------------------------------ static
    if (req.method === 'GET') {
      const file = p === '/' ? 'index.html' : p.slice(1);
      const full = path.join(PUBLIC, file);
      if (full.startsWith(PUBLIC) && fs.existsSync(full) && fs.statSync(full).isFile()) {
        res.writeHead(200, { 'content-type': MIME[path.extname(full)] || 'text/plain' });
        return res.end(fs.readFileSync(full));
      }
    }

    send(res, 404, { error: 'not found' });
  } catch (err) {
    send(res, 500, { error: String(err && err.message) });
  }
});

if (require.main === module) {
  server.listen(PORT, () => console.log(`MVCC workbench on http://localhost:${PORT}`));
}

module.exports = { server, store };
