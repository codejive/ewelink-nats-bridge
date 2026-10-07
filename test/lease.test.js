'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {Lease, openLeaseBucket, validateLeaseBucket} = require('../lease');
const config = {key:'bridge', renewal:60000, duration:30000};
test('bucket diagnostics identify mismatches with expected and actual values', () => {
  const settings = {...config, bucket:'leases'};
  const status = {ttl:30000, history:1, streamInfo:{config:{subjects:['$KV.leases.>'], max_msgs:-1, max_bytes:-1}}};
  assert.doesNotThrow(() => validateLeaseBucket(status, settings));
  status.ttl = 60000;
  status.streamInfo.config.max_bytes = 512;
  status.streamInfo.config.allow_msg_ttl = true;
  assert.throws(() => validateLeaseBucket(status, settings), err => {
    assert.match(err.message, /TTL .*expected 30000, actual 60000/);
    assert.match(err.message, /max_bytes: expected -1 \(unlimited\) or at least 1024 bytes, actual 512/);
    assert.match(err.message, /allow_msg_ttl: expected false, actual true/);
    assert.doesNotMatch(err.message, /history|subjects/);
    return true;
  });
  delete status.streamInfo.config.max_msgs;
  assert.throws(() => validateLeaseBucket(status, settings), /max_msgs: expected -1, actual missing/);
});
test('bucket accepts unlimited or sufficient finite byte capacity', () => {
  const settings = {...config, bucket:'leases'};
  const status = {ttl:30000, history:1, streamInfo:{config:{subjects:['$KV.leases.>'], max_msgs:-1}}};
  for (const capacity of [-1, 1024, 1048576]) {
    status.streamInfo.config.max_bytes = capacity;
    assert.doesNotThrow(() => validateLeaseBucket(status, settings));
  }
  for (const capacity of [undefined, -2, 0, 1023, 1024.5]) {
    status.streamInfo.config.max_bytes = capacity;
    assert.throws(() => validateLeaseBucket(status, settings), /max_bytes: expected -1 \(unlimited\) or at least 1024 bytes/);
  }
});
function store() {
  let entry, seq = 0;
  const conflict = () => Object.assign(new Error('wrong revision'), {code:10071});
  return {
    async create(key, value) { if (entry) throw conflict(); entry = {value, revision:++seq}; return seq; },
    async update(key, value, revision) { if (entry?.revision !== revision) throw conflict(); entry = {value, revision:++seq}; return seq; },
    async delete(key, {previousSeq}) { if (entry?.revision !== previousSeq) throw conflict(); entry = undefined; },
    expire() { entry = undefined; },
    get entry() { return entry; }
  };
}
test('two contenders, revision renewal, expiry takeover and recovered standby', async () => {
  const kv = store(); const a = new Lease(kv, config, () => {}); const b = new Lease(kv, config, () => {});
  try {
    assert.equal(await a.acquire(), true); assert.equal(await b.acquire(), false);
    await a.renew(); assert.equal(a.revision, 2);
    kv.expire(); assert.equal(await b.acquire(), true);
    let lost = false; a.onLost = () => { lost = true; };
    await a.renew(); assert.equal(a.owned, false); assert.equal(lost, true);
    const recovered = new Lease(kv, config, () => {});
    assert.equal(await recovered.acquire(), false); recovered.invalidate();
    await a.release(); assert.equal(kv.entry.value.toString(), b.id);
  } finally { a.invalidate(); b.invalidate(); }
});
test('uncertain renewal invalidates ownership; delayed success cannot restore it; calls serialize', async () => {
  const kv = store(); let resolve;
  const lease = new Lease(kv, config, () => {});
  await lease.acquire();
  kv.update = () => new Promise(r => { resolve = r; });
  const pending = lease.renew(); assert.equal(lease.renew(), pending);
  lease.invalidate(); resolve(99); await pending;
  assert.equal(lease.revision, 0); assert.equal(lease.owned, false);
  const other = new Lease(store(), config, () => {});
  await other.acquire(); other.kv.update = async () => { throw new Error('timeout after committed write'); };
  await other.renew(); assert.equal(other.owned, false); assert.equal(other.revision, 0);
});
test('release waits for confirmed renewal and never deletes newer ownership', async () => {
  const kv = store(); const lease = new Lease(kv, config, () => {});
  await lease.acquire();
  const update = kv.update.bind(kv); let finish;
  kv.update = (...args) => new Promise(resolve => { finish = async () => resolve(await update(...args)); });
  const renewal = lease.renew(); const stopping = lease.stopRenewals();
  assert.equal(lease.revision, 1); await finish(); await renewal; await stopping;
  assert.equal(lease.revision, 2); await lease.release(); assert.equal(kv.entry, undefined);
  const stale = new Lease(kv, config, () => {}); await stale.acquire();
  kv.expire(); await kv.create('bridge', Buffer.from('new-owner'));
  await stale.stopRenewals(); await assert.rejects(stale.release(), /wrong revision/);
  assert.equal(kv.entry.value.toString(), 'new-owner');
});
test('delayed acquisition after invalidation never establishes ownership; errors are not contention', async () => {
  let resolve; const lease = new Lease({create: () => new Promise(r => { resolve = r; })}, config, () => {});
  const pending = lease.acquire(); lease.invalidate(); resolve(1);
  assert.equal(await pending, false); assert.equal(lease.owned, false);
  const denied = new Lease({create:async () => { throw new Error('permission denied'); }}, config, () => {});
  await assert.rejects(denied.acquire(), /permission denied/);
});
test('real NATS atomic acquisition, bucket validation and server TTL', {skip: !process.env.NATS_TEST_SERVER}, async () => {
  const {spawn} = require('node:child_process');
  const {mkdtemp, rm} = require('node:fs/promises');
  const {tmpdir} = require('node:os'); const {join} = require('node:path');
  const {connect} = require('@nats-io/transport-node'); const {Kvm} = require('@nats-io/kv');
  const directory = await mkdtemp(join(tmpdir(), 'ewelink-kv-'));
  const server = spawn(process.env.NATS_TEST_SERVER, ['-js', '-p', '-1', '-sd', directory], {stdio:['ignore','ignore','pipe']});
  let nc;
  try {
    const port = await new Promise((resolve, reject) => {
      let output = ''; const timer = setTimeout(() => reject(new Error('NATS startup timeout')), 10000);
      server.once('error', err => { clearTimeout(timer); reject(err); });
      server.stderr.on('data', data => { output += data; const match = output.match(/Listening for client connections on .*:(\d+)/); if (match) { clearTimeout(timer); resolve(match[1]); } });
    });
    nc = await connect({servers:`nats://127.0.0.1:${port}`});
    await new Kvm(nc).create('leases', {ttl:1000, history:1});
    const settings = {...config, bucket:'leases', duration:1000};
    const kv = await openLeaseBucket(nc, settings);
    await assert.rejects(openLeaseBucket(nc, {...settings, duration:30000}), /TTL .*expected 30000, actual 1000/);
    const a = new Lease(kv, settings, () => {}); const b = new Lease(kv, settings, () => {});
    try {
      const results = await Promise.all([a.acquire(), b.acquire()]); assert.equal(results.filter(Boolean).length, 1);
      const winner = results[0] ? a : b; const standby = results[0] ? b : a;
      await winner.renew(); assert.equal(winner.revision, 2);
      await new Promise(resolve => setTimeout(resolve, 1600));
      assert.equal(await standby.acquire(), true);
      await winner.stopRenewals(); await assert.rejects(winner.release());
      assert.equal(Buffer.from((await kv.get(settings.key)).value).toString(), standby.id);
    } finally { a.invalidate(); b.invalidate(); }
  } finally {
    if (nc) await nc.close();
    const exited = new Promise(resolve => server.once('exit', resolve)); server.kill(); await exited;
    await rm(directory, {recursive:true, force:true});
  }
});
