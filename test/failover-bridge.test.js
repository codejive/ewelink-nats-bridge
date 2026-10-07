'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {spawnSync} = require('node:child_process');
const path = require('node:path');

for (const scenario of ['standby', 'active', 'lost-login', 'lost-dispatch', 'displaced', 'disconnect']) {
  test(`failover lifecycle: ${scenario}`, () => {
    const script = `
      const assert = require('node:assert/strict');
      const Module = require('node:module'); const original = Module._load;
      const scenario = ${JSON.stringify(scenario)};
      let acquired = false, closed = false, login = false, released = false;
      const published = [];
      const kv = {
        async status() { return {ttl:30000, history:1, streamInfo:{config:{subjects:['$KV.ewelink_leases.>'], max_msgs:-1, max_bytes:-1}}}; },
        async create() {
          if (scenario === 'standby') { setImmediate(() => process.emit('SIGTERM')); throw Object.assign(new Error('occupied'), {code:10071}); }
          acquired = true; return 1;
        },
        async update() { throw new Error('uncertain renewal'); },
        async delete(key, options) {
          assert.equal(closed, true); assert.equal(options.previousSeq, 1);
          assert.equal(published.at(-1), 'offline'); released = true;
        }
      };
      const nc = {
        publish(subject, data) { published.push(data.toString()); }, async flush() {},
        closed() { return new Promise(() => {}); },
        async *status() {
          if (scenario === 'disconnect') { await new Promise(r => setTimeout(r, 25)); yield {type:'disconnect'}; }
        },
        isClosed() { return false; }, async close() {},
        async drain() {
          if (scenario === 'standby') { assert.equal(login, false); assert.deepEqual(published, []); assert.equal(released, false); }
          else if (scenario.startsWith('lost') || scenario === 'disconnect') { assert.equal(released, false); assert.deepEqual(published, ['online']); }
          else { assert.equal(released, true); assert.deepEqual(published, ['online','offline']); }
          console.log('VERIFIED');
        }
      };
      class WebAPI {
        constructor() {
          assert.equal(acquired, true); login = true;
          this.user = {login: async () => { if (scenario === 'lost-login') await new Promise(r => setTimeout(r, 50)); return {error:0}; }};
        }
      }
      class Ws {
        constructor() {
          assert.notEqual(scenario, 'lost-login');
          this.Connect = {create:async (opts, opened, onClose) => {
            if (scenario === 'lost-dispatch') await new Promise(r => setTimeout(r, 50));
            if (scenario === 'active') setImmediate(() => process.emit('SIGTERM'));
            if (scenario === 'displaced') setImmediate(() => { closed = true; onClose(); });
            return {close() { closed = true; onClose(); }};
          }};
        }
      }
      Module._load = function(name, ...args) {
        if (name === '@nats-io/kv') return {Kvm:class {async open() { return kv; }}};
        if (name === '@nats-io/transport-node') return {connect:async () => nc};
        if (name === 'ewelink-api-next') return {default:{WebAPI,Ws}};
        return original.call(this, name, ...args);
      };
      require('./bridge');
    `;
    const result = spawnSync(process.execPath, ['-e', script], {
      cwd:path.join(__dirname, '..'), encoding:'utf8', timeout:10000,
      env:{...process.env, EWELINK_ACCOUNT:'test', EWELINK_PASSWORD:'test', EWELINK_APP_ID:'test', EWELINK_APP_SECRET:'test', NATS_TOKEN:'', NATS_USER:'', NATS_PASS:'', NATS_CREDS:'', FAILOVER_ENABLED:'true', FAILOVER_RENEWAL_INTERVAL:scenario.startsWith('lost') ? '10' : '5000', EXIT_ON_WEBSOCKET_CLOSE:'false'}
    });
    assert.equal(result.status, ['active','standby'].includes(scenario) ? 0 : 1, result.stderr);
    assert.match(result.stdout, /VERIFIED/, result.stderr);
  });
}
