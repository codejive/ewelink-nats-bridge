'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

for (const [stage, expected] of [
  ['nats', 'Failed to connect to NATS'],
  ['login', 'Failed to authenticate with eWeLink cloud'],
  ['websocket', 'Failed to connect to eWeLink websocket']
]) {
  test(`startup authorization error identifies ${stage}`, async () => {
    const errors = [];
    let finish;
    const exited = new Promise(resolve => { finish = resolve; });
    const fail = () => { throw new Error('Authorization Violation'); };
    const connection = {
      closed: () => new Promise(() => {}),
      status: async function* () {},
      isClosed: () => false,
      drain: async () => {}
    };
    const config = { failover: { enabled: false }, natsOptions: {}, ewelinkRegion: 'eu' };
    const modules = {
      'ewelink-api-next': { default: {
        WebAPI: class { constructor() { this.user = { login: stage === 'login' ? fail : async () => ({ error: 0 }) }; } },
        Ws: class { constructor() { this.Connect = { create: fail }; } }
      } },
      '@nats-io/transport-node': { connect: stage === 'nats' ? fail : async () => connection },
      './config': { loadConfig: () => config },
      './lease': {}
    };
    vm.runInNewContext(fs.readFileSync(path.join(__dirname, '..', 'bridge.js'), 'utf8'), {
      require: name => {
        assert.ok(Object.hasOwn(modules, name), `Unexpected module ${name}`);
        return modules[name];
      },
      console: { log() {}, error: (...args) => errors.push(args.join(' ')) },
      process: { on() {}, exit: finish },
      Buffer, setImmediate, setTimeout, clearTimeout
    });
    assert.equal(await exited, 1);
    assert.ok(errors.includes(`Failed to start bridge: ${expected}: Authorization Violation`), errors.join('\n'));
  });
}
