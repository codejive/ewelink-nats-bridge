'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {spawnSync} = require('node:child_process');
const path = require('node:path');
test('bridge logs in with region redirect, publishes cloud update and drains on shutdown', () => {
  const script = `
    const Module = require('node:module');
    const assert = require('node:assert/strict');
    const original = Module._load;
    const published = [];
    let loginCount = 0;
    let websocketClosed = false;
    const nc = {
      publish(subject, data) { published.push([subject, data.toString()]); },
      async flush() {},
      closed() { return new Promise(() => {}); },
      async *status() {},
      isClosed() { return false; },
      async drain() {
        assert.equal(websocketClosed, true);
        assert.deepEqual(published, [
          ['ewelink.bridge.status','online'],
          ['ewelink.123.state.raw','{"switch":"on"}'],
          ['ewelink.123.state.switch','on'],
          ['ewelink.bridge.status','offline']
        ]);
        console.log('DRAIN_VERIFIED');
      }
    };
    class WebAPI {
      constructor() {
        this.at = 'access'; this.userApiKey = 'api';
        this.user = {login: async () => ++loginCount === 1 ? {error:10004,data:{region:'eu'}} : {error:0}};
      }
      setUrl(region) { assert.equal(region, 'eu'); }
    }
    class Ws {
      constructor(options) {
        assert.equal(options.region, 'eu');
        this.Connect = {create: async (options, opened, closed, error, message) => {
          assert.equal(options.at, 'access');
          opened();
          await message(null, {data:Buffer.from('{"action":"update","deviceid":"123","params":{"switch":"on"}}')});
          setImmediate(() => process.emit('SIGTERM'));
          return {close() { websocketClosed = true; closed(); }};
        }};
      }
    }
    Module._load = function(name, ...args) {
      if (name === 'ewelink-api-next') return {default:{WebAPI,Ws}};
      if (name === '@nats-io/transport-node') return {connect: async options => {
        assert.equal(options.token, 'token'); return nc;
      }};
      return original.call(this,name,...args);
    };
    require('./bridge');
  `;
  const result = spawnSync(process.execPath, ['-e', script], {
    cwd:path.join(__dirname, '..'), encoding:'utf8', timeout:10000,
    env:{...process.env, EWELINK_ACCOUNT:'test', EWELINK_PASSWORD:'test', EWELINK_APP_ID:'test', EWELINK_APP_SECRET:'test', NATS_TOKEN:'token', NATS_USER:'', NATS_PASS:'', NATS_CREDS:''}
  });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /DRAIN_VERIFIED/);
});
