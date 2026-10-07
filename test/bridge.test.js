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
    let flushed = false;
    const nc = {
      publish(subject, data) { published.push([subject, data.toString()]); },
      async flush() { await new Promise(resolve => setTimeout(resolve, 20)); flushed = true; },
      closed() { return new Promise(() => {}); },
      async *status() {},
      isClosed() { return false; },
      async drain() {
        assert.equal(websocketClosed, true);
        assert.deepEqual(published, [
          ['ewelink.bridge.status','online'],
          ['ewelink.123.state.raw','{"action":"update","deviceid":"123","params":{"switch":"on"}}'],
          ['ewelink.123.state.switch','on'],
          ['ewelink.bridge.status','offline']
        ]);
        console.log('DRAIN_VERIFIED');
      }
    };
    class WebAPI {
      constructor() {
        assert.equal(flushed, true, 'source must wait for NATS flush');
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

test('NATS publish permission error blocks source startup even when flush succeeds', () => {
  const script = `
    const Module = require('node:module');
    const original = Module._load;
    let emitError;
    const errorReady = new Promise(resolve => { emitError = resolve; });
    const nc = {
      publish() {},
      async flush() {
        emitError();
        await new Promise(resolve => setImmediate(resolve));
      },
      closed() { return new Promise(() => {}); },
      async *status() {
        await errorReady;
        const error = new Error('Permissions Violation for Publish to "ewelink.bridge.status"');
        error.name = 'PermissionViolationError';
        yield {type:'error', error};
      },
      isClosed() { return false; },
      async drain() {},
      async close() {}
    };
    Module._load = function(name, ...args) {
      if (name === 'ewelink-api-next') return {default:{WebAPI:class {
        constructor() { console.log('SOURCE_STARTED'); }
      }}};
      if (name === '@nats-io/transport-node') return {connect:async () => nc};
      return original.call(this,name,...args);
    };
    require('./bridge');
  `;
  const result = spawnSync(process.execPath, ['-e', script], {
    cwd:path.join(__dirname, '..'), encoding:'utf8', timeout:10000,
    env:{...process.env, EWELINK_ACCOUNT:'test', EWELINK_PASSWORD:'test', EWELINK_APP_ID:'test', EWELINK_APP_SECRET:'test', NATS_TOKEN:'', NATS_USER:'', NATS_PASS:'', NATS_CREDS:''}
  });
  assert.equal(result.status, 1, result.stderr);
  assert.match(result.stderr, /NATS error:.*PermissionViolationError/);
  assert.match(result.stderr, /ewelink\.bridge\.status/);
  assert.doesNotMatch(result.stdout, /SOURCE_STARTED|Connecting to eWeLink|Connected to NATS/);
});
