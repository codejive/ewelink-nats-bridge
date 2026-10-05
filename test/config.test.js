'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {loadConfig, deviceMessages} = require('../config');
const credentials = {EWELINK_ACCOUNT: 'a', EWELINK_PASSWORD: 'b', EWELINK_APP_ID: 'c', EWELINK_APP_SECRET: 'd'};
test('environment-only NATS authentication, TLS and multiple servers', () => {
  const config = loadConfig({...credentials, NATS_SERVERS: 'nats://one:4222, nats://two:4222', NATS_TOKEN: 'secret', NATS_TLS_CA: 'pem', SUBJECT_PREFIX: 'home.ewelink'});
  assert.deepEqual(config.natsOptions.servers, ['nats://one:4222', 'nats://two:4222']);
  assert.equal(config.natsOptions.token, 'secret');
  assert.deepEqual(config.natsOptions.tls, {ca: 'pem', cert: undefined, key: undefined});
  assert.equal(config.subjectPrefix, 'home.ewelink');
});
test('reject invalid subjects, authentication, booleans and numeric settings', () => {
  for (const settings of [{SUBJECT_PREFIX:'a..b'}, {SUBJECT_PREFIX:'a.*'}, {NATS_USER:'a', NATS_TOKEN:'b'}, {NATS_TLS_CERT:'pem'}, {NATS_CONNECT_TIMEOUT:'0'}, {NATS_MAX_RECONNECT_ATTEMPTS:'-2'}, {VERBOSE:'maybe'}, {NATS_SERVERS:'a,'}]) {
    assert.throws(() => loadConfig({...credentials, ...settings}));
  }
  assert.throws(() => loadConfig({}), /required/);
});
test('cloud updates preserve raw, scalar, structured and null payloads and escape subjects', () => {
  const params = {temperature:22.5, switches:[{switch:'on'}], empty:null, 'a.* >':'on'};
  const messages = deviceMessages({deviceid:'a.b', params}, loadConfig(credentials));
  assert.deepEqual(messages, [
    ['ewelink.a_b.state.raw', JSON.stringify(params)],
    ['ewelink.a_b.state.temperature', '22.5'],
    ['ewelink.a_b.state.switches', '[{"switch":"on"}]'],
    ['ewelink.a_b.state.empty', ''],
    ['ewelink.a_b.state.a____', 'on']
  ]);
});
test('raw publishing can be disabled and aliases work', () => {
  const config = loadConfig({...credentials, EWELINK_ACCOUNT:'', EWELINK_EMAIL:'email', NATS_URL:'nats://alias:4222', PUBLISH_RAW_STATE:'off'});
  assert.equal(config.ewelinkAccount, 'email');
  assert.deepEqual(config.natsOptions.servers, ['nats://alias:4222']);
  assert.deepEqual(deviceMessages({deviceid:'123', params:{switch:'on'}}, config), [['ewelink.123.state.switch', 'on']]);
});
