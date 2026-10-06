'use strict';

function loadConfig(env = process.env) {
  const get = (name, fallback) => env[name] === undefined || env[name] === '' ? fallback : env[name];
  const bool = (name, fallback) => {
    const value = String(get(name, fallback)).toLowerCase();
    if (!['true', 'false', '1', '0', 'yes', 'no', 'on', 'off'].includes(value)) throw new Error(`${name} must be a boolean.`);
    return ['true', '1', 'yes', 'on'].includes(value);
  };
  const integer = (name, fallback, minimum) => {
    const value = Number(get(name, fallback));
    if (!Number.isSafeInteger(value) || value < minimum) throw new Error(`${name} must be an integer >= ${minimum}.`);
    return value;
  };
  const config = {
    ewelinkAccount: get('EWELINK_ACCOUNT', get('EWELINK_EMAIL')),
    ewelinkPassword: get('EWELINK_PASSWORD'),
    ewelinkRegion: get('EWELINK_REGION', 'us'),
    ewelinkAreaCode: get('EWELINK_AREA_CODE', '+1'),
    ewelinkAppId: get('EWELINK_APP_ID'),
    ewelinkAppSecret: get('EWELINK_APP_SECRET'),
    subjectPrefix: get('SUBJECT_PREFIX', 'ewelink'),
    publishRawState: String(get('PUBLISH_RAW_STATE', true)).toLowerCase() === 'only' ? 'only' : bool('PUBLISH_RAW_STATE', true),
    verbose: bool('VERBOSE', false),
    exitOnWsClose: bool('EXIT_ON_WEBSOCKET_CLOSE', true),
    natsOptions: {
      servers: get('NATS_SERVERS', get('NATS_URL', 'nats://127.0.0.1:4222')).split(',').map(value => value.trim()),
      name: get('NATS_NAME', 'ewelink-nats-bridge'),
      reconnect: true,
      reconnectTimeWait: integer('NATS_RECONNECT_TIME_WAIT', 3000, 0),
      maxReconnectAttempts: integer('NATS_MAX_RECONNECT_ATTEMPTS', -1, -1),
      timeout: integer('NATS_CONNECT_TIMEOUT', 10000, 1)
    }
  };
  for (const [name, value] of Object.entries({EWELINK_ACCOUNT: config.ewelinkAccount, EWELINK_PASSWORD: config.ewelinkPassword, EWELINK_APP_ID: config.ewelinkAppId, EWELINK_APP_SECRET: config.ewelinkAppSecret})) {
    if (!value) throw new Error(`${name} is required.`);
  }
  if (!config.subjectPrefix.split('.').every(part => part && !/[\s*>]/.test(part))) throw new Error('SUBJECT_PREFIX must contain nonempty dot-separated tokens without whitespace or wildcards.');
  if (config.natsOptions.servers.some(server => !server)) throw new Error('NATS_SERVERS must contain nonempty server addresses.');
  const user = get('NATS_USER');
  const pass = get('NATS_PASS');
  const token = get('NATS_TOKEN');
  const creds = get('NATS_CREDS');
  if ([Boolean(user || pass), Boolean(token), Boolean(creds)].filter(Boolean).length > 1) throw new Error('Choose one NATS authentication method: user/pass, token, or credentials.');
  if (pass && !user) throw new Error('NATS_PASS requires NATS_USER.');
  Object.assign(config.natsOptions, {user, pass, token});
  config.natsCreds = creds;
  const ca = get('NATS_TLS_CA');
  const cert = get('NATS_TLS_CERT');
  const key = get('NATS_TLS_KEY');
  if (Boolean(cert) !== Boolean(key)) throw new Error('NATS_TLS_CERT and NATS_TLS_KEY must be set together.');
  if (bool('NATS_TLS', false) || ca || cert || key) config.natsOptions.tls = {ca, cert, key};
  return config;
}

function sanitizeSubjectToken(value) {
  return String(value).replace(/[.\s*>]/g, '_') || '_';
}

function deviceMessages(action, config) {
  const base = `${config.subjectPrefix}.${sanitizeSubjectToken(action.deviceid)}.state`;
  const messages = config.publishRawState ? [[`${base}.raw`, JSON.stringify(action.params)]] : [];
  if (config.publishRawState === 'only') return messages;
  for (const [key, value] of Object.entries(action.params)) {
    messages.push([`${base}.${sanitizeSubjectToken(key)}`, value == null ? '' : typeof value === 'object' ? JSON.stringify(value) : String(value)]);
  }
  return messages;
}

module.exports = {loadConfig, sanitizeSubjectToken, deviceMessages};
