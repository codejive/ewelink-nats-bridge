'use strict';
// Adapted from ewelink-mqtt-bridge: NATS transport and environment configuration.
const Ewelink = require('ewelink-api-next').default;
const { connect, credsAuthenticator } = require('@nats-io/transport-node');
const { loadConfig, deviceMessages } = require('./config');
const {Lease, openLeaseBucket} = require('./lease');
let config;
try { config = loadConfig(); } catch (err) { console.error(err.message); process.exit(1); }
let websocket;
let natsClient;
let shuttingDown = false;
let natsError;
let natsConnected = false;
let sourceStarting = false;
let lease;
let sourceTask;
let sourceClosed;
let resolveSourceClosed;
const ownsSource = () => !config.failover.enabled || Boolean(lease?.owned);
function debug(message) {
  if (config.verbose) console.log(`${lease ? `[${lease.id}] ` : ''}${message}`);
}

async function publishDeviceUpdate(action) {
  if (shuttingDown || !ownsSource()) return;
  const messages = deviceMessages(action, config);
  for (const [subject, payload] of messages) natsClient.publish(subject, Buffer.from(payload));
  console.log(`Published ${messages.length} subjects for device ${action.deviceid}`);
}
function getWebsocketRawMessage(message) {
  if (!message || typeof message.data === 'undefined' || message.data === null) {
    return null;
  }

  return Buffer.isBuffer(message.data) ? message.data.toString('utf8') : String(message.data);
}

function parseWebsocketPayload(raw) {
  if (!raw || raw[0] !== '{') {
    return null;
  }

  let payload;
  try {
    payload = JSON.parse(raw);
  } catch (err) {
    console.error('Failed to parse eWeLink websocket message:', err.message || err);
    return null;
  }

  // Handshake packet includes config but no device state.
  if (payload && payload.config) {
    console.log(`eWeLink websocket config received: ${raw}`);
    return null;
  }

  if (!payload || !payload.deviceid || !payload.params || typeof payload.params !== 'object') {
    return null;
  }

  if (payload.action && payload.action !== 'update') {
    return null;
  }

  return payload;
}

async function loginWithRegionFallback(client) {
  let region = config.ewelinkRegion;

  let response = await client.user.login({
    account: config.ewelinkAccount,
    password: config.ewelinkPassword,
    areaCode: config.ewelinkAreaCode,
    lang: 'en'
  });

  if (!shuttingDown && ownsSource() && response && response.error === 10004 && response.data && response.data.region) {
    region = response.data.region;
    console.log(`eWeLink account redirects to region: ${region}`);
    client.setUrl(region);
    response = await client.user.login({
      account: config.ewelinkAccount,
      password: config.ewelinkPassword,
      areaCode: config.ewelinkAreaCode,
      lang: 'en'
    });
  }

  return { response, region };
}

async function startBridge() {
  if (shuttingDown || !ownsSource()) return;
  console.log(`Connecting to eWeLink cloud region: ${config.ewelinkRegion}`);

  const webApi = new Ewelink.WebAPI({
    appId: config.ewelinkAppId,
    appSecret: config.ewelinkAppSecret,
    region: config.ewelinkRegion
  });

  const { response: loginResponse, region: resolvedRegion } = await loginWithRegionFallback(webApi);
  if (shuttingDown || !ownsSource()) { debug('Discarding delayed login result: shutting down or ownership invalid'); return; }
  if (!loginResponse || loginResponse.error) {
    const message = loginResponse && loginResponse.msg ? loginResponse.msg : JSON.stringify(loginResponse || {});
    throw new Error(`Failed to authenticate with eWeLink cloud: ${message}`);
  }

  const wsClient = new Ewelink.Ws({
    appId: config.ewelinkAppId,
    appSecret: config.ewelinkAppSecret,
    region: resolvedRegion
  });

  sourceClosed = new Promise(resolve => { resolveSourceClosed = resolve; });
  console.log('eWeLink authentication succeeded; creating websocket connection');
  websocket = await wsClient.Connect.create(
    {
      region: resolvedRegion,
      at: webApi.at,
      userApiKey: webApi.userApiKey,
      appId: config.ewelinkAppId
    },
    () => {
      console.log('eWeLink websocket opened and listening for updates');
    },
    () => {
      console.error('eWeLink websocket closed');
      resolveSourceClosed();
      if (!shuttingDown && (config.failover.enabled || config.exitOnWsClose)) {
        shutdown('WEBSOCKET_CLOSED', 1);
      }
    },
    (event) => {
      console.error('eWeLink websocket error:', event && event.message ? event.message : event);
    },
    async (_ws, message) => {
      const rawMessage = getWebsocketRawMessage(message);

      if (config.verbose && rawMessage !== null) {
        console.log('eWeLink websocket message:', rawMessage);
      }

      const action = parseWebsocketPayload(rawMessage);
      if (!action) {
        return;
      }

      await publishDeviceUpdate(action);
    }
  );

  if (shuttingDown || !ownsSource()) {
    debug('Closing delayed websocket result: shutting down or ownership invalid');
    websocket.close(); return;
  }
  console.log('Bridge is running. Waiting for device updates...');
}

async function shutdown(signal, exitCode = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`${lease ? `[${lease.id}] ` : ''}Received ${signal}, shutting down...`);
  const timeout = setTimeout(() => {
    console.error(`${lease ? `[${lease.id}] ` : ''}Shutdown timed out; any unreleased lease will expire`);
    process.exit(exitCode || 1);
  }, 5000);
  try {
    const stoppingRenewals = lease?.stopRenewals();
    debug('Closing source and waiting for pending startup and renewal operations');
    if (websocket) websocket.close();
    await stoppingRenewals;
    // A delayed dispatch/login must finish before release. The shutdown limit
    // exits without release if it cannot finish, leaving server TTL to recover.
    if (sourceTask) await sourceTask.catch(() => {});
    if (websocket) websocket.close();
    if (config.failover.enabled && websocket) {
      debug('Waiting for confirmed eWeLink websocket closure before lease release');
      await sourceClosed;
      debug('eWeLink websocket closure confirmed');
    }
    if (natsClient && !natsClient.isClosed()) {
      if (lease) await lease.release().catch(err => console.error(`[${lease.id}] Safe release failed; leaving lease to expire:`, err.message));
      debug('Draining NATS connection');
      await natsClient.drain();
      console.log('NATS connection drained; shutdown complete');
    } else if (lease) {
      console.log(`[${lease.id}] Lease release skipped: NATS unavailable; leaving lease to expire`);
    }
  } catch (err) {
    console.error('Failed to shut down cleanly:', err.message || err);
    if (natsClient) await natsClient.close();
  } finally {
    clearTimeout(timeout);
    process.exit(exitCode);
  }
}

async function monitorNats() {
  for await (const status of natsClient.status()) {
    if (status.type === 'error') {
      natsError = status.error || new Error('Unknown NATS server error');
      console.error('NATS error:', natsError);
      if ((sourceStarting || config.failover.enabled) && !shuttingDown) {
        if (lease) lease.invalidate();
        shutdown('NATS_ERROR', 1);
      }
      continue;
    }
    if (status.type === 'reconnect' && !shuttingDown) {
      natsConnected = true;
      console.log('Reconnected to NATS');
    } else if (['disconnect', 'reconnecting'].includes(status.type)) {
      natsConnected = false;
      console.log(`NATS ${status.type}${status.server ? `: ${status.server}` : ''}`);
      if (config.failover.enabled && !shuttingDown) {
        if (lease) lease.invalidate();
        shutdown('NATS_DISCONNECTED', 1);
      }
    }
  }
}

async function main() {
  const options = {...config.natsOptions};
  if (config.natsCreds) options.authenticator = credsAuthenticator(Buffer.from(config.natsCreds));
  natsClient = await connect(options);
  natsConnected = true;
  if (shuttingDown) { await natsClient.close(); return; }
  natsClient.closed().then(err => {
    if (!shuttingDown) {
      console.error('NATS connection closed:', err ? err.message : 'connection ended');
      if (lease) lease.invalidate();
      shutdown('NATS_CLOSED', 1);
    }
  });
  monitorNats().catch(err => { console.error('NATS status error:', err.message); shutdown('NATS_STATUS_ERROR', 1); });
  if (config.failover.enabled) {
    debug(`Opening failover bucket ${config.failover.bucket}`);
    const kv = await openLeaseBucket(natsClient, config.failover);
    debug(`Failover bucket validated: TTL ${config.failover.duration}ms, history 1`);
    if (shuttingDown) return;
    lease = new Lease(kv, config.failover, err => {
      console.error(`[${lease.id}] active -> standby: lease renewal failed or uncertain:`, err.message);
      shutdown('LEASE_LOST', 1);
    }, undefined, (message, verboseOnly) => { if (!verboseOnly || config.verbose) console.log(message); });
    console.log(`[${lease.id}] standby; waiting for ${config.failover.bucket}/${config.failover.key}`);
    while (!shuttingDown && !await lease.acquire()) {
      const delay = config.failover.retry * (0.8 + Math.random() * 0.4);
      debug(`Standby acquisition retry in ${Math.round(delay)}ms`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
    if (shuttingDown) return;
    console.log(`[${lease.id}] standby -> active; revision ${lease.revision}`);
  }
  // Let queued connection events be handled before starting the source.
  await new Promise(resolve => setImmediate(resolve));
  if (natsError) throw natsError;
  if (shuttingDown) return;
  if (!natsConnected || natsClient.isClosed()) throw new Error('NATS disconnected before source startup');
  console.log('Connected to NATS');
  sourceStarting = true;
  sourceTask = startBridge();
  await sourceTask;
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('unhandledRejection', reason => { console.error('Unhandled rejection:', reason); shutdown('UNHANDLED_REJECTION', 1); });
process.on('uncaughtException', err => { console.error('Uncaught exception:', err); shutdown('UNCAUGHT_EXCEPTION', 1); });
main().catch(err => { console.error('Failed to start bridge:', err.message || err); shutdown('STARTUP_FAILURE', 1); });
