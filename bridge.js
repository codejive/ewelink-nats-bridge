'use strict';
// Adapted from ewelink-mqtt-bridge: NATS transport and environment configuration.
const Ewelink = require('ewelink-api-next').default;
const { connect, credsAuthenticator } = require('@nats-io/transport-node');
const { loadConfig, deviceMessages } = require('./config');
let config;
try { config = loadConfig(); } catch (err) { console.error(err.message); process.exit(1); }
let websocket;
let natsClient;
let shuttingDown = false;
const bridgeStatusSubject = `${config.subjectPrefix}.bridge.status`;

async function publishDeviceUpdate(action) {
  if (shuttingDown) return;
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

  if (response && response.error === 10004 && response.data && response.data.region) {
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
  console.log(`Connecting to eWeLink cloud region: ${config.ewelinkRegion}`);

  const webApi = new Ewelink.WebAPI({
    appId: config.ewelinkAppId,
    appSecret: config.ewelinkAppSecret,
    region: config.ewelinkRegion
  });

  const { response: loginResponse, region: resolvedRegion } = await loginWithRegionFallback(webApi);
  if (!loginResponse || loginResponse.error) {
    const message = loginResponse && loginResponse.msg ? loginResponse.msg : JSON.stringify(loginResponse || {});
    throw new Error(`Failed to authenticate with eWeLink cloud: ${message}`);
  }

  const wsClient = new Ewelink.Ws({
    appId: config.ewelinkAppId,
    appSecret: config.ewelinkAppSecret,
    region: resolvedRegion
  });

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
      if (!shuttingDown && config.exitOnWsClose) {
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

  console.log('Bridge is running. Waiting for device updates...');
}

async function shutdown(signal, exitCode = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`Received ${signal}, shutting down...`);
  const timeout = setTimeout(() => process.exit(exitCode || 1), 5000);
  try {
    if (websocket) websocket.close();
    if (natsClient && !natsClient.isClosed()) {
      natsClient.publish(bridgeStatusSubject, Buffer.from('offline'));
      await natsClient.drain();
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
    if (status.type === 'reconnect' && !shuttingDown) {
      console.log('Reconnected to NATS');
      natsClient.publish(bridgeStatusSubject, Buffer.from('online'));
    } else if (['disconnect', 'error', 'reconnecting'].includes(status.type)) {
      console.log(`NATS ${status.type}`);
    }
  }
}

async function main() {
  const options = {...config.natsOptions};
  if (config.natsCreds) options.authenticator = credsAuthenticator(Buffer.from(config.natsCreds));
  natsClient = await connect(options);
  if (shuttingDown) { await natsClient.close(); return; }
  console.log('Connected to NATS');
  natsClient.closed().then(err => {
    if (!shuttingDown) {
      console.error('NATS connection closed:', err ? err.message : 'connection ended');
      shutdown('NATS_CLOSED', 1);
    }
  });
  monitorNats().catch(err => { console.error('NATS status error:', err.message); shutdown('NATS_STATUS_ERROR', 1); });
  natsClient.publish(bridgeStatusSubject, Buffer.from('online'));
  await natsClient.flush();
  await startBridge();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('unhandledRejection', reason => { console.error('Unhandled rejection:', reason); shutdown('UNHANDLED_REJECTION', 1); });
process.on('uncaughtException', err => { console.error('Uncaught exception:', err); shutdown('UNCAUGHT_EXCEPTION', 1); });
main().catch(err => { console.error('Failed to start bridge:', err.message || err); shutdown('STARTUP_FAILURE', 1); });
