# eWeLink to NATS Bridge

Adapted from [ewelink-mqtt-bridge](https://github.com/codejive/ewelink-mqtt-bridge), this bridge authenticates to eWeLink cloud and republishes live websocket device updates to Core [NATS](https://nats.io/). Configuration uses only environment variables; no configuration file is required.

Looking for the MQTT version of this bridge? Check out [ewelink-mqtt-bridge](https://github.com/codejive/ewelink-mqtt-bridge).

## How It Works

1. Authenticate to eWeLink cloud using app credentials and account credentials.
2. Open a persistent websocket to receive live device update events.
3. Publish each update to NATS topics using a predictable topic structure.

## Subjects and payloads

- `ewelink.<deviceId>.state.<key>`: individual value (objects/arrays as JSON, scalars as text, null as empty text).
- `ewelink.<deviceId>.state.raw`: complete update params as JSON (when `PUBLISH_RAW_STATE` is set to `true`).
- `ewelink.bridge.status`: `online` on connection/reconnection, `offline` on graceful shutdown.

For example, `{"temperature":22.5,"humidity":48}` publishes a raw JSON message and two individual messages, `22.5` and `48`. Dots, whitespace and NATS wildcards in device IDs or parameter names become underscores. Subscribe to `ewelink.>` for all messages.

Core NATS delivers messages to current subscribers without retention or MQTT QoS/last-will semantics. Late subscribers do not receive previous state or status. An abrupt crash or disconnect cannot publish `offline`. Updates buffered during reconnect can be lost if the process exits; this bridge does not provide durable delivery or configure JetStream. See the [NATS client documentation](https://github.com/nats-io/nats.js/tree/main/core).

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `EWELINK_ACCOUNT` | required | Email or phone; `EWELINK_EMAIL` is a fallback alias. |
| `EWELINK_PASSWORD` | required | Account password. |
| `EWELINK_APP_ID` | required | Developer app ID. |
| `EWELINK_APP_SECRET` | required | Developer app secret. |
| `EWELINK_REGION` | `us` | Cloud region (`us`, `eu`, `cn`, `as`); login follows region redirects. |
| `EWELINK_AREA_CODE` | `+1` | Login area code. |
| `NATS_SERVERS` | `nats://127.0.0.1:4222` | Comma-separated server addresses. `NATS_URL` is a fallback alias. |
| `NATS_USER` | empty | Authentication username. |
| `NATS_PASS` | empty | Authentication password. |
| `NATS_TOKEN` | empty | Authentication token, instead of user/password. |
| `NATS_CREDS` | empty | Full multiline NATS JWT/NKey credentials contents, instead of user/password or token. |
| `NATS_NAME` | `ewelink-nats-bridge` | Connection name. |
| `NATS_TLS` | `false` | Require TLS with runtime trust roots. |
| `NATS_TLS_CA` | empty | PEM CA certificate contents; enables TLS. |
| `NATS_TLS_CERT` | empty | PEM client certificate contents; enables TLS and requires key. |
| `NATS_TLS_KEY` | empty | PEM client private key contents; requires certificate. |
| `NATS_CONNECT_TIMEOUT` | `10000` | Initial connection timeout in milliseconds, positive integer. |
| `NATS_RECONNECT_TIME_WAIT` | `3000` | Reconnect delay in milliseconds, nonnegative integer (client adds jitter). |
| `NATS_MAX_RECONNECT_ATTEMPTS` | `-1` | Reconnect attempts per server; `-1` unlimited, `0` disables retries. |
| `SUBJECT_PREFIX` | `ewelink` | Nonempty dot-separated subject prefix without wildcards/whitespace. |
| `PUBLISH_RAW_STATE` | `true` | Publish full update params on the raw subject. |
| `VERBOSE` | `false` | Log all eWeLink websocket packets. |
| `EXIT_ON_WEBSOCKET_CLOSE` | `true` | Exit with failure on cloud websocket closure, for container restart. |

Booleans accept `true/false`, `1/0`, `yes/no`, `on/off`. TLS certificates and credentials accept actual multiline contents, so no mounted configuration files are needed. Choose a single authentication method. Obtain eWeLink app credentials from the [developer platform](https://dev.ewelink.io/).

## Run with Node.js

Requires Node.js 22 or newer.

```sh
npm ci
cp .env.example .env
# Fill in credentials and NATS server settings in .env.
node --env-file=.env bridge.js
```

Alternatively export environment variables and run `npm start`. On PowerShell use `Copy-Item .env.example .env` and the same Node command. `.env` is optional and ignored by Git.

## Docker

```sh
docker build -t ewelink-nats-bridge:local .
docker run -d --name ewelink-nats-bridge --restart unless-stopped \
  --env-file .env ewelink-nats-bridge:local
```

Or, after filling in `.env`, run `docker compose up -d --build`. Set `NATS_SERVERS` to a broker address reachable from the container: `127.0.0.1` inside a container refers to that container. Docker's `--env-file` does not support multiline values; supply multiline credentials/certificates through environment injection or Compose's supported quoted multiline `.env` values.

The bridge reconnects automatically to NATS. Initial connection/login failures and exhausted NATS reconnect attempts exit with failure. SIGINT/SIGTERM closes the cloud websocket and drains NATS, with a five-second shutdown limit. Disabling `EXIT_ON_WEBSOCKET_CLOSE` leaves the process running after cloud closure without automatically reconnecting the cloud websocket.

## Validation

```sh
npm test
node --check bridge.js
```

## GitHub Actions

The workflows follow the MQTT bridge setup:

- `.github/workflows/build-check.yml`: runs on pull requests, pushes to `main`, and manual dispatch. Installs dependencies with Node.js 24, checks JavaScript syntax, runs tests, and builds the Docker image for `linux/amd64` and `linux/arm64` without publishing.
- `.github/workflows/docker-release.yml`: publishes a Docker Hub image for pushed tags matching `v*`, using the Git tag as the image tag (for example, `v1.0.0`). It builds both `linux/amd64` and `linux/arm64`; it does not publish a `latest` tag.

Configure these GitHub repository secrets before releasing:

| Secret | Description |
| --- | --- |
| `DOCKERHUB_USERNAME` | Docker Hub login username. |
| `DOCKERHUB_TOKEN` | Docker Hub access token with push permission. |
| `DOCKERHUB_NAMESPACE` | Docker Hub user or organization owning the image. |

Published image: `<DOCKERHUB_NAMESPACE>/ewelink-nats-bridge:<git-tag>`.

## License

Apache License 2.0; see [LICENSE](LICENSE). Based on the sibling eWeLink MQTT bridge.

