# eWeLink to NATS Bridge

Adapted from [ewelink-mqtt-bridge](https://github.com/codejive/ewelink-mqtt-bridge), this bridge authenticates to eWeLink cloud and republishes live websocket device updates to Core [NATS](https://nats.io/). Configuration uses only environment variables; no configuration file is required.

Looking for the MQTT version of this bridge? Check out [ewelink-mqtt-bridge](https://github.com/codejive/ewelink-mqtt-bridge).

## How It Works

1. Connect to NATS.
2. Authenticate to eWeLink cloud using app credentials and account credentials, then open a persistent websocket to receive live device update events.
3. Publish each update to NATS subjects using a predictable subject structure.

## Subjects and payloads

- `ewelink.<deviceId>.state.<key>`: individual value (objects/arrays as JSON, scalars as text, null as empty text).
- `ewelink.<deviceId>.state.raw`: complete update event as JSON (when `PUBLISH_RAW_STATE` is set to `true` or `only`).

For example, `{"temperature":22.5,"humidity":48}` publishes a raw JSON message and two individual messages, `22.5` and `48`. Dots, whitespace and NATS wildcards in device IDs or parameter names become underscores. Subscribe to `ewelink.>` for all messages.

Core NATS delivers messages to current subscribers without retention or MQTT QoS/last-will semantics. Late subscribers do not receive previous state. Updates buffered during reconnect can be lost if the process exits; this bridge does not provide durable delivery. Optional failover uses an externally provisioned JetStream KV bucket only for leadership. See the [NATS client documentation](https://github.com/nats-io/nats.js/tree/main/core).

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
| `PUBLISH_RAW_STATE` | `true` | `true`: publish raw JSON and individual state keys; `false`: individual keys only; `only`: raw JSON only. |
| `VERBOSE` | `false` | Log all eWeLink websocket packets and detailed failover acquisition/retry, renewal revision, bucket validation and shutdown progress. |
| `EXIT_ON_WEBSOCKET_CLOSE` | `true` | Exit with failure on cloud websocket closure, for container restart. |

Booleans accept `true/false`, `1/0`, `yes/no`, `on/off`. TLS certificates and credentials accept actual multiline contents, so no mounted configuration files are needed. Choose a single authentication method. Obtain eWeLink app credentials from the [developer platform](https://dev.ewelink.io/).

## Getting App Credentials

This bridge requires your own eWeLink app credentials:

1. **Register an eWeLink Developer Account**
   - Visit [eWeLink Developer Platform](https://dev.ewelink.io/)
   - Create an account or log in

2. **Create an Application**
   - Navigate to the applications/credentials section
   - Create a new application
   - You'll receive an `APP_ID` and `APP_SECRET`

3. **Configure the Bridge**
   - Add `EWELINK_APP_ID` and `EWELINK_APP_SECRET` to your `.env` file or pass them as environment variables
   - Set `EWELINK_ACCOUNT` to your eWeLink email or phone number

If you're unable to obtain credentials, check the [eWeLink API Next documentation](https://www.npmjs.com/package/ewelink-api-next) or the [eWeLink community forums](https://www.ewelink.cc/).

## Usage

Copy `.env.example` to `.env`, fill in your credentials, and set `NATS_SERVERS` to a broker address reachable from the container (not `127.0.0.1`). Replace `<version>` below with the exact tag of a [published release](https://github.com/codejive/ewelink-nats-bridge/releases), including the `v` prefix. Normally, use the latest release:

```sh
docker run -d --name ewelink-nats-bridge --restart unless-stopped \
  --env-file .env codejive/ewelink-nats-bridge:<version>
```

## Developing

### Run with Node.js

Requires Node.js 22 or newer.

```sh
npm ci
cp .env.example .env
# Fill in credentials and NATS server settings in .env.
node --env-file=.env bridge.js
```

Alternatively export environment variables and run `npm start`. On PowerShell use `Copy-Item .env.example .env` and the same Node command. `.env` is optional and ignored by Git.

### Docker

```sh
docker build -t ewelink-nats-bridge:local .
docker run -d --name ewelink-nats-bridge --restart unless-stopped \
  --env-file .env ewelink-nats-bridge:local
```

Or, after filling in `.env`, run `docker compose up -d --build`. Set `NATS_SERVERS` to a broker address reachable from the container: `127.0.0.1` inside a container refers to that container. Docker's `--env-file` does not support multiline values; supply multiline credentials/certificates through environment injection or Compose's supported quoted multiline `.env` values.   

### Validation

```sh
npm test
node --check bridge.js
```

## Operational Notes

### Optional active/standby failover

Failover is disabled by default. Enabling it requires JetStream on the existing NATS server and an **externally provisioned dedicated KV bucket**. The bridge opens and validates that bucket; it never creates or modifies server resources. The KV dependency matches the installed NATS 3.x client API; see the [NATS KV documentation](https://github.com/nats-io/nats.js/blob/main/kv/README.md).

Provision once using the NATS CLI with administrator credentials (these commands are instructions only):

```sh
nats --server "$NATS_SERVERS" kv add ewelink_leases --history=1 --ttl=30s --storage=file --replicas=1
```

The bucket must have history 1 and a TTL exactly matching `FAILOVER_LEASE_DURATION`. Its backing stream must use only `$KV.ewelink_leases.>`, unlimited total messages, and either unlimited bytes (`max_bytes=-1`) or a byte limit of at least 1024 (1 KiB). It must have no mirror, sources, per-message TTL or automatic delete markers. A 1 KiB limit is sufficient for a single UUID lease with message overhead; allow more capacity when sharing the bucket across multiple account keys. Use a separate bucket per account when different lease durations are needed. Bucket permission, configuration and JetStream availability failures stop startup with a diagnostic; contention simply remains on standby.

| Variable | Default | Description |
| --- | --- | --- |
| `FAILOVER_ENABLED` | `false` | Opt in to active/standby leadership. |
| `FAILOVER_BUCKET` | `ewelink_leases` | Externally provisioned dedicated KV bucket. |
| `FAILOVER_LEASE_KEY` | `bridge` | Same key on both VPSes for the same account; different keys for different accounts. |
| `FAILOVER_LEASE_DURATION` | `30000` | Bucket TTL in milliseconds, integer at least 1000. |
| `FAILOVER_RENEWAL_INTERVAL` | `5000` | Milliseconds between serialized renewal attempts; positive integer strictly shorter than TTL. |
| `FAILOVER_RETRY_INTERVAL` | `2000` | Standby acquisition retry interval in milliseconds, positive integer, with ±20% jitter. |

On both VPSes, use the same eWeLink credentials, external `NATS_SERVERS`, subject prefix, bucket, key and timing settings; set `FAILOVER_ENABLED=true`. Run both containers with `--restart unless-stopped` (or the provided Compose restart policy). Both applications subscribe to the same external Core NATS subjects. Each bridge process generates a unique UUID and logs standby/active transitions with that ID. A recovered bridge remains on standby while another owns the key.

Runtime credentials need the existing device publish permissions and JetStream account/stream information and KV operations. For the default bucket, allow requests to `$JS.API.INFO`, `$JS.API.STREAM.INFO.KV_ewelink_leases`, and `$JS.API.STREAM.MSG.GET.KV_ewelink_leases`; allow publish to `$KV.ewelink_leases.bridge` (or all configured keys) and subscribe to reply inboxes `_INBOX.>`. KV create may read existing delete markers through `$JS.API.DIRECT.GET.KV_ewelink_leases.>`; permit those requests as well. Adapt API prefixes if using a JetStream domain. No stream creation, consumer creation, or unconditional stream purge permission is needed. Have your NATS administrator apply permissions; this implementation does not change external configuration.

Leadership uses atomic KV create and revision-checked updates/deletes. NATS server time controls expiry; VPS clocks are never used to decide ownership. Renewals begin immediately after acquisition, including while login is pending. Device subjects and payloads retain the current behavior.

Lease logs include the process UUID and bucket/key. Routine standby contention, successful renewals, ownership invalidation, renewal stopping and release attempts/confirmation are logged only with `VERBOSE=true`. Acquisition, skipped release and discarded delayed lease results remain visible with verbose logging disabled, along with failures and shutdown timeouts. A confirmed release log means the revision-checked KV delete was acknowledged; skipped or failed release leaves any remaining lease to server-managed expiry.

Renewal failure (including an ambiguous timeout), NATS disconnect, or source closure stops the process. Failover mode always exits on source closure, overriding `EXIT_ON_WEBSOCKET_CLOSE=false`. Container restart performs a fresh acquisition before any authentication. The installed eWeLink dependency has no automatic source reconnect. During graceful shutdown, renewals stop, pending source creation completes, and WebSocket closure is confirmed before revision-checked release. If shutdown cannot finish within five seconds, or safe release fails, the lease expires on the server instead.

After a crash or unconfirmed release, takeover normally takes the remaining TTL (up to 30 seconds by default), plus up to about 2.4 seconds for an acquisition retry, plus eWeLink login/connection time. Graceful release can shorten that delay. There is no local lease-expiration watchdog or stronger fencing: a paused process with a delayed login can briefly displace the current leader. Its subsequent closure/retry returns through fresh acquisition so the system settles back to one active bridge. Brief interruptions and lost messages are accepted. NATS remains a shared dependency; this feature does not address NATS outages or provide replay/durable delivery.

To run the integration test against a temporary local server, set `NATS_TEST_SERVER` to the absolute path of a `nats-server` executable and run `npm test`. The test launches JetStream on a random local port, validates atomic acquisition, renewal revisions, server TTL takeover and stale-release protection, then removes its temporary storage. Without that variable, only the real-server test is skipped.

- With failover disabled, the bridge reconnects automatically to NATS. With failover enabled, a disconnect stops the process for a fresh lease acquisition on restart.
- NATS server errors are logged with their full details. Allow publishing to device state subjects (or the corresponding custom prefix). Publish permissions are checked by the server when device updates are sent.
- If websocket connectivity drops, the bridge can exit and rely on container restart policy.
- Initial connection/login failures and exhausted NATS reconnect attempts exit with failure.
- SIGINT/SIGTERM closes the cloud websocket and drains NATS, with a five-second shutdown limit.
- With failover disabled, disabling `EXIT_ON_WEBSOCKET_CLOSE` leaves the process running after cloud closure without automatically reconnecting the cloud websocket.

## License

Apache License 2.0; see [LICENSE](LICENSE). Based on the sibling eWeLink MQTT bridge.
