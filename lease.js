'use strict';
const {randomUUID} = require('node:crypto');

function validateLeaseBucket(status, config) {
  const stream = status.streamInfo.config;
  const mismatches = [];
  const check = (name, actual, expected) => {
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
      mismatches.push(`${name}: expected ${JSON.stringify(expected)}, actual ${actual === undefined ? 'missing' : JSON.stringify(actual)}`);
    }
  };
  check('TTL (milliseconds; FAILOVER_LEASE_DURATION)', status.ttl, config.duration);
  check('history (max_msgs_per_subject)', status.history, 1);
  check('subjects', stream.subjects, [`$KV.${config.bucket}.>`]);
  check('max_msgs', stream.max_msgs, -1);
  if (stream.max_bytes !== -1 && (!Number.isSafeInteger(stream.max_bytes) || stream.max_bytes < 1024)) {
    mismatches.push(`max_bytes: expected -1 (unlimited) or at least 1024 bytes, actual ${stream.max_bytes === undefined ? 'missing' : JSON.stringify(stream.max_bytes)}`);
  }
  check('mirror', stream.mirror ?? null, null);
  check('sources', stream.sources ?? [], []);
  check('allow_msg_ttl', stream.allow_msg_ttl ?? false, false);
  check('subject_delete_marker_ttl (nanoseconds)', stream.subject_delete_marker_ttl ?? 0, 0);
  if (mismatches.length) throw new Error(`incompatible bucket settings: ${mismatches.join('; ')}`);
}

async function openLeaseBucket(nc, config) {
  const {Kvm} = require('@nats-io/kv');
  try {
    const kv = await new Kvm(nc).open(config.bucket);
    const status = await kv.status();
    validateLeaseBucket(status, config);
    return kv;
  } catch (err) {
    throw new Error(`Failover bucket ${config.bucket}: ${err.message}. Provision the dedicated bucket and grant JetStream/KV permissions.`, {cause: err});
  }
}

class Lease {
  constructor(kv, config, onLost, id = randomUUID(), log = () => {}) {
    this.kv = kv; this.config = config; this.onLost = onLost; this.id = id;
    this.value = Buffer.from(id); this.revision = 0; this.stopped = false;
    this.pending = null; this.timer = null;
    this.log = (message, debug = false) => log(`[${this.id}] Lease ${this.config.bucket}/${this.config.key}: ${message}`, debug);
  }
  get owned() { return !this.stopped && this.revision > 0; }
  async acquire() {
    if (this.stopped) return false;
    this.log('attempting atomic acquisition', true);
    try {
      const revision = await this.kv.create(this.config.key, this.value);
      if (this.stopped) {
        this.log(`discarding delayed acquisition revision ${revision}; leaving lease to expire`);
        return false;
      }
      this.revision = revision;
      this.log(`acquired revision ${revision}`, true);
      this.schedule();
      return true;
    } catch (err) {
      if (err.code === 10071 || err.code === 10164) {
        this.log('already owned; remaining on standby', true);
        return false;
      }
      this.log(`acquisition failed or uncertain; any committed lease will expire: ${err.message}`);
      throw err;
    }
  }
  schedule() {
    this.timer = setTimeout(() => { this.renew(); }, this.config.renewal);
  }
  renew() {
    if (this.pending) return this.pending;
    if (!this.owned) return Promise.resolve();
    clearTimeout(this.timer);
    this.log(`renewing revision ${this.revision}`, true);
    this.pending = (async () => {
      try {
        const revision = await this.kv.update(this.config.key, this.value, this.revision);
        if (!this.stopped) {
          this.log(`renewed revision ${this.revision} -> ${revision}`, true);
          this.revision = revision; this.schedule();
        } else this.log(`discarding delayed renewal revision ${revision}; ownership remains invalid`);
      } catch (err) {
        if (!this.stopped) {
          this.invalidate();
          this.onLost(err);
        }
      } finally { this.pending = null; }
    })();
    return this.pending;
  }
  invalidate() {
    if (this.revision) this.log(`invalidating local ownership at revision ${this.revision}`, true);
    this.stopped = true; this.revision = 0; clearTimeout(this.timer);
  }
  async stopRenewals() {
    this.log(`stopping renewals${this.pending ? '; waiting for in-flight renewal' : ''}`, true);
    clearTimeout(this.timer);
    // Wait for a confirmed in-flight revision before freezing ownership.
    if (this.pending) await this.pending;
    clearTimeout(this.timer);
    this.stopped = true;
  }
  async release() {
    const revision = this.revision;
    this.invalidate();
    if (!revision) {
      this.log('release skipped: no confirmed ownership; any remaining lease will expire');
      return;
    }
    this.log(`releasing with revision-checked delete at revision ${revision}`, true);
    await this.kv.delete(this.config.key, {previousSeq: revision});
    this.log(`release confirmed at revision ${revision}`, true);
  }
}
module.exports = {Lease, openLeaseBucket, validateLeaseBucket};
