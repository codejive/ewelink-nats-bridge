'use strict';
const {randomUUID} = require('node:crypto');

async function openLeaseBucket(nc, config) {
  const {Kvm} = require('@nats-io/kv');
  try {
    const kv = await new Kvm(nc).open(config.bucket);
    const status = await kv.status();
    const stream = status.streamInfo.config;
    if (status.ttl !== config.duration || status.history !== 1 || stream.mirror || stream.sources?.length || stream.subjects?.length !== 1 || stream.subjects[0] !== `$KV.${config.bucket}.>` || stream.allow_msg_ttl || stream.subject_delete_marker_ttl || stream.max_msgs !== -1 || stream.max_bytes !== -1) {
      throw new Error('bucket must have matching TTL, history 1, unlimited capacity, a local KV subject and no per-message TTL/markers, mirror or sources');
    }
    return kv;
  } catch (err) {
    throw new Error(`Failover bucket ${config.bucket}: ${err.message}. Provision the dedicated bucket and grant JetStream/KV permissions.`, {cause: err});
  }
}

class Lease {
  constructor(kv, config, onLost, id = randomUUID()) {
    this.kv = kv; this.config = config; this.onLost = onLost; this.id = id;
    this.value = Buffer.from(id); this.revision = 0; this.stopped = false;
    this.pending = null; this.timer = null;
  }
  get owned() { return !this.stopped && this.revision > 0; }
  async acquire() {
    if (this.stopped) return false;
    try {
      const revision = await this.kv.create(this.config.key, this.value);
      if (this.stopped) return false; // An ambiguous/delayed write is left to expire.
      this.revision = revision;
      this.schedule();
      return true;
    } catch (err) {
      if (err.code === 10071 || err.code === 10164) return false;
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
    this.pending = (async () => {
      try {
        const revision = await this.kv.update(this.config.key, this.value, this.revision);
        if (!this.stopped) { this.revision = revision; this.schedule(); }
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
    this.stopped = true; this.revision = 0; clearTimeout(this.timer);
  }
  async stopRenewals() {
    clearTimeout(this.timer);
    // Wait for a confirmed in-flight revision before freezing ownership.
    if (this.pending) await this.pending;
    clearTimeout(this.timer);
    this.stopped = true;
  }
  async release() {
    const revision = this.revision;
    this.invalidate();
    if (revision) await this.kv.delete(this.config.key, {previousSeq: revision});
  }
}
module.exports = {Lease, openLeaseBucket};
