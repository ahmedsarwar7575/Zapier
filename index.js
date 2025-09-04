import 'dotenv/config';
import express from 'express';
import crypto from 'crypto';
import IORedis from 'ioredis';
import { Queue, Worker } from 'bullmq';
import fetch from 'node-fetch';

const app = express();
app.use(express.json());

const redis = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null });
const queue = new Queue('meeting-check', { connection: redis });

const P = id => `presence:${id}`;
const K_STARTED = id => `started:${id}`;
const K_NOTIF   = id => `notified:${id}`;
const TTL = 60 * 60 * 24;

const keyFrom = p => (p?.email || '').toLowerCase() || p?.user_name || ''
const nowISO = () => new Date().toISOString();
const isHost = (p, hostId) =>
  (p?.id && String(p.id) === String(hostId)) ||
  p?.is_host === true || String(p?.role || '').toLowerCase() === 'host';

async function getState(meetingId) {
  const raw = await redis.get(P(meetingId));
  return raw ? JSON.parse(raw) : { present: {}, hostId: null, updatedAt: null };
}
async function setState(meetingId, state) {
  state.updatedAt = nowISO();
  await redis.set(P(meetingId), JSON.stringify(state), 'EX', TTL);
}

function classify(state, hostIdParam) {
  const present = state.present || {};
  const hostKey = state.hostAccountKey || hostIdParam || state.hostId || null;
  const hostPresent = hostKey ? !!present[hostKey] : Object.values(present).some(p => p.isHost);
  const someoneElsePresent = Object.entries(present).some(([k, p]) => (hostKey ? k !== hostKey : !p.isHost));
  let outcome = 'BOTH_PRESENT';
  if (hostPresent && !someoneElsePresent) outcome = 'ONLY_HOST';
  else if (!hostPresent && someoneElsePresent) outcome = 'ONLY_PARTICIPANT';
  else if (!hostPresent && !someoneElsePresent) outcome = 'NONE';
  const person = state.latest_non_host || state.first_non_host || null;
  return { outcome, hostPresent, someoneElsePresent, person };
}

/* Zoom URL validation */
app.post('/zoom', async (req, res) => {
  if (req.body?.event === 'endpoint.url_validation') {
    const plainToken = req.body?.payload?.plainToken;
    const encryptedToken = crypto
      .createHmac('sha256', process.env.ZOOM_SECRET_TOKEN)
      .update(plainToken)
      .digest('hex');
    return res.status(200).json({ plainToken, encryptedToken });
  }

  const { event, payload } = req.body || {};
  const obj = payload?.object || {};
  const meetingId = obj.id;
  if (!meetingId) return res.json({ ok: true });

  // participant join/left
  if (event === 'meeting.participant_joined' || event === 'meeting.participant_left') {
    const state = await getState(meetingId);
    state.hostId ||= obj.host_id || null;
    state.present ||= {};
    state.first_non_host ??= null;
    state.latest_non_host ??= null;

    const p = obj.participant || {};
    const k = keyFrom(p);
    if (!k) { await setState(meetingId, state); return res.json({ ok: true }); }

    if (event === 'meeting.participant_joined') {
      const profile = {
        key: k,
        name: p.user_name ?? null,
        email: (p.email || '').toLowerCase() || null,
        joined_at: p.join_time || nowISO(),
        last_seen_at: nowISO(),
        isHost: isHost(p, state.hostId)
      };
      state.present[k] = { ...(state.present[k] || {}), ...profile };
      if (profile.isHost) state.hostAccountKey = k;
      if (!profile.isHost) {
        if (!state.first_non_host) state.first_non_host = state.present[k];
        state.latest_non_host = state.present[k];
      }
    } else {
      delete state.present[k];
    }
    await setState(meetingId, state);
    return res.json({ ok: true });
  }

  // meeting.started → schedule check in 5 min
  if (event === 'meeting.started') {
    const topic = obj.topic, hostId = obj.host_id, start_time = obj.start_time, timezone = obj.timezone;
    const started = await redis.incr(K_STARTED(meetingId));
    await redis.expire(K_STARTED(meetingId), TTL);
    if (started === 1) {
      await queue.add(
        'check',
        { meetingId, hostId, topic, start_time, timezone, zapHook: process.env.ZAPIER_OUTCOME_WEBHOOK },
        { delay: Number(process.env.JOB_DELAY_MS || 300000), removeOnComplete: true, removeOnFail: true }
      );
    }
    return res.json({ ok: true, scheduled: started === 1 });
  }

  res.json({ ok: true });
});

/* Worker → compute & POST to Zapier once */
new Worker(
  'meeting-check',
  async job => {
    const { meetingId, hostId, topic, start_time, timezone, zapHook } = job.data;
    const notified = await redis.incr(K_NOTIF(meetingId));
    await redis.expire(K_NOTIF(meetingId), TTL);
    if (notified !== 1) return { skipped: 'already-notified' };

    const state = await getState(meetingId);
    const result = classify(state, hostId);

    await fetch(zapHook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        meetingId,
        topic,
        hostId,
        start_time,
        timezone,
        updatedAt: state.updatedAt,
        presentCount: Object.keys(state.present || {}).length,
        outcome: result.outcome,               // ONLY_HOST | ONLY_PARTICIPANT | NONE | BOTH_PRESENT
        hostPresent: result.hostPresent,
        someoneElsePresent: result.someoneElsePresent,
        person: result.person                  // latest/first non-host details (name/email/key/…)
      })
    });

    return { posted: true, outcome: result.outcome };
  },
  { connection: redis }
);

app.listen(process.env.PORT || 3000);
