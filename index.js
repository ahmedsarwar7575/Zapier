import "dotenv/config";
import express from "express";
import crypto from "crypto";
import IORedis from "ioredis";
import { Queue, Worker } from "bullmq";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

const redis = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null });
const queue = new Queue("meeting-check", { connection: redis });

const P = (id) => `presence:${id}`;
const K_STARTED = (id) => `started:${id}`;
const K_NOTIF = (id) => `notified:${id}`;
const K_RUSH = (id) => `rushed:${id}`;
const TTL = 60 * 60 * 24;

const keyFrom = (p = {}) => {
  const a = p?.id;
  if (a !== undefined && a !== null && String(a).trim() !== "") return String(a).trim();
  const b = p?.user_id;
  if (b !== undefined && b !== null && String(b).trim() !== "") return String(b).trim();
  const c = p?.email;
  if (c && String(c).trim() !== "") return String(c).toLowerCase().trim();
  const d = p?.user_name;
  if (d && String(d).trim() !== "") return String(d).trim();
  return "";
};

const nowISO = () => new Date().toISOString();
const isHost = (p, hostId) =>
  (p?.id && String(p.id) === String(hostId)) ||
  p?.is_host === true ||
  String(p?.role || "").toLowerCase() === "host";

const scheduleCheck = (meetingId, hostId, topic, start_time, timezone, delayMs) =>
  queue.add(
    "check",
    { meetingId, hostId, topic, start_time, timezone, zapHook: process.env.ZAPIER_OUTCOME_WEBHOOK },
    { delay: Number(delayMs), removeOnComplete: true, removeOnFail: true }
  );

async function getState(meetingId) {
  const raw = await redis.get(P(meetingId));
  return raw ? JSON.parse(raw) : { present: {}, hostId: null, updatedAt: null };
}
async function setState(meetingId, state) {
  state.updatedAt = nowISO();
  await redis.set(P(meetingId), JSON.stringify(state), "EX", TTL);
}

function classify(state, hostIdParam) {
  const present = state.present || {};
  const hostKey = state.hostAccountKey || hostIdParam || state.hostId || null;
  const hostPresent = (hostKey && present[hostKey]) ? true : Object.values(present).some((p) => p.isHost);
  const someoneElsePresent = Object.entries(present).some(([k, p]) => (hostKey ? k !== hostKey : !p.isHost));
  let outcome = "BOTH_PRESENT";
  if (hostPresent && !someoneElsePresent) outcome = "ONLY_HOST";
  else if (!hostPresent && someoneElsePresent) outcome = "ONLY_PARTICIPANT";
  else if (!hostPresent && !someoneElsePresent) outcome = "NONE";
  const person = state.latest_non_host || state.first_non_host || null;
  return { outcome, hostPresent, someoneElsePresent, person };
}

app.post("/zoom", async (req, res) => {
  if (req.body?.event === "endpoint.url_validation") {
    const plainToken = req.body?.payload?.plainToken;
    const encryptedToken = crypto.createHmac("sha256", process.env.ZOOM_SECRET_TOKEN).update(plainToken).digest("hex");
    return res.status(200).json({ plainToken, encryptedToken });
  }

  const { event, payload } = req.body || {};
  const obj = payload?.object || {};
  const meetingId = obj.id;
  if (!meetingId) return res.json({ ok: true });

  if (event === "meeting.participant_joined" || event === "meeting.participant_left") {
    const state = await getState(meetingId);
    state.hostId ||= obj.host_id || null;
    state.present ||= {};
    state.first_non_host ??= null;
    state.latest_non_host ??= null;

    const p = obj.participant || {};
    const k = keyFrom(p);
    if (!k) {
      await setState(meetingId, state);
      return res.json({ ok: true });
    }

    if (event === "meeting.participant_joined") {
      const profile = {
        key: k,
        name: p.user_name ?? null,
        email: (p.email || "").toLowerCase() || null,
        joined_at: p.join_time || nowISO(),
        last_seen_at: nowISO(),
        isHost: isHost(p, state.hostId),
      };
      state.present[k] = { ...(state.present[k] || {}), ...profile };
      if (profile.isHost) state.hostAccountKey = k;
      if (!profile.isHost) {
        if (!state.first_non_host) state.first_non_host = state.present[k];
        state.latest_non_host = state.present[k];
      }

      const hostKey = state.hostAccountKey || state.hostId || null;
      const hostPresent = hostKey ? !!state.present[hostKey] : Object.values(state.present).some((pp) => pp.isHost);
      if (!profile.isHost && !hostPresent) {
        const startedCount = Number(await redis.get(K_STARTED(meetingId))) || 0;
        if (startedCount >= 1) {
          const rushed = await redis.incr(K_RUSH(meetingId));
          await redis.expire(K_RUSH(meetingId), TTL);
          if (rushed === 1) {
            await scheduleCheck(
              meetingId,
              state.hostId,
              state.topic || null,
              state.start_time || null,
              state.timezone || null,
              Number(process.env.RUSH_DELAY_MS || 10000)
            );
          }
        }
      }
    } else {
      delete state.present[k];
    }

    await setState(meetingId, state);
    return res.json({ ok: true });
  }

  if (event === "meeting.started") {
    const topic = obj.topic, hostId = obj.host_id, start_time = obj.start_time, timezone = obj.timezone;

    const s = await getState(meetingId);
    s.hostId = hostId || s.hostId;
    s.topic = topic || s.topic || null;
    s.start_time = start_time || s.start_time || null;
    s.timezone = timezone || s.timezone || null;
    await setState(meetingId, s);

    const started = await redis.incr(K_STARTED(meetingId));
    await redis.expire(K_STARTED(meetingId), TTL);

    let delay = Number(process.env.JOB_DELAY_MS || 300000);
    try {
      const { outcome } = classify(s, hostId);
      if (outcome === "ONLY_PARTICIPANT") delay = Number(process.env.RUSH_DELAY_MS || 10000);
    } catch {}

    if (started === 1) {
      await scheduleCheck(meetingId, hostId, s.topic, s.start_time, s.timezone, delay);
    }
    return res.json({ ok: true, scheduled: started === 1, delayMs: delay });
  }

  res.json({ ok: true });
});

new Worker(
  "meeting-check",
  async (job) => {
    const { meetingId, hostId, topic, start_time, timezone, zapHook } = job.data;
    const notified = await redis.incr(K_NOTIF(meetingId));
    await redis.expire(K_NOTIF(meetingId), TTL);
    if (notified !== 1) return { skipped: "already-notified" };

    const state = await getState(meetingId);
    const result = classify(state, hostId);

    if (zapHook) {
      await fetch(zapHook, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          meetingId,
          topic,
          hostId,
          start_time,
          timezone,
          updatedAt: state.updatedAt,
          presentCount: Object.keys(state.present || {}).length,
          outcome: result.outcome,
          hostPresent: result.hostPresent,
          someoneElsePresent: result.someoneElsePresent,
          person: result.person,
        }),
      });
    }

    return { posted: !!zapHook, outcome: result.outcome };
  },
  { connection: redis }
);

app.listen(process.env.PORT || 3000);
