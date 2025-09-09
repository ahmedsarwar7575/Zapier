// server.js
import "dotenv/config";
import express from "express";
import crypto from "crypto";
import IORedis from "ioredis";
import { Queue, Worker } from "bullmq";
import fetch from "node-fetch";

/**
 * What this file guarantees:
 *  - Classifies presence and Waiting Room; returns outcome "WaitingRoom" if anyone is waiting.
 *  - Schedules notifications at (scheduled_start_time + 5 minutes), not at actual host start.
 *    -> Example: meeting scheduled 5:00 PM, host starts 5:30 PM (or never) => notification 5:05 PM.
 *  - Idempotent: multiple schedules (created/updated/started) won't double-post.
 */

const app = express();
app.use(express.json());

// ----------------------------------------------------------------------------
// Redis / BullMQ
// ----------------------------------------------------------------------------
const redis = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null });
const queue = new Queue("meeting-check", { connection: redis });

// ----------------------------------------------------------------------------
// Webhook endpoints to post results to (first + second, both optional)
// ----------------------------------------------------------------------------
const ZAP_ONE = process.env.ZAPIER_OUTCOME_WEBHOOK_ONE || "";
const ZAP_TWO = process.env.ZAPIER_OUTCOME_WEBHOOK_TWO || "";

// ----------------------------------------------------------------------------
// Timing (defaults: 5 minutes after scheduled start)
// ----------------------------------------------------------------------------
const DEFAULT_JOB_DELAY_MS = Number(process.env.JOB_DELAY_MS ?? 300_000); // 5m

// ----------------------------------------------------------------------------
// Redis keys / helpers
// ----------------------------------------------------------------------------
const P = (id) => `presence:${id}`;                         // full state per meeting
const K_NOTIF = (id, hookKey) => `notified:${id}:${hookKey}`; // per-webhook dedupe
const TTL = 60 * 60 * 24; // 24h

const nowISO = () => new Date().toISOString();

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

const isHost = (p, hostId) =>
  (p?.id && String(p.id) === String(hostId)) ||
  p?.is_host === true ||
  String(p?.role || "").toLowerCase() === "host";

async function getState(meetingId) {
  const raw = await redis.get(P(meetingId));
  return raw
    ? JSON.parse(raw)
    : {
        present: {},            // { [key]: profile }
        waiting: {},            // { [key]: profile }
        hostId: null,
        hostAccountKey: null,
        topic: null,
        scheduled_start_time: null, // ISO string from meeting.created/updated
        start_time: null,           // actual meeting.started time (ISO) if any
        timezone: null,
        first_non_host: null,
        latest_non_host: null,
        updatedAt: null,
      };
}

async function setState(meetingId, state) {
  state.updatedAt = nowISO();
  await redis.set(P(meetingId), JSON.stringify(state), "EX", TTL);
}

/**
 * Determine overall outcome.
 * Priority: if anyone is in Waiting Room => "WaitingRoom"
 */
function classify(state, hostIdParam) {
  const present = state.present || {};
  const waiting = state.waiting || {};

  const hostKey = state.hostAccountKey || hostIdParam || state.hostId || null;
  const hostPresent = hostKey
    ? !!present[hostKey]
    : Object.values(present).some((p) => p.isHost);

  const someoneElsePresent = Object.entries(present).some(([k, p]) =>
    hostKey ? k !== hostKey : !p.isHost
  );

  const waitingCount = Object.keys(waiting).length;
  if (waitingCount > 0) {
    const person = Object.values(waiting)[0] || null; // representative waiter
    return { outcome: "WaitingRoom", hostPresent, someoneElsePresent, person, waitingCount };
  }

  let outcome = "BOTH_PRESENT";
  if (hostPresent && !someoneElsePresent) outcome = "ONLY_HOST";
  else if (!hostPresent && someoneElsePresent) outcome = "ONLY_PARTICIPANT";
  else if (!hostPresent && !someoneElsePresent) outcome = "NONE";

  const person = state.latest_non_host || state.first_non_host || null;
  return { outcome, hostPresent, someoneElsePresent, person, waitingCount: 0 };
}

const hookKey = (url) => {
  if (!url) return "none";
  if (ZAP_ONE && url === ZAP_ONE) return "one";
  if (ZAP_TWO && url === ZAP_TWO) return "two";
  return crypto.createHash("md5").update(url).digest("hex").slice(0, 6);
};

const scheduleCheck = (
  meetingId,
  hostId,
  topic,
  scheduled_start_time, // ISO string (preferred)
  start_time,            // actual start if any
  timezone,
  zapHook,
  delayMs,
  reason
) =>
  queue.add(
    `check:${hookKey(zapHook)}`,
    {
      meetingId,
      hostId,
      topic,
      scheduled_start_time,
      start_time,
      timezone,
      zapHook,
      webhookKey: hookKey(zapHook),
      reason: reason || "regular",
    },
    { delay: Number(delayMs), removeOnComplete: true, removeOnFail: true }
  );

function msUntilFiveAfter(tsISO) {
  const base = Date.parse(tsISO || new Date().toISOString());
  const fireAt = base + DEFAULT_JOB_DELAY_MS;
  return Math.max(0, fireAt - Date.now());
}

// ----------------------------------------------------------------------------
// Routes
// ----------------------------------------------------------------------------
app.post("/zoom", async (req, res) => {
  // URL Validation for Webhook-Only app
  if (req.body?.event === "endpoint.url_validation") {
    const plainToken = req.body?.payload?.plainToken;
    const encryptedToken = crypto
      .createHmac("sha256", process.env.ZOOM_SECRET_TOKEN)
      .update(plainToken)
      .digest("hex");
    return res.status(200).json({ plainToken, encryptedToken });
  }

  const { event, payload } = req.body || {};
  const obj = payload?.object || {};
  const meetingId = obj.id;
  if (!meetingId) return res.json({ ok: true, skip: "no-meeting-id" });

  // --------------------------------------------------------------------------
  // Waiting Room related events
  // --------------------------------------------------------------------------
  const WR_JOIN_EVENTS = new Set([
    "meeting.participant_joined_waiting_room",
    "meeting.participant_jbh_waiting",         // joined before host (waiting for host)
    "meeting.participant_put_in_waiting_room", // moved back to WR during meeting
  ]);

  const WR_LEAVE_EVENTS = new Set([
    "meeting.participant_left_waiting_room",
    "meeting.participant_admitted", // host admits from WR
  ]);

  if (WR_JOIN_EVENTS.has(event)) {
    const state = await getState(meetingId);
    state.waiting ||= {};
    const p = obj.participant || {};
    const k = keyFrom(p);

    if (k) {
      const profile = {
        key: k,
        name: p.user_name ?? null,
        email: (p.email || "").toLowerCase() || null,
        waiting_since: p.join_time || nowISO(),
        isHost: isHost(p, state.hostId),
        jbh: event === "meeting.participant_jbh_waiting",
      };
      state.waiting[k] = { ...(state.waiting[k] || {}), ...profile };
    }

    await setState(meetingId, state);
    return res.json({ ok: true, event });
  }

  if (WR_LEAVE_EVENTS.has(event)) {
    const state = await getState(meetingId);
    state.waiting ||= {};
    const p = obj.participant || {};
    const k = keyFrom(p);
    if (k) delete state.waiting[k];
    await setState(meetingId, state);
    return res.json({ ok: true, event });
  }

  // --------------------------------------------------------------------------
  // Participant joined/left the actual meeting
  // (No scheduling here—notifications are based on scheduled start.)
  // --------------------------------------------------------------------------
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
      return res.json({ ok: true, skip: "no-participant-key" });
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

      // If they were waiting, clear that record
      if (state.waiting) delete state.waiting[k];

      if (profile.isHost) state.hostAccountKey = k;
      if (!profile.isHost) {
        if (!state.first_non_host) state.first_non_host = state.present[k];
        state.latest_non_host = state.present[k];
      }
    } else {
      delete state.present[k];
    }

    await setState(meetingId, state);
    return res.json({ ok: true, event });
  }

  // --------------------------------------------------------------------------
  // Meeting created / updated: store scheduled start and schedule checks
  // --------------------------------------------------------------------------
  if (event === "meeting.created" || event === "meeting.updated") {
    const topic = obj.topic;
    const hostId = obj.host_id;
    const scheduled_start_time = obj.start_time; // ISO (UTC) per Zoom payload
    const timezone = obj.timezone;

    const s = await getState(meetingId);
    s.hostId = hostId || s.hostId;
    s.topic = topic || s.topic || null;
    s.scheduled_start_time = scheduled_start_time || s.scheduled_start_time || null;
    s.timezone = timezone || s.timezone || null;
    await setState(meetingId, s);

    if (scheduled_start_time) {
      const delay = msUntilFiveAfter(scheduled_start_time);
      if (ZAP_ONE)
        await scheduleCheck(meetingId, hostId, s.topic, s.scheduled_start_time, s.start_time, s.timezone, ZAP_ONE, delay, event);
      if (ZAP_TWO)
        await scheduleCheck(meetingId, hostId, s.topic, s.scheduled_start_time, s.start_time, s.timezone, ZAP_TWO, delay, event);
    }

    return res.json({ ok: true, scheduled: !!scheduled_start_time, event });
  }

  // --------------------------------------------------------------------------
  // Meeting started (optional: store actual start; we DO NOT base the time on this)
  // --------------------------------------------------------------------------
  if (event === "meeting.started") {
    const topic = obj.topic,
      hostId = obj.host_id,
      start_time = obj.start_time, // actual start (ISO)
      timezone = obj.timezone;

    const s = await getState(meetingId);
    s.hostId = hostId || s.hostId;
    s.topic = topic || s.topic || null;
    s.start_time = start_time || s.start_time || null;
    s.timezone = timezone || s.timezone || null;
    await setState(meetingId, s);

    // Fallback scheduling ONLY if we don't know the scheduled start:
    if (!s.scheduled_start_time) {
      const delay = msUntilFiveAfter(start_time);
      if (ZAP_ONE)
        await scheduleCheck(meetingId, hostId, s.topic, s.scheduled_start_time, s.start_time, s.timezone, ZAP_ONE, delay, "started-fallback");
      if (ZAP_TWO)
        await scheduleCheck(meetingId, hostId, s.topic, s.scheduled_start_time, s.start_time, s.timezone, ZAP_TWO, delay, "started-fallback");
    }

    return res.json({ ok: true, scheduledFallback: !s.scheduled_start_time });
  }

  // Unknown/unhandled event
  return res.json({ ok: true, event, note: "unhandled-event-type (ignored)" });
});

// ----------------------------------------------------------------------------
// Worker: runs the classification and posts the outcome
// ----------------------------------------------------------------------------
new Worker(
  "meeting-check",
  async (job) => {
    const {
      meetingId,
      hostId,
      topic,
      scheduled_start_time: jobSched,
      start_time: jobActualStart,
      timezone,
      zapHook,
      webhookKey,
      reason,
    } = job.data;

    const state = await getState(meetingId);

    // Choose the best known scheduled start (prefer job payload, then state)
    const scheduledISO =
      jobSched ||
      state.scheduled_start_time ||
      null;

    // If we have a scheduled start, enforce "not before scheduled+5m"
    if (scheduledISO) {
      const minFireAt = Date.parse(scheduledISO) + DEFAULT_JOB_DELAY_MS;
      const now = Date.now();
      if (now < minFireAt) {
        // Too early (perhaps meeting time was moved) → re-enqueue for the remaining delay
        const delay = Math.max(0, minFireAt - now);
        await scheduleCheck(
          meetingId,
          hostId,
          topic,
          scheduledISO,
          jobActualStart || state.start_time,
          timezone,
          zapHook,
          delay,
          "rescheduled-to-scheduled+5m"
        );
        return { rescheduled: true, webhook: webhookKey, delay };
      }
    }

    // De-dupe just before posting
    const notified = await redis.incr(K_NOTIF(meetingId, webhookKey));
    await redis.expire(K_NOTIF(meetingId, webhookKey), TTL);
    if (notified !== 1) return { skipped: "already-notified", webhook: webhookKey, reason };

    const result = classify(state, hostId);

    if (zapHook) {
      await fetch(zapHook, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          meetingId,
          topic,
          hostId,
          scheduled_start_time: scheduledISO,
          start_time: jobActualStart || state.start_time,
          timezone,
          updatedAt: state.updatedAt,
          presentCount: Object.keys(state.present || {}).length,
          outcome: result.outcome,               // includes "WaitingRoom" when applicable
          hostPresent: result.hostPresent,
          someoneElsePresent: result.someoneElsePresent,
          person: result.person,
          waitingCount: result.waitingCount ?? Object.keys(state.waiting || {}).length,
          webhook: webhookKey,
          reason,
        }),
      });
    }

    return { posted: !!zapHook, webhook: webhookKey, outcome: result.outcome, reason };
  },
  { connection: redis, concurrency: 1 }
);

// ----------------------------------------------------------------------------
// Boot
// ----------------------------------------------------------------------------
app.listen(process.env.PORT || 3000, () => {
  console.log(`Server listening on :${process.env.PORT || 3000}`);
});
