// server.js (or index.js)
const express = require("express");
const bodyParser = require("body-parser");

const app = express();
app.use(bodyParser.json());

const meetings = {}; // in-memory store

// handle zoom webhook
app.post("/zoom", (req, res) => {
  const { event, payload } = req.body;
  const meetingId = payload?.object?.id;
  const hostId = payload?.object?.host_id;
  const participant = payload?.object?.participant;

  if (!meetingId) return res.status(400).json({ error: "missing meetingId" });

  if (!meetings[meetingId]) {
    meetings[meetingId] = {
      hostId,
      participants: {},
      updatedAt: null,
    };
  }

  // participant joined
  if (event === "meeting.participant_joined") {
    meetings[meetingId].participants[participant.user_id] = {
      id: participant.user_id,
      name: participant.user_name,
      email: participant.email,
      joined_at: participant.join_time || new Date().toISOString(),
      last_seen_at: new Date().toISOString(),
    };
    meetings[meetingId].updatedAt = new Date().toISOString();
  }

  // participant left
  if (event === "meeting.participant_left") {
    delete meetings[meetingId].participants[participant.user_id];
    meetings[meetingId].updatedAt = new Date().toISOString();
  }

  res.json({ ok: true });
});

// check meeting status
app.get("/meetings/:id/status", (req, res) => {
  const { id } = req.params;
  const hostId = req.query.hostId;
  const meeting = meetings[id];

  if (!meeting) {
    return res.json({
      meetingId: id,
      hostId,
      updatedAt: null,
      presentCount: 0,
      outcome: "NONE",
      hostPresent: false,
      someoneElsePresent: false,
      person: null,
    });
  }

  const participants = Object.values(meeting.participants || {});
  const hostPresent = !!participants.find(p => p.id === hostId);
  const others = participants.filter(p => p.id !== hostId);

  let outcome = "NONE";
  if (hostPresent && others.length === 0) outcome = "HOST_ONLY";
  else if (!hostPresent && others.length > 0) outcome = "PARTICIPANT_ONLY";
  else if (!hostPresent && others.length === 0) outcome = "NONE";
  else if (hostPresent && others.length > 0) outcome = "BOTH";

  res.json({
    meetingId: id,
    hostId,
    updatedAt: meeting.updatedAt,
    presentCount: participants.length,
    outcome,
    hostPresent,
    someoneElsePresent: others.length > 0,
    person: others[0] || null,
  });
});

app.listen(3000, () => console.log("Server running on 3000"));
