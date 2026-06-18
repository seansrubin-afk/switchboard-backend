/* ============================================================================
   THE SWITCHBOARD v3.4 — backend orchestrator
   - Parallel outbound dialing with AMD
   - Custom voicemail drop (YOUR recorded voice, not TTS)
   - Inbound call forwarding (callbacks reach your phone)
   - Inbound call/text logging (see who called/texted back)
   ============================================================================ */

import express from "express";

const app = express();
app.use(express.json({ limit: "10mb" }));

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

const TELNYX_API_KEY   = process.env.TELNYX_API_KEY;
const CONNECTION_ID    = process.env.TELNYX_CONNECTION_ID;
let   FROM_NUMBERS      = (process.env.FROM_NUMBERS || "").split(",").map(s => s.trim()).filter(Boolean);
const PUBLIC_URL       = process.env.PUBLIC_URL;
const DEFAULT_MARKET   = (process.env.DEFAULT_MARKET || "US").toUpperCase();
const MY_PHONE         = process.env.MY_PHONE || "";

const TELNYX = "https://api.telnyx.com/v2";

// ---- Per-number reputation stats (answer-rate proxy for spam detection) ----
// Tracks calls placed vs answered per FROM number. A sharp answer-rate drop is
// the earliest in-app signal that a number may be carrier-flagged.
const numberStats = {}; // { "+1...": { placed, answered } }
function ensureNumberStats(n) { if (!numberStats[n]) numberStats[n] = { placed: 0, answered: 0 }; return numberStats[n]; }
function recordPlaced(n) { if (n) ensureNumberStats(n).placed += 1; }
function recordAnswered(n) { if (n) ensureNumberStats(n).answered += 1; }
// Warn only after a meaningful sample, so early noise doesn't false-alarm.
const SPAM_MIN_SAMPLE = 15;   // need at least this many placed calls
const SPAM_RATE_FLOOR = 0.05; // under 5% answer rate => likely flagged
function numberReputation() {
  return FROM_NUMBERS.map(n => {
    const s = numberStats[n] || { placed: 0, answered: 0 };
    const rate = s.placed > 0 ? s.answered / s.placed : null;
    const suspect = s.placed >= SPAM_MIN_SAMPLE && rate !== null && rate < SPAM_RATE_FLOOR;
    return { number: n, placed: s.placed, answered: s.answered, answerRate: rate, suspect };
  });
}

// ---- in-memory state ----
const session = { confId: null, confName: null, seanCallId: null, seanUp: false, pending: null };
const batches = new Map();
let rrIndex = 0;

// ---- BACKEND-ENFORCED CALL SAFETY (never trust the frontend) ----
// Hard caps so no business can ever be called repeatedly, regardless of any
// frontend bug, auto-dial loop, or duplicate lead in the queue.
const MAX_ATTEMPTS_PER_NUMBER = 3;        // absolute max calls to one number, ever, per server run
const MIN_SECONDS_BETWEEN_CALLS = 90;     // a number cannot be re-dialed within this window
const callHistory = {};                   // { "+1leadphone": { attempts, lastCalledAt } }
function canCall(toNumber) {
  const h = callHistory[toNumber];
  if (!h) return { ok: true };
  if (h.attempts >= MAX_ATTEMPTS_PER_NUMBER)
    return { ok: false, reason: `max ${MAX_ATTEMPTS_PER_NUMBER} attempts reached` };
  const since = (Date.now() - (h.lastCalledAt || 0)) / 1000;
  if (since < MIN_SECONDS_BETWEEN_CALLS)
    return { ok: false, reason: `called ${Math.round(since)}s ago (min ${MIN_SECONDS_BETWEEN_CALLS}s)` };
  return { ok: true };
}
function recordCallAttempt(toNumber) {
  const h = callHistory[toNumber] || { attempts: 0, lastCalledAt: 0 };
  h.attempts += 1; h.lastCalledAt = Date.now();
  callHistory[toNumber] = h;
}

// Voicemail audio (uploaded by user, stored in memory)
let voicemailAudio = null; // { buffer: Buffer, contentType: string }

// ---- SYSTEM HEALTH / ALERT TRACKING ----
// Tracks specific failures so the app can show exactly what's wrong.
const sysHealth = {
  lastBalance: null,        // last known Telnyx balance (number, USD)
  lastBalanceCheck: 0,      // timestamp of last balance poll
  consecutiveCallFailures: 0, // dial attempts that failed in a row
  lastFailureReason: null,  // human-readable reason of most recent failure
  lastFailureCode: null,    // Telnyx error code (e.g. "10015")
  lastFailureAt: null,      // ISO timestamp
  apiKeyOk: true,           // false if Telnyx rejected our key (401)
  alerts: [],               // active alert strings shown in the banner
};

// Translate a Telnyx error into a plain-English, specific reason.
function describeTelnyxError(code, detail, httpStatus) {
  const c = String(code || "");
  if (httpStatus === 401 || c === "10009") return "Telnyx API key rejected — key may be wrong, revoked, or expired.";
  if (httpStatus === 403) {
    if (/balance|fund|insufficient/i.test(detail || "")) return "Telnyx account out of funds — calls blocked until you top up.";
    return "Telnyx rejected the call (403) — usually the concurrent-call limit. Upgrade to 10 calls, or fewer simultaneous dials.";
  }
  if (c === "10015" || /balance|insufficient.*fund/i.test(detail || "")) return "Telnyx balance depleted — add funds to keep calling.";
  if (httpStatus === 422 || c === "10004") return "Bad phone number format — one or more leads have an invalid number.";
  if (httpStatus === 429) return "Telnyx rate limit hit — dialing too fast. Slow the batch pace.";
  return detail ? `Telnyx error: ${detail}` : `Telnyx error (HTTP ${httpStatus || "?"}, code ${c || "?"}).`;
}

// Poll Telnyx for the current account balance (cached ~60s).
async function checkBalance() {
  const now = Date.now();
  if (now - sysHealth.lastBalanceCheck < 60000 && sysHealth.lastBalance !== null) return sysHealth.lastBalance;
  try {
    const res = await fetch(TELNYX + "/balance", {
      headers: { Authorization: `Bearer ${TELNYX_API_KEY}` },
    });
    if (res.status === 401) { sysHealth.apiKeyOk = false; return null; }
    sysHealth.apiKeyOk = true;
    const j = await res.json();
    const bal = parseFloat(j?.data?.balance);
    if (!isNaN(bal)) { sysHealth.lastBalance = bal; sysHealth.lastBalanceCheck = now; }
    return sysHealth.lastBalance;
  } catch { return sysHealth.lastBalance; }
}

// Record a dial failure with a specific reason.
function recordCallFailure(reason, code) {
  sysHealth.consecutiveCallFailures += 1;
  sysHealth.lastFailureReason = reason;
  sysHealth.lastFailureCode = code || null;
  sysHealth.lastFailureAt = new Date().toISOString();
}
function recordCallSuccess() {
  sysHealth.consecutiveCallFailures = 0;
}

// Build the list of active alerts (what the banner shows).
function computeAlerts() {
  const alerts = [];
  if (!sysHealth.apiKeyOk) alerts.push("TELNYX API KEY REJECTED — calls cannot be placed. Check the key in Railway.");
  if (sysHealth.lastBalance !== null && sysHealth.lastBalance < 1)
    alerts.push(`TELNYX BALANCE LOW: $${sysHealth.lastBalance.toFixed(2)} — top up to keep calling.`);
  if (sysHealth.consecutiveCallFailures >= 3)
    alerts.push(`${sysHealth.consecutiveCallFailures} CALLS FAILED IN A ROW — ${sysHealth.lastFailureReason || "unknown reason"}`);
  sysHealth.alerts = alerts;
  return alerts;
}


// Inbound call/text log
const inboundLog = []; // { type: "call"|"text", from, to, timestamp, body? }

const pickFrom = () => FROM_NUMBERS[(rrIndex++) % FROM_NUMBERS.length];

// Verify a call leg is genuinely still active by asking Telnyx directly.
// Returns true only if Telnyx reports the call as active. Any error/unknown
// => false, so we fail safe toward calling Sean's phone again.
async function isLegAlive(ccid) {
  if (!ccid) return false;
  try {
    const res = await fetch(TELNYX + `/calls/${ccid}`, {
      headers: { Authorization: `Bearer ${TELNYX_API_KEY}` },
    });
    if (!res.ok) return false;
    const j = await res.json();
    // Telnyx returns call status; "active"/"bridged" mean the leg is live.
    const status = (j?.data?.call_status || j?.data?.status || "").toLowerCase();
    return status === "active" || status === "bridged" || j?.data?.is_alive === true;
  } catch { return false; }
}

const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64");
const unb64 = (s) => { try { return JSON.parse(Buffer.from(s, "base64").toString()); } catch { return {}; } };

async function telnyx(path, body, method = "POST") {
  const res = await fetch(TELNYX + path, {
    method,
    headers: { Authorization: `Bearer ${TELNYX_API_KEY}`, "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { json = { raw: text }; }
  if (json && typeof json === "object") json._httpStatus = res.status;
  if (!res.ok) {
    // Hanging up an already-ended call (90018) is expected/harmless noise when
    // we tidy up losing lines after a bridge. Don't spam the logs with it.
    const code = json?.errors?.[0]?.code;
    const isHarmlessHangup = path.endsWith("/hangup") && (code === "90018" || /already ended/i.test(text));
    if (!isHarmlessHangup) console.error("Telnyx error", res.status, path, text.slice(0, 400));
  }
  return json;
}

const action = (ccid, name, payload = {}) =>
  telnyx(`/calls/${ccid}/actions/${name}`, payload);

// Conference helpers using Telnyx's ACTUAL endpoints (verified against docs):
//  - Create:  POST /conferences { name, call_control_id }  (first leg creates it)
//  - Join:    POST /conferences/{id}/actions/join { call_control_id }
// Sean's leg creates the conference; each lead joins it by id.
async function createConferenceWithSean(seanCcid, name) {
  const r = await telnyx("/conferences", {
    name,
    call_control_id: seanCcid,
    beep_enabled: "never",
    start_conference_on_create: true,
  });
  const id = r?.data?.id || null;
  if (r?.errors) console.error("CONFERENCE CREATE FAILED:", JSON.stringify(r.errors));
  return id;
}
async function joinConference(confId, leadCcid) {
  return telnyx(`/conferences/${confId}/actions/join`, {
    call_control_id: leadCcid,
    beep_enabled: "never",
    start_conference_on_enter: false,
  });
}

// ============================================================================
//  VOICEMAIL MANAGEMENT
// ============================================================================
app.post("/voicemail", (req, res) => {
  const { audio, contentType } = req.body || {};
  if (!audio) return res.status(400).json({ error: "need audio (base64)" });
  voicemailAudio = {
    buffer: Buffer.from(audio, "base64"),
    contentType: contentType || "audio/mpeg",
  };
  console.log("Voicemail uploaded:", voicemailAudio.buffer.length, "bytes");
  res.json({ ok: true, size: voicemailAudio.buffer.length });
});

app.get("/voicemail.mp3", (req, res) => {
  if (!voicemailAudio) return res.status(404).send("No voicemail uploaded");
  res.set("Content-Type", voicemailAudio.contentType);
  res.set("Content-Length", voicemailAudio.buffer.length);
  res.send(voicemailAudio.buffer);
});

app.get("/voicemail-status", (req, res) => {
  res.json({ hasVoicemail: !!voicemailAudio, size: voicemailAudio?.buffer?.length || 0 });
});

// ============================================================================
//  SESSION START — call Sean once, hold him in a persistent conference.
//  Leads are bridged into this conference instantly, so connects have NO ring
//  delay. Sean stays on one call for the whole session.
// ============================================================================
app.post("/session/start", async (req, res) => {
  const { myPhone } = req.body || {};
  if (!myPhone) return res.status(400).json({ error: "need myPhone" });
  if (!FROM_NUMBERS.length) return res.status(500).json({ error: "no FROM_NUMBERS configured" });

  // If Sean is already verified in the conference, do nothing.
  if (session.seanUp && session.seanCallId && await isLegAlive(session.seanCallId)) {
    return res.json({ ok: true, already: true, confName: session.confName });
  }

  session.confName = "sw_" + Date.now().toString(36);
  session.confId = null;
  session.pending = null;
  console.log("Session start — calling Sean to hold in conference:", session.confName);
  const call = await telnyx("/calls", {
    connection_id: CONNECTION_ID,
    to: myPhone,
    from: pickFrom(),
    webhook_url: `${PUBLIC_URL}/webhook`,
    client_state: b64({ seanHold: true }),
  });
  session.seanCallId = call?.data?.call_control_id || null;
  if (!session.seanCallId) {
    const err = call?.errors?.[0] || {};
    const reason = describeTelnyxError(err.code, err.detail, call?._httpStatus);
    recordCallFailure(reason, err.code);
    return res.status(502).json({ error: reason });
  }
  recordCallSuccess();
  res.json({ ok: true, confName: session.confName });
});

// ============================================================================
//  DIAL A BATCH
// ============================================================================
app.post("/dial-batch", async (req, res) => {
  const { myPhone, leads, market } = req.body || {};
  if (!myPhone || !Array.isArray(leads) || leads.length === 0)
    return res.status(400).json({ error: "need myPhone and leads[]" });
  if (!FROM_NUMBERS.length) return res.status(500).json({ error: "no FROM_NUMBERS configured" });

  const mkt = (market || DEFAULT_MARKET).toUpperCase();
  const batchId = "b_" + Date.now().toString(36);

  // Sean must already be held in the conference (via /session/start). If he's
  // not verified live, tell the frontend to start a session first. We NEVER
  // dial leads without Sean already on the line — that's what caused leads to
  // reach voicemail.
  const seanLive = session.seanUp && session.seanCallId && await isLegAlive(session.seanCallId);
  if (!seanLive) {
    session.seanUp = false; session.seanCallId = null;
    return res.status(409).json({ error: "no_session", message: "Start a session first — Sean must be on the line." });
  }

  console.log("Sean held in conference — firing leads directly");
  await fireBatch(batchId, leads, mkt);
  return res.json({ batchId });
});


async function fireBatch(batchId, leads, market = DEFAULT_MARKET) {
  const lines = new Map();
  batches.set(batchId, { lines, connected: false, done: false, market });

  await Promise.all(leads.slice(0, 10).map(async (ld) => {
    // BACKEND SAFETY: refuse to call a number that's hit its cap or was just
    // called. This is the hard guarantee against calling anyone 20x in a row.
    const gate = canCall(ld.to);
    if (!gate.ok) {
      console.log("BLOCKED re-dial of", ld.to, "—", gate.reason);
      lines.set("blocked_" + ld.leadId, { leadId: ld.leadId, to: ld.to, status: "blocked", attempt: ld.attempt || 0, fromNumber: "-" });
      return;
    }
    const from = pickFrom();
    try {
      recordCallAttempt(ld.to); // count the attempt BEFORE dialing, so a crash mid-dial still counts
      const call = await telnyx("/calls", {
        connection_id: CONNECTION_ID,
        to: ld.to,
        from,
        webhook_url: `${PUBLIC_URL}/webhook`,
        client_state: b64({ batchId, leadId: ld.leadId }),
      });
      const ccid = call?.data?.call_control_id;
      if (ccid) {
        lines.set(ccid, { leadId: ld.leadId, to: ld.to, status: "dialing", attempt: ld.attempt || 0, fromNumber: from });
        recordPlaced(from);
        recordCallSuccess();
      } else {
        // Call creation failed — capture the SPECIFIC reason from Telnyx.
        const err = call?.errors?.[0] || {};
        const reason = describeTelnyxError(err.code, err.detail, call?._httpStatus);
        recordCallFailure(reason, err.code);
        console.log("Call failed for", ld.to, "—", reason);
      }
    } catch (e) {
      recordCallFailure("Network error reaching Telnyx — backend may be offline or rate-limited.", null);
      console.log("Call error for", ld.to, ":", e.message);
    }
  }));

  // If no lines were created (all calls failed), mark batch as done immediately
  if (lines.size === 0) {
    const batch = batches.get(batchId);
    if (batch) batch.done = true;
    console.log("Batch", batchId, "— all calls failed, marking done");
  }

  // Safety timeout: mark batch done after 30 seconds if it hasn't resolved
  setTimeout(() => {
    const batch = batches.get(batchId);
    if (batch && !batch.done) {
      batch.done = true;
      console.log("Batch", batchId, "— timed out, marking done");
    }
  }, 30000);
}

// ============================================================================
//  TELNYX WEBHOOK
// ============================================================================
app.post("/webhook", async (req, res) => {
  res.sendStatus(200);
  const ev = req.body?.data;
  if (!ev) return;
  const type = ev.event_type;
  const p = ev.payload || {};
  const ccid = p.call_control_id;
  const state = p.client_state ? unb64(p.client_state) : {};

  // ---- INBOUND CALL — forward to your phone ----
  if (type === "call.initiated" && p.direction === "incoming") {
    const from = p.from || p.caller_id_name || "unknown";
    const to = p.to || "";
    console.log("Inbound call from", from, "to", to);
    inboundLog.unshift({ type: "call", from, to, timestamp: new Date().toISOString() });
    if (inboundLog.length > 100) inboundLog.length = 100;

    // Forward to your phone if configured
    if (MY_PHONE && ccid) {
      await action(ccid, "answer");
      await action(ccid, "transfer", { to: MY_PHONE });
    }
    return;
  }

  // ---- DIRECT CALL: your phone answered → dial the lead immediately (using session state) ----
  if (type === "call.answered" && directCallPending && ccid === directCallPending.seanCcid) {
    console.log("Direct call: you answered! Now dialing lead", directCallPending.to);
    const call2 = await telnyx("/calls", {
      connection_id: CONNECTION_ID, to: directCallPending.to, from: directCallPending.from || FROM_NUMBERS[0],
      webhook_url: `${PUBLIC_URL}/webhook`,
    });
    const leadCcid = call2?.data?.call_control_id;
    console.log("Direct call: lead call ID =", leadCcid || "FAILED");
    if (leadCcid) directCallPending.leadCcid = leadCcid;
    return;
  }

  // ---- DIRECT BRIDGE: lead answered → bridge with Sean immediately ----
  if (type === "call.answered" && directCallPending && directCallPending.leadCcid && ccid === directCallPending.leadCcid) {
    console.log("Direct bridge: lead answered! Bridging with Sean:", directCallPending.seanCcid);
    if (!directCallPending.seanCcid) {
      console.error("DIRECT BRIDGE ABORTED — seanCcid is blank.");
      directCallPending = null;
      return;
    }
    const bridgeResult = await action(ccid, "bridge", {
      call_control_id: directCallPending.seanCcid,
    });
    console.log("Bridge result:", JSON.stringify(bridgeResult));
    directCallPending = null;
    return;
  }

  // ---- CALLBACK: your phone answered a call-back → now dial the lead ----
  if (type === "call.answered" && state.callback) {
    // Bridge: transfer this call to the lead
    await action(ccid, "transfer", { to: state.callbackTo, from: state.callbackFrom || FROM_NUMBERS[0] });
    return;
  }

  // ---- YOUR leg answered -> require a keypress to confirm a HUMAN picked up
  //      (not your voicemail). Voicemail can't press 1, so this guarantees a
  //      real person is on the line before any lead is ever connected.
  if (type === "call.answered" && ccid === session.seanCallId && state.seanHold) {
    console.log("Sean's phone answered — confirming human with keypress.");
    // No 'answer' command here — Telnyx rejects answering an OUTBOUND call (the
    // call is already answered when Sean picks up). Go straight to the prompt.
    await action(ccid, "gather_using_speak", {
      payload: "Press any key to start your session.",
      voice: "female",
      language: "en-US",
      min: 1,
      max: 1,
      tries: 3,                 // re-prompt up to 3x if no key yet
      timeout: 10000,           // wait up to 10s for the first key
      inter_digit_timeout: 5000,
    });
    return;
  }

  // ---- Sean pressed a key -> confirmed human -> create conference & hold him ----
  if (type === "call.gather.ended" && ccid === session.seanCallId) {
    const digits = p.digits || "";
    if (!digits) {
      // No key pressed within timeout => almost certainly voicemail. Hang up,
      // do NOT mark Sean up, do NOT let any lead connect.
      console.error("No keypress from Sean leg — treating as voicemail. Hanging up, session NOT started.");
      await action(ccid, "hangup", {});
      session.seanUp = false; session.seanCallId = null; session.pending = null; session.confId = null;
      return;
    }
    console.log("Sean confirmed human (key:", digits, ") — creating conference:", session.confName);
    session.confId = await createConferenceWithSean(ccid, session.confName);
    if (!session.confId) {
      console.error("Could not create conference — ending session to avoid bad state.");
      await action(ccid, "hangup", {});
      session.seanUp = false; session.seanCallId = null; session.pending = null;
      return;
    }
    session.seanUp = true;
    console.log("Sean held in conference id:", session.confId);
    if (session.pending) {
      const { batchId, leads, market } = session.pending;
      session.pending = null;
      await fireBatch(batchId, leads, market);
    }
    return;
  }

  // ---- SEAN's leg hung up -> Sean ended the whole session.
  //      (A lead hanging up does NOT touch Sean's leg in the conference model,
  //      so this only fires when Sean himself hangs up his phone.)
  if (type === "call.hangup" && ccid === session.seanCallId) {
    console.log("Sean hung up his phone — ending session.");
    for (const b of batches.values()) {
      if (!b.done) {
        for (const ln of b.lines.values()) {
          if (ln.status === "dialing" || ln.status === "ringing") ln.status = "ended";
        }
        b.done = true;
      }
    }
    session.confId = session.confName = session.seanCallId = null;
    session.seanUp = false; session.pending = null;
    console.log("Session ended.");
    return;
  }

  // ---- a LEAD line: find its batch
  const batch = state.batchId ? batches.get(state.batchId) : null;
  if (!batch) return;
  const line = batch.lines.get(ccid);

  switch (type) {
    case "call.answered": {
      // INSTANT CONNECT: the moment the lead picks up, join them to Sean's
      // conference. No AMD wait, no detection pause — Sean is talking the second
      // they say hello. (Tradeoff: if it's their voicemail, Sean hears it live
      // and hangs up that lead himself; no silent voicemail is ever left.)
      if (!line) break;
      line.status = "ringing";
      recordAnswered(line.fromNumber);

      if (batch.connected) {
        // Sean is already talking to another lead — this extra pickup can't be
        // connected. Hang it up so no one sits on a dead/silent line.
        line.status = "dropped";
        action(ccid, "hangup").catch(() => {});
        break;
      }
      if (!session.seanCallId || !session.seanUp || !session.confId) {
        console.error("LEAD DROPPED — Sean not held in conference. Not connecting.");
        action(ccid, "hangup").catch(() => {});
        break;
      }
      const seanAlive = await isLegAlive(session.seanCallId);
      if (!seanAlive) {
        console.error("LEAD DROPPED — Sean's conference leg not live. Not connecting.");
        action(ccid, "hangup").catch(() => {});
        session.seanUp = false; session.seanCallId = null;
        break;
      }
      batch.connected = true;
      line.status = "connected";
      console.log("Lead answered — joining INSTANTLY to conference", session.confId, ":", ccid);
      const joinResult = await joinConference(session.confId, ccid);
      if (joinResult?.errors) {
        console.error("CONFERENCE JOIN FAILED:", JSON.stringify(joinResult.errors));
        line.status = "dropped";
        batch.connected = false;
        action(ccid, "hangup").catch(() => {});
      } else {
        console.log("Connected instantly — lead in Sean's conference", ccid);
        // Hang up the other still-ringing leads — Sean can only talk to one.
        for (const [otherId, other] of batch.lines) {
          if (otherId !== ccid && (other.status === "dialing" || other.status === "ringing")) {
            other.status = "dropped";
            action(otherId, "hangup").catch(() => {});
          }
        }
      }
      break;
    }

    case "call.machine.premium.detection.ended":
    case "call.machine.detection.ended":
      // No longer used for connect decisions (we connect on answer for zero
      // delay). Kept as a NO-OP in case AMD is still enabled on the number.
      break;

    case "call.hangup":
      // A LEAD line ended. This NEVER affects Sean — he stays in the conference.
      if (line && line.status === "connected") {
        line.status = "ended";
        // Free the conference so the NEXT lead can be connected to Sean.
        batch.connected = false;
        console.log("Lead left the conference — Sean stays on, ready for next lead.");
      } else if (line) {
        line.status = line.status || "dropped";
      }
      if (![...batch.lines.values()].some(l => l.status === "dialing" || l.status === "ringing"))
        batch.done = true;
      break;

    default:
      break;
  }
});

// ============================================================================
//  INBOUND SMS WEBHOOK (if messaging is configured on the number)
// ============================================================================
app.post("/messaging", (req, res) => {
  res.sendStatus(200);
  const ev = req.body?.data;
  if (!ev) return;
  if (ev.event_type === "message.received") {
    const p = ev.payload || {};
    inboundLog.unshift({
      type: "text",
      from: p.from?.phone_number || "unknown",
      to: p.to?.[0]?.phone_number || "",
      body: (p.text || "").slice(0, 500),
      timestamp: new Date().toISOString(),
    });
    if (inboundLog.length > 100) inboundLog.length = 100;
    console.log("Inbound text from", p.from?.phone_number, ":", (p.text || "").slice(0, 80));
  }
});

// ============================================================================
//  DIRECT CALL — bridge without AMD (for testing audio)
// ============================================================================
let directCallPending = null;

app.post("/direct-call", async (req, res) => {
  const { myPhone, to } = req.body || {};
  if (!myPhone || !to) return res.status(400).json({ error: "need myPhone and to" });
  const from = FROM_NUMBERS[0] || "";
  directCallPending = null;
  try {
    console.log("Direct call: calling your phone", myPhone, "then will dial", to);
    const call1 = await telnyx("/calls", {
      connection_id: CONNECTION_ID, to: myPhone, from,
      webhook_url: `${PUBLIC_URL}/webhook`,
    });
    const ccid = call1?.data?.call_control_id;
    directCallPending = { seanCcid: ccid, to, from };
    console.log("Direct call: your call ID =", ccid);
    res.json({ ok: true, callId: ccid });
  } catch (e) { console.error("Direct call error:", e); res.status(500).json({ error: e.message }); }
});

// ============================================================================
//  CALL BACK & TEXT — use the SAME Telnyx number they saw
// ============================================================================
app.post("/call-back", async (req, res) => {
  const { myPhone, to, fromNumber } = req.body || {};
  if (!myPhone || !to) return res.status(400).json({ error: "need myPhone and to" });
  const from = fromNumber || FROM_NUMBERS[0] || "";
  if (!from) return res.status(500).json({ error: "no FROM number" });
  try {
    // Call your phone first
    const call1 = await telnyx("/calls", {
      connection_id: CONNECTION_ID, to: myPhone, from,
      webhook_url: `${PUBLIC_URL}/webhook`,
      client_state: b64({ callback: true, callbackTo: to, callbackFrom: from }),
    });
    res.json({ ok: true, callId: call1?.data?.call_control_id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Handle callback bridge — when your phone answers, dial the lead
app.post("/webhook-callback-bridge", async (req, res) => {
  // This is handled in the main webhook below
  res.sendStatus(200);
});

app.post("/send-text", async (req, res) => {
  const { to, fromNumber, body } = req.body || {};
  if (!to || !body) return res.status(400).json({ error: "need to and body" });
  const from = fromNumber || FROM_NUMBERS[0] || "";
  if (!from) return res.status(500).json({ error: "no FROM number" });
  try {
    const result = await telnyx("/messages", {
      from, to, text: body, type: "SMS",
    });
    if (result.errors) return res.json({ ok: false, error: result.errors[0]?.detail || "SMS failed" });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ============================================================================
//  STATUS + LOGS
// ============================================================================
app.get("/batch-status/:id", (req, res) => {
  const batch = batches.get(req.params.id);
  if (!batch) return res.json({ lines: [], done: false });
  res.json({
    done: batch.done,
    connected: batch.connected,
    lines: [...batch.lines.values()].map(l => ({ leadId: l.leadId, status: l.status, fromNumber: l.fromNumber })),
  });
});

app.get("/inbound-log", (req, res) => {
  res.json({ log: inboundLog });
});

app.post("/hangup-all", async (req, res) => {
  // Emergency stop: hang up EVERY lead leg, including connected ones, so nothing
  // can linger (e.g. a misclassified voicemail sitting in the conference).
  for (const batch of batches.values()) {
    for (const [ccid, l] of batch.lines) {
      if (!String(ccid).startsWith("blocked_")) action(ccid, "hangup").catch(() => {});
      l.status = "ended";
    }
    batch.done = true;
    batch.connected = false;
  }
  res.json({ ok: true });
});

app.post("/session/end", async (req, res) => {
  // Drop ALL lead legs first, then Sean's leg, then clear all state.
  for (const batch of batches.values()) {
    for (const [ccid] of batch.lines) {
      if (!String(ccid).startsWith("blocked_")) action(ccid, "hangup").catch(() => {});
    }
    batch.done = true; batch.connected = false;
  }
  if (session.seanCallId) action(session.seanCallId, "hangup").catch(() => {});
  session.confId = session.confName = session.seanCallId = null;
  session.seanUp = false; session.pending = null;
  console.log("Session ended by user — all legs dropped.");
  res.json({ ok: true });
});

// ---- LIVE SESSION STATUS (frontend reads this so the board shows real state) ----
app.get("/session/status", (_req, res) => {
  // Find the most recent active batch and report its real line states.
  let activeLines = [];
  for (const [, b] of batches) {
    if (!b.done) {
      activeLines = [...b.lines.values()].map(l => ({
        to: l.to, status: l.status, fromNumber: l.fromNumber, attempt: l.attempt,
      }));
    }
  }
  res.json({
    seanUp: session.seanUp,
    seanOnCall: Boolean(session.seanCallId),
    activeLines,
  });
});

// ---- DROP CURRENT LEAD (keep Sean on the line) ----
// Hangs up whatever lead is currently connected to Sean, WITHOUT ending Sean's
// session. Sean stays held in the conference, ready for the next lead.
app.post("/drop-lead", async (req, res) => {
  let dropped = 0;
  for (const batch of batches.values()) {
    for (const [ccid, l] of batch.lines) {
      if (l.status === "connected" && !String(ccid).startsWith("blocked_")) {
        action(ccid, "hangup").catch(() => {});
        l.status = "ended";
        dropped++;
      }
    }
    // free the slot so the next lead can connect / next batch can fire
    batch.connected = false;
  }
  console.log("Dropped current lead(s):", dropped, "— Sean stays on the line.");
  res.json({ ok: true, dropped });
});

// ---- NUMBER MANAGEMENT (live, syncs the dialer's rotation) ----
app.get("/numbers", (_req, res) => {
  res.json({ numbers: FROM_NUMBERS, reputation: numberReputation() });
});
app.post("/numbers", (req, res) => {
  const { numbers } = req.body || {};
  if (!Array.isArray(numbers)) return res.status(400).json({ error: "numbers must be an array" });
  const clean = [...new Set(numbers.map(s => String(s).trim()).filter(s => /^\+\d{8,15}$/.test(s)))];
  if (clean.length === 0) return res.status(400).json({ error: "no valid E.164 numbers (+1...)" });
  FROM_NUMBERS = clean;
  rrIndex = 0;
  console.log("FROM_NUMBERS updated live ->", FROM_NUMBERS.join(", "));
  res.json({ numbers: FROM_NUMBERS, reputation: numberReputation() });
});

app.get("/health", async (_req, res) => {
  // Refresh balance (cached ~60s) and recompute alerts on each poll.
  const balance = await checkBalance();
  const alerts = computeAlerts();
  // Add answer-rate (spam-proxy) warnings to the banner.
  const rep = numberReputation();
  rep.filter(r => r.suspect).forEach(r => {
    alerts.push(`NUMBER MAY BE FLAGGED: ${r.number} — answer rate ${(r.answerRate * 100).toFixed(0)}% over ${r.placed} calls. Verify it and consider swapping.`);
  });
  res.json({
    ok: alerts.length === 0,
    configured: Boolean(TELNYX_API_KEY && CONNECTION_ID && FROM_NUMBERS.length && PUBLIC_URL),
    fromNumbers: FROM_NUMBERS.length,
    hasVoicemail: !!voicemailAudio,
    inboundCount: inboundLog.length,
    // --- alerting ---
    alerts,
    balance: balance,
    apiKeyOk: sysHealth.apiKeyOk,
    consecutiveCallFailures: sysHealth.consecutiveCallFailures,
    lastFailureReason: sysHealth.lastFailureReason,
    lastFailureAt: sysHealth.lastFailureAt,
    // --- number reputation (answer-rate proxy) ---
    reputation: rep,
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Switchboard v3.7.2 backend listening on :${PORT}`));
