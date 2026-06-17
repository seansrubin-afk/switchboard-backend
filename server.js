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
//  DIAL A BATCH
// ============================================================================
app.post("/dial-batch", async (req, res) => {
  const { myPhone, leads, market } = req.body || {};
  if (!myPhone || !Array.isArray(leads) || leads.length === 0)
    return res.status(400).json({ error: "need myPhone and leads[]" });
  if (!FROM_NUMBERS.length) return res.status(500).json({ error: "no FROM_NUMBERS configured" });

  const mkt = (market || DEFAULT_MARKET).toUpperCase();
  const batchId = "b_" + Date.now().toString(36);

  // If Sean is already on the line from a previous batch, just fire leads directly.
  if (session.seanUp && session.seanCallId) {
    console.log("Sean already on line, firing leads directly");
    await fireBatch(batchId, leads, mkt);
    return res.json({ batchId });
  }

  // If a call to Sean's phone is already in flight (seanCallId set but not yet
  // answered), don't dial him again — queue these leads to fire when he answers.
  if (session.seanCallId && !session.seanUp) {
    console.log("Sean's phone already ringing — queueing leads for when he answers");
    session.pending = { batchId, leads, market: mkt };
    batches.set(batchId, { lines: new Map(), connected: false, done: false, market: mkt });
    return res.json({ batchId });
  }

  // Otherwise, call Sean's phone first
  session.pending = { batchId, leads, market: mkt };
  console.log("Calling Sean's phone:", myPhone);
  const call = await telnyx("/calls", {
    connection_id: CONNECTION_ID,
    to: myPhone,
    from: pickFrom(),
    webhook_url: `${PUBLIC_URL}/webhook`,
  });
  session.seanCallId = call?.data?.call_control_id || null;
  if (!session.seanCallId) {
    const err = call?.errors?.[0] || {};
    const reason = describeTelnyxError(err.code, err.detail, call?._httpStatus);
    recordCallFailure(reason, err.code);
    console.log("Failed to call Sean's phone —", reason);
  } else {
    recordCallSuccess();
  }

  batches.set(batchId, { lines: new Map(), connected: false, done: false, market: mkt });
  res.json({ batchId });
});

async function fireBatch(batchId, leads, market = DEFAULT_MARKET) {
  const lines = new Map();
  batches.set(batchId, { lines, connected: false, done: false, market });

  await Promise.all(leads.slice(0, 10).map(async (ld) => {
    const from = pickFrom();
    try {
      const call = await telnyx("/calls", {
        connection_id: CONNECTION_ID,
        to: ld.to,
        from,
        webhook_url: `${PUBLIC_URL}/webhook`,
        answering_machine_detection: "premium",
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

  // ---- YOUR leg answered -> store your call ID, then fire any pending batch
  if (type === "call.answered" && ccid === session.seanCallId) {
    session.seanUp = true;
    console.log("Sean answered, call ID:", ccid);
    if (session.pending) {
      const { batchId, leads, market } = session.pending;
      session.pending = null;
      await fireBatch(batchId, leads, market);
    }
    return;
  }

  // ---- YOUR leg hung up -> end session ONLY if it was a real hangup by you,
  //      not a bridged lead dropping. Verify your leg is actually gone before
  //      tearing down, so one rude lead can't kill your whole session.
  if (type === "call.hangup" && ccid === session.seanCallId) {
    const cause = (p.hangup_cause || "").toLowerCase();
    const source = (p.hangup_source || "").toLowerCase();
    console.log("Sean-leg ended. cause:", cause, "source:", source, "— resetting session cleanly.");
    // Your leg is physically gone no matter who caused it. Don't pretend it's
    // alive (that caused the freeze + phantom transfers). Reset cleanly so the
    // next Dial re-establishes you instantly. Mark active batch lines ended.
    if (session.pending) {
      const b = batches.get(session.pending.batchId);
      if (b) b.done = true;
    }
    // close out any in-flight batch lines so the board doesn't hang on "ringing"
    for (const b of batches.values()) {
      if (!b.done) {
        for (const ln of b.lines.values()) {
          if (ln.status === "dialing" || ln.status === "ringing") ln.status = "ended";
        }
        if (![...b.lines.values()].some(l => l.status === "dialing" || l.status === "ringing")) b.done = true;
      }
    }
    session.confId = session.confName = session.seanCallId = null;
    session.seanUp = false; session.pending = null;
    console.log("Session reset — ready to re-dial. (Lead hangups no longer freeze the board.)");
    return;
  }

  // ---- a LEAD line: find its batch
  const batch = state.batchId ? batches.get(state.batchId) : null;
  if (!batch) return;
  const line = batch.lines.get(ccid);

  switch (type) {
    case "call.answered":
      if (line) { line.status = "ringing"; recordAnswered(line.fromNumber); }
      break;

    case "call.machine.premium.detection.ended":
    case "call.machine.detection.ended": {
      const result = (p.result || "").toLowerCase();
      const isHuman = result.includes("human");
      const isMachine = result.includes("machine");

      if (isHuman && !batch.connected) {
        batch.connected = true;
        if (line) line.status = "connected";
        console.log("Human detected! Bridging", ccid, "to Sean:", session.seanCallId);
        // Guard: never fire a bridge with a blank target (the v3.2 422 bug)
        if (!session.seanCallId) {
          console.error("BRIDGE ABORTED — session.seanCallId is blank. Sean's leg is not up.");
          batch.connected = false;
          break;
        }
        // Direct bridge — connects Sean's call audio to this lead's call audio
        const bridgeResult = await action(ccid, "bridge", {
          call_control_id: session.seanCallId,
        });
        if (bridgeResult?.errors) {
          console.error("BRIDGE FAILED:", JSON.stringify(bridgeResult.errors));
        } else {
          console.log("Bridge OK — Sean connected to lead", ccid);
        }
        for (const [otherId, other] of batch.lines) {
          if (otherId !== ccid && (other.status === "dialing" || other.status === "ringing")) {
            other.status = "dropped";
            action(otherId, "hangup").catch(() => {});
          }
        }
      } else if (isHuman && batch.connected) {
        if (line) line.status = "dropped";
        action(ccid, "hangup").catch(() => {});
      } else if (isMachine) {
        if (line) line.status = "machine";
        // Only leave a voicemail on the FIRST attempt — never spam the same person
        if (voicemailAudio && PUBLIC_URL && line && (line.attempt || 0) === 0) {
          await action(ccid, "playback_start", {
            audio_url: `${PUBLIC_URL}/voicemail.mp3`,
          });
          setTimeout(() => action(ccid, "hangup").catch(() => {}), 15000);
        } else {
          // Second/third attempt or no voicemail — hang up silently
          action(ccid, "hangup").catch(() => {});
        }
      }
      break;
    }

    case "call.hangup":
      // A LEAD line ended. This must NEVER affect Sean's session.
      if (line && line.status !== "connected") line.status = line.status || "dropped";
      if (line && line.status === "connected") {
        line.status = "ended";
        console.log("Lead hung up after talking — session stays live for next batch.");
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
  for (const batch of batches.values())
    for (const [ccid, l] of batch.lines)
      if (l.status !== "connected") action(ccid, "hangup").catch(() => {});
  res.json({ ok: true });
});

app.post("/session/end", async (req, res) => {
  if (session.seanCallId) action(session.seanCallId, "hangup").catch(() => {});
  session.confId = session.confName = session.seanCallId = null;
  session.seanUp = false; session.pending = null;
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
app.listen(PORT, () => console.log(`Switchboard v3.5.0 backend listening on :${PORT}`));
