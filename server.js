/* ============================================================================
   THE SWITCHBOARD v4.1.0-INTL — backend orchestrator
   - Parallel outbound dialing with AMD
   - Custom voicemail drop (YOUR recorded voice, not TTS)
   - Inbound call forwarding (callbacks reach your phone)
   - Inbound call/text logging (see who called/texted back)

   v4.1.0-INTL changes (formatting only — all call logic is unchanged):
     • Multi-country number formatting: a number is identified by its OWN country
       (its + country code, its market label, or its national shape). No single
       "default country" is ever forced onto every number.
     • Added South Africa (ZA / +27), which was missing from the dial codes.
     • Numbers that can't be safely identified are SKIPPED + logged, never dialed
       as a guessed wrong country.
     • Startup log now prints DEFAULT_MARKET too, so you can see the env state.
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
// All three Telnyx caller-ID numbers are baked in as the permanent default, so the
// dialer always has them even if the Railway FROM_NUMBERS variable is ever empty or
// wiped on a redeploy. Setting FROM_NUMBERS in Railway still overrides this list.
const DEFAULT_FROM_NUMBERS = "+13159232442,+17852024556,+16363302146";
let   FROM_NUMBERS      = (process.env.FROM_NUMBERS || DEFAULT_FROM_NUMBERS).split(",").map(s => s.trim()).filter(Boolean);
const PUBLIC_URL       = process.env.PUBLIC_URL;
// Optional FALLBACK country for national-format numbers that carry NO country code,
// NO market label, AND don't match a known national pattern. LEAVE THIS UNSET for
// multi-country dialing: unset means every number is identified by its own country
// code / market label / shape, and anything still ambiguous is SKIPPED rather than
// guessed as a wrong country. Set it (e.g. DEFAULT_MARKET=NZ) only if you want one
// specific fallback for a single-country campaign.
const DEFAULT_MARKET   = (process.env.DEFAULT_MARKET || "").toUpperCase();
const MY_PHONE         = process.env.MY_PHONE || "";
// If set (e.g. "sip:myusername@sip.telnyx.com"), the Switchboard rings this SIP
// softphone over the internet instead of the PSTN number. Fixes laggy WiFi-Calling
// delivery when abroad. Leave blank to use the phone number as before.
const SIP_DESTINATION  = process.env.SIP_DESTINATION || "";
const ringMe = (phoneFallback) => SIP_DESTINATION || phoneFallback || MY_PHONE;
// Automatic voicemail drop. DEFAULT OFF, because answering-machine detection was
// timing out on every call in production (adding a ~4s silent pause before each
// connect and not reliably detecting machines). Off means instant connect the moment
// a lead answers, exactly like the original. Set VM_AUTO=on in Railway to try the
// voicemail drop again (only worth it if premium detection is actually working).
const VM_AUTO = String(process.env.VM_AUTO ?? "off").toLowerCase() === "on";
// Detection engine: "premium" (most accurate at telling humans from machines, small
// per-call cost) or "detect" (standard, free, a bit less accurate). Only used when
// VM_AUTO is on. Change with the AMD_MODE Railway variable.
const AMD_MODE = (process.env.AMD_MODE || "premium").toLowerCase();
const TELNYX = "https://api.telnyx.com/v2";
// ---- Per-number reputation stats (answer-rate proxy for spam detection) ----
const numberStats = {}; // { "+1...": { placed, answered } }
function ensureNumberStats(n) { if (!numberStats[n]) numberStats[n] = { placed: 0, answered: 0 }; return numberStats[n]; }
function recordPlaced(n) { if (n) ensureNumberStats(n).placed += 1; }
function recordAnswered(n) { if (n) ensureNumberStats(n).answered += 1; }
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
const session = { confId: null, confName: null, seanCallId: null, seanUp: false, pending: null, leadOnLine: false, connectedLeadCcid: null };
const batches = new Map();
let rrIndex = 0;
// ---- BACKEND-ENFORCED CALL SAFETY (never trust the frontend) ----
const MAX_ATTEMPTS_PER_NUMBER = 3;        // absolute max calls to one number, ever, per server run
const MIN_SECONDS_BETWEEN_CALLS = 90;     // a number cannot be re-dialed within this window
const callHistory = {};                   // { "+1leadphone": { attempts, lastCalledAt } }
// ---- DAILY RESET of the per-number attempt cap (local midnight). ----
const DAILY_RESET_UTC_OFFSET = Number(process.env.DAILY_RESET_UTC_OFFSET ?? 8);
let resetDayKey = null;
function localDayKey() {
  const shifted = new Date(Date.now() + DAILY_RESET_UTC_OFFSET * 3600 * 1000);
  return shifted.toISOString().slice(0, 10); // YYYY-MM-DD in the chosen timezone
}
function maybeDailyReset() {
  const today = localDayKey();
  if (resetDayKey === null) { resetDayKey = today; return; }
  if (today !== resetDayKey) {
    const cleared = Object.keys(callHistory).length;
    for (const k in callHistory) delete callHistory[k];
    resetDayKey = today;
    console.log(`Daily reset — cleared attempt counts for ${cleared} number(s); new day ${today} (UTC+${DAILY_RESET_UTC_OFFSET})`);
  }
}
// Country dial codes for the markets we call. Add a market here to support a new
// country (key = the market code, value = its phone country code).
const DIAL_CODES = { US: "1", CA: "1", AU: "61", NZ: "64", IE: "353", UK: "44", GB: "44", ZA: "27" };
// Accept whatever the frontend or env sends for the market — "au", "Australia",
// "AUS" all mean AU — and map it to a code we know how to format.
const MARKET_ALIASES = {
  AU: "AU", AUS: "AU", AUSTRALIA: "AU",
  NZ: "NZ", NZL: "NZ", "NEW ZEALAND": "NZ",
  IE: "IE", IRL: "IE", IRELAND: "IE",
  UK: "UK", GB: "UK", GBR: "UK", "UNITED KINGDOM": "UK", BRITAIN: "UK",
  US: "US", USA: "US", "UNITED STATES": "US",
  CA: "CA", CAN: "CA", CANADA: "CA",
  ZA: "ZA", RSA: "ZA", "SOUTH AFRICA": "ZA",
};
function normalizeMarket(m) {
  if (!m) return null;
  return MARKET_ALIASES[String(m).trim().toUpperCase()] || null;
}
// HARD OVERRIDE. If FORCE_MARKET is set in Railway (e.g. FORCE_MARKET=AU), every
// number is formatted as that country. Use ONLY when 100% of your leads are from one
// country. LEAVE IT UNSET for multi-country dialing.
const FORCE_MARKET = normalizeMarket(process.env.FORCE_MARKET) || null;

// ===========================================================================
//  PHONE NUMBER FORMATTING (multi-country, no forced default)
// ===========================================================================
// A number is identified by its OWN country, in this order of trust:
//   (1) it already carries a country code:  +27..., +353..., +61..., +44..., +1...
//   (2) the lead has an explicit market label: AU / IE / UK / ZA / NZ / US ...
//   (3) its national shape is unambiguous:   AU mobile 04..., UK 07..., US 10-digit
// If none identify it, it is SKIPPED (returns null -> the caller logs it), so we
// NEVER dial a number as a guessed wrong country.
// "+0..." is junk (country codes never start with 0) and is treated as national.
// "+10402687704" (a national 0-number wrongly stamped +1) is rejected by the NANP
// rule: a +1 number's first national digit must be 2-9, never 0/1.

// E.164 validity, including the NANP (+1) area-code rule.
function isValidE164(s) {
  if (!/^\+[1-9]\d{7,14}$/.test(s)) return false;
  if (s.startsWith("+1")) return /^\+1[2-9]\d{9}$/.test(s); // US/CA: exactly 10 digits, area code 2-9
  return true;
}
// Apply a country dial code to a national-digits string. Strips the national trunk 0
// for trunk-0 countries (AU/NZ/IE/UK/ZA); NANP has no trunk 0. Returns valid E.164 or null.
function applyCode(code, n) {
  if (!code) return null;
  if (code === "1") {                                    // US / CA: no trunk 0
    if (n.length === 11 && n[0] === "1") n = n.slice(1);  // tolerate a leading 1
    const e = "+1" + n;
    return isValidE164(e) ? e : null;
  }
  n = n.replace(/^0+/, "");                              // strip national trunk 0(s)
  const e = n.startsWith(code) ? "+" + n : "+" + code + n;
  return isValidE164(e) ? e : null;
}
// Identify a national number by its own shape when there's no reliable label. Only
// high-confidence, non-overlapping patterns; anything else returns null so the caller
// SKIPS it rather than guessing a wrong country.
function detectFromPattern(d) {
  if (/^[2-9]\d{9}$/.test(d))   return "+1"  + d;          // US/CA 10-digit (area code 2-9)
  if (/^1[2-9]\d{9}$/.test(d))  return "+"   + d;          // US/CA with leading 1
  if (/^04\d{8}$/.test(d))      return "+61" + d.slice(1); // AU mobile (04 + 8 digits)
  if (/^0[1237]\d{9}$/.test(d)) return "+44" + d.slice(1); // UK 11-digit (07 mobile / 01,02,03)
  return null;
}
// Known country codes, longest first, so "353" matches before "1" etc.
const KNOWN_CODES = [...new Set(Object.values(DIAL_CODES))].sort((a, b) => b.length - a.length);
function matchKnownCode(digits) {
  for (const c of KNOWN_CODES) if (digits.startsWith(c)) return c;
  return null;
}
function toE164(raw, market) {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  const digits = s.replace(/[^\d]/g, "");
  if (!digits) return null;
  const hadPlus = s.startsWith("+");

  // (1) Already valid international (+, first digit not 0): pass through, validated.
  if (!FORCE_MARKET && hadPlus && !digits.startsWith("0")) {
    // Guard: a national trunk 0 stuck right after a real country code (e.g.
    // "+61 0402..." or "+27 (0)82...") is never valid in E.164. Strip it and
    // reformat by that code. A 0 immediately after a country code is always junk.
    const cc = matchKnownCode(digits);
    if (cc && digits[cc.length] === "0") {
      const e = applyCode(cc, digits.slice(cc.length));
      if (e) return e;
    }
    return isValidE164("+" + digits) ? "+" + digits : null;
  }

  // National format from here (no +, or the bogus "+0..." case, or a FORCE override).
  const labelled = normalizeMarket(market);          // (2) explicit per-lead label
  const fallback = normalizeMarket(DEFAULT_MARKET);   // optional, ONLY if you set one

  // (0) Hard single-country override, if you ever set FORCE_MARKET.
  if (FORCE_MARKET) { const e = applyCode(DIAL_CODES[FORCE_MARKET], digits); if (e) return e; }
  // (2) Trust an explicit market label.
  if (labelled)     { const e = applyCode(DIAL_CODES[labelled],     digits); if (e) return e; }
  // (3) Identify by the number's own shape (AU mobile, UK, US...) BEFORE any default.
  const det = detectFromPattern(digits); if (det) return det;
  // (4) Only if YOU deliberately set DEFAULT_MARKET, use it as a last resort.
  if (fallback)     { const e = applyCode(DIAL_CODES[fallback],     digits); if (e) return e; }
  // (5) Maybe it already carries a country code but lost its leading +.
  if (isValidE164("+" + digits)) return "+" + digits;
  // Could not safely identify the country -> SKIP. The caller logs the raw value.
  return null;
}
function canCall(toNumber) {
  maybeDailyReset();                        // free up numbers if the day has rolled over
  const h = callHistory[toNumber];
  if (!h) return { ok: true };
  if (h.attempts >= MAX_ATTEMPTS_PER_NUMBER)
    return { ok: false, reason: `max ${MAX_ATTEMPTS_PER_NUMBER} attempts reached` };
  const since = (Date.now() - (h.lastCalledAt || 0)) / 1000;
  if (since < MIN_SECONDS_BETWEEN_CALLS)
    return { ok: false, reason: `called ${Math.round(since)}s ago (min ${MIN_SECONDS_BETWEEN_CALLS}s)` };
  return { ok: true };
}
function recordCallAttempt(toNumber, name) {
  const h = callHistory[toNumber] || { attempts: 0, lastCalledAt: 0, name: "", history: [] };
  h.attempts += 1; h.lastCalledAt = Date.now();
  if (name) h.name = name;
  if (!h.history) h.history = [];
  h.history.push({ at: new Date().toISOString(), outcome: "dialed" });
  callHistory[toNumber] = h;
}
function recordOutcome(toNumber, outcome) {
  const h = callHistory[toNumber];
  if (!h) return;
  h.lastOutcome = outcome;
  if (h.history && h.history.length) h.history[h.history.length - 1].outcome = outcome;
}
// ---- SERVER-SIDE DISPOSITIONS (survive device switches / re-imports) ----
const dispositions = {}; // { "+phone": { status, at } }
function isDispositioned(phone) {
  const d = dispositions[phone];
  return d && d.status && d.status !== "callback"; // "callback" leads are still callable later
}
// Voicemail audio (uploaded by user, stored in memory)
let voicemailAudio = null; // { buffer: Buffer, contentType: string }
// ---- SYSTEM HEALTH / ALERT TRACKING ----
const sysHealth = {
  lastBalance: null, lastBalanceCheck: 0, consecutiveCallFailures: 0,
  lastFailureReason: null, lastFailureCode: null, lastFailureAt: null,
  apiKeyOk: true, alerts: [],
};
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
async function checkBalance() {
  const now = Date.now();
  if (now - sysHealth.lastBalanceCheck < 60000 && sysHealth.lastBalance !== null) return sysHealth.lastBalance;
  try {
    const res = await fetch(TELNYX + "/balance", { headers: { Authorization: `Bearer ${TELNYX_API_KEY}` } });
    if (res.status === 401) { sysHealth.apiKeyOk = false; return null; }
    sysHealth.apiKeyOk = true;
    const j = await res.json();
    const bal = parseFloat(j?.data?.balance);
    if (!isNaN(bal)) { sysHealth.lastBalance = bal; sysHealth.lastBalanceCheck = now; }
    return sysHealth.lastBalance;
  } catch { return sysHealth.lastBalance; }
}
function recordCallFailure(reason, code) {
  sysHealth.consecutiveCallFailures += 1;
  sysHealth.lastFailureReason = reason;
  sysHealth.lastFailureCode = code || null;
  sysHealth.lastFailureAt = new Date().toISOString();
}
function recordCallSuccess() { sysHealth.consecutiveCallFailures = 0; }
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
async function isLegAlive(ccid) {
  if (!ccid) return false;
  try {
    const res = await fetch(TELNYX + `/calls/${ccid}`, { headers: { Authorization: `Bearer ${TELNYX_API_KEY}` } });
    if (!res.ok) return false;
    const j = await res.json();
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
    // Hanging up an already-ended call is expected/harmless noise when we tidy up
    // losing lines after a bridge. 90018 = already ended; 90015 = invalid call control
    // id (the leg already went away). Don't spam the logs with either.
    const code = json?.errors?.[0]?.code;
    const isHarmlessHangup = path.endsWith("/hangup") &&
      (code === "90018" || code === "90015" || /already ended|not valid|invalid call control/i.test(text));
    if (!isHarmlessHangup) console.error("Telnyx error", res.status, path, text.slice(0, 400));
  }
  return json;
}
const action = (ccid, name, payload = {}) => telnyx(`/calls/${ccid}/actions/${name}`, payload);
async function createConferenceWithSean(seanCcid, name) {
  const r = await telnyx("/conferences", {
    name, call_control_id: seanCcid, beep_enabled: "never",
    start_conference_on_create: true, comfort_noise: true,
  });
  const id = r?.data?.id || null;
  if (r?.errors) console.error("CONFERENCE CREATE FAILED:", JSON.stringify(r.errors));
  return id;
}
async function joinConference(confId, leadCcid) {
  // start_conference_on_enter: true => audio is LIVE the moment the lead enters.
  return telnyx(`/conferences/${confId}/actions/join`, {
    call_control_id: leadCcid, beep_enabled: "never",
    start_conference_on_enter: true, end_conference_on_exit: false, supervisor_role: "none",
  });
}
// ============================================================================
//  VOICEMAIL MANAGEMENT
// ============================================================================
app.post("/voicemail", (req, res) => {
  const { audio, contentType } = req.body || {};
  if (!audio) return res.status(400).json({ error: "need audio (base64)" });
  voicemailAudio = { buffer: Buffer.from(audio, "base64"), contentType: contentType || "audio/mpeg" };
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
// ============================================================================
app.post("/session/start", async (req, res) => {
  const { myPhone } = req.body || {};
  if (!myPhone) return res.status(400).json({ error: "need myPhone" });
  if (!FROM_NUMBERS.length) return res.status(500).json({ error: "no FROM_NUMBERS configured" });
  if (session.seanUp && session.seanCallId && await isLegAlive(session.seanCallId)) {
    return res.json({ ok: true, already: true, confName: session.confName });
  }
  // DEBOUNCE: if we just called Sean's phone in the last 30s, don't ring him again.
  if (session.seanCallId && session.startedAt && (Date.now() - session.startedAt) < 30000) {
    return res.json({ ok: true, pending: true, message: "Already calling your phone — answer it." });
  }
  session.confName = "sw_" + Date.now().toString(36);
  session.confId = null;
  session.pending = null;
  session.startedAt = Date.now();
  console.log("Session start — calling Sean to hold in conference:", session.confName);
  const call = await telnyx("/calls", {
    connection_id: CONNECTION_ID, to: ringMe(myPhone), from: pickFrom(),
    webhook_url: `${PUBLIC_URL}/webhook`, client_state: b64({ seanHold: true }),
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
  const { myPhone, leads, market, dialCount } = req.body || {};
  if (!myPhone || !Array.isArray(leads) || leads.length === 0)
    return res.status(400).json({ error: "need myPhone and leads[]" });
  if (!FROM_NUMBERS.length) return res.status(500).json({ error: "no FROM_NUMBERS configured" });
  const mkt = (market || DEFAULT_MARKET).toUpperCase();
  const batchId = "b_" + Date.now().toString(36);
  const count = Math.max(1, Math.min(10, parseInt(dialCount, 10) || leads.length));
  const seanLive = session.seanUp && session.seanCallId && await isLegAlive(session.seanCallId);
  if (!seanLive) {
    session.seanUp = false; session.seanCallId = null;
    return res.status(409).json({ error: "no_session", message: "Start a session first — Sean must be on the line." });
  }
  if (session.leadOnLine) {
    return res.status(409).json({ error: "busy", message: "You're on a call — finish it before dialing the next batch." });
  }
  console.log("Sean held in conference — firing", count, "leads");
  await fireBatch(batchId, leads, mkt, count);
  return res.json({ batchId });
});
async function fireBatch(batchId, leads, market = DEFAULT_MARKET, count = 10) {
  const lines = new Map();
  batches.set(batchId, { lines, connected: false, done: false, market });
  await Promise.all(leads.slice(0, Math.max(1, Math.min(10, count))).map(async (ld) => {
    const to = toE164(ld.to, market);
    console.log("FORMAT raw=" + JSON.stringify(ld.to) + " market=" + JSON.stringify(market) + " force=" + (FORCE_MARKET || "-") + " -> " + to);
    if (!to) {
      console.log("SKIPPED unreadable number", JSON.stringify(ld.to));
      lines.set("badnum_" + ld.leadId, { leadId: ld.leadId, to: ld.to, status: "blocked", attempt: ld.attempt || 0, fromNumber: "-" });
      return;
    }
    const gate = canCall(to);
    if (!gate.ok) {
      console.log("BLOCKED re-dial of", to, "—", gate.reason);
      lines.set("blocked_" + ld.leadId, { leadId: ld.leadId, to, status: "blocked", attempt: ld.attempt || 0, fromNumber: "-" });
      return;
    }
    if (isDispositioned(to)) {
      console.log("SKIPPED already-dispositioned lead", to, "—", dispositions[to].status);
      lines.set("dispo_" + ld.leadId, { leadId: ld.leadId, to, status: "done", attempt: ld.attempt || 0, fromNumber: "-" });
      return;
    }
    const from = pickFrom();
    try {
      recordCallAttempt(to, ld.name); // count the attempt BEFORE dialing
      const call = await telnyx("/calls", {
        connection_id: CONNECTION_ID, to, from,
        webhook_url: `${PUBLIC_URL}/webhook`,
        client_state: b64({ batchId, leadId: ld.leadId }),
        ...(VM_AUTO ? { answering_machine_detection: AMD_MODE } : {}),
      });
      const ccid = call?.data?.call_control_id;
      if (ccid) {
        lines.set(ccid, { leadId: ld.leadId, to, status: "dialing", attempt: ld.attempt || 0, fromNumber: from });
        recordPlaced(from);
        recordCallSuccess();
      } else {
        const err = call?.errors?.[0] || {};
        const reason = describeTelnyxError(err.code, err.detail, call?._httpStatus);
        recordCallFailure(reason, err.code);
        console.log("Call failed for", to, "—", reason);
      }
    } catch (e) {
      recordCallFailure("Network error reaching Telnyx — backend may be offline or rate-limited.", null);
      console.log("Call error for", to, ":", e.message);
    }
  }));
  if (lines.size === 0) {
    const batch = batches.get(batchId);
    if (batch) batch.done = true;
    console.log("Batch", batchId, "— all calls failed, marking done");
  }
  setTimeout(() => {
    const batch = batches.get(batchId);
    if (batch && !batch.done) { batch.done = true; console.log("Batch", batchId, "— timed out, marking done"); }
  }, 30000);
}
// ============================================================================
//  CONNECT vs VOICEMAIL HELPERS
// ============================================================================
async function bridgeLeadToSean(ccid, batch, line) {
  if (!line) return;
  if (line.amdTimer) { clearTimeout(line.amdTimer); line.amdTimer = null; }
  line.awaitingAMD = false;
  // GLOBAL one-at-a-time guard. CLAIM the slot synchronously, before any await.
  if (session.leadOnLine || batch.connected) {
    line.status = "dropped";
    action(ccid, "hangup").catch(() => {});
    return;
  }
  if (!session.seanCallId || !session.seanUp || !session.confId) {
    console.error("LEAD DROPPED — Sean not held in conference. Not connecting.");
    action(ccid, "hangup").catch(() => {});
    return;
  }
  session.leadOnLine = true;
  session.connectedLeadCcid = ccid;
  batch.connected = true;
  const seanAlive = await isLegAlive(session.seanCallId);
  if (!seanAlive) {
    console.error("LEAD DROPPED — Sean's conference leg not live. Not connecting.");
    session.leadOnLine = false;
    session.connectedLeadCcid = null;
    batch.connected = false;
    session.seanUp = false; session.seanCallId = null;
    action(ccid, "hangup").catch(() => {});
    return;
  }
  line.status = "connected";
  recordOutcome(line.to, "connected");
  console.log("Lead is a human — joining to conference", session.confId, ":", ccid);
  const joinResult = await joinConference(session.confId, ccid);
  if (joinResult?.errors) {
    console.error("CONFERENCE JOIN FAILED:", JSON.stringify(joinResult.errors));
    line.status = "dropped";
    batch.connected = false;
    session.leadOnLine = false;
    session.connectedLeadCcid = null;
    action(ccid, "hangup").catch(() => {});
  } else {
    console.log("Connected — lead in Sean's conference", ccid);
    for (const [otherId, other] of batch.lines) {
      if (otherId !== ccid && (other.status === "dialing" || other.status === "ringing")) {
        other.status = "dropped";
        action(otherId, "hangup").catch(() => {});
      }
    }
  }
}
async function dropVoicemail(ccid, batch, line) {
  if (!line) return;
  if (line.amdTimer) { clearTimeout(line.amdTimer); line.amdTimer = null; }
  line.awaitingAMD = false;
  line.status = "voicemail";
  recordOutcome(line.to, "voicemail");
  if (!voicemailAudio || !PUBLIC_URL) {
    console.log("Machine detected, no recording available — hanging up", ccid);
    action(ccid, "hangup").catch(() => {});
    return;
  }
  line.vmPlaying = true;
  dispositions[line.to] = { status: "voicemail", at: new Date().toISOString() };
  console.log("Machine detected — dropping your voicemail on", ccid, "(won't redial)");
  const r = await action(ccid, "playback_start", { audio_url: `${PUBLIC_URL}/voicemail.mp3` });
  if (r?.errors) {
    console.error("VOICEMAIL PLAYBACK FAILED:", JSON.stringify(r.errors));
    line.vmPlaying = false;
    action(ccid, "hangup").catch(() => {});
  }
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
    if (MY_PHONE && ccid) {
      await action(ccid, "answer");
      await action(ccid, "transfer", { to: ringMe(MY_PHONE) });
    }
    return;
  }
  // ---- DIRECT CALL: your phone answered → dial the lead immediately ----
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
    const bridgeResult = await action(ccid, "bridge", { call_control_id: directCallPending.seanCcid });
    console.log("Bridge result:", JSON.stringify(bridgeResult));
    directCallPending = null;
    return;
  }
  // ---- CALLBACK: your phone answered a call-back → now dial the lead ----
  if (type === "call.answered" && state.callback) {
    await action(ccid, "transfer", { to: state.callbackTo, from: state.callbackFrom || FROM_NUMBERS[0] });
    return;
  }
  // ---- YOUR leg answered -> put you straight into the conference, held. ----
  if (type === "call.answered" && ccid === session.seanCallId && state.seanHold) {
    console.log("Sean's phone answered — creating conference and holding him.");
    session.confId = await createConferenceWithSean(ccid, session.confName);
    if (!session.confId) {
      console.error("Could not create conference — ending session.");
      await action(ccid, "hangup", {}).catch(() => {});
      session.seanUp = false; session.seanCallId = null; session.pending = null; session.startedAt = null;
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
  // ---- SEAN's leg hung up -> Sean ended the whole session. ----
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
    session.seanUp = false; session.pending = null; session.startedAt = null; session.leadOnLine = false; session.connectedLeadCcid = null;
    console.log("Session ended.");
    return;
  }
  // ---- a LEAD line: find its batch
  const batch = state.batchId ? batches.get(state.batchId) : null;
  if (!batch) return;
  const line = batch.lines.get(ccid);
  switch (type) {
    case "call.answered": {
      if (!line) break;
      line.status = "ringing";
      recordAnswered(line.fromNumber);
      if (!VM_AUTO) { await bridgeLeadToSean(ccid, batch, line); break; }
      line.awaitingAMD = true;
      line.amdTimer = setTimeout(() => {
        if (line.awaitingAMD) {
          console.log("Detection timed out — treating as human and connecting", ccid);
          bridgeLeadToSean(ccid, batch, line).catch(() => {});
        }
      }, 4000);
      break;
    }
    case "call.machine.premium.detection.ended":
    case "call.machine.detection.ended": {
      if (!VM_AUTO || !line || !line.awaitingAMD) break;
      const result = String(p.result || "").toLowerCase();
      const isMachine = result.includes("machine") || result === "fax";
      line.awaitingAMD = false;
      if (line.amdTimer) { clearTimeout(line.amdTimer); line.amdTimer = null; }
      if (isMachine) {
        line.machineDetected = true;
        line.vmBeepTimer = setTimeout(() => {
          if (line.machineDetected && !line.vmPlaying) dropVoicemail(ccid, batch, line).catch(() => {});
        }, 22000);
      } else {
        await bridgeLeadToSean(ccid, batch, line);
      }
      break;
    }
    case "call.machine.premium.greeting.ended":
    case "call.machine.greeting.ended": {
      if (!VM_AUTO || !line) break;
      const gresult = String(p.result || "").toLowerCase();
      if ((gresult === "beep_detected" || line.machineDetected) && !line.vmPlaying) {
        line.awaitingAMD = false;
        if (line.amdTimer) { clearTimeout(line.amdTimer); line.amdTimer = null; }
        if (line.vmBeepTimer) { clearTimeout(line.vmBeepTimer); line.vmBeepTimer = null; }
        await dropVoicemail(ccid, batch, line);
      }
      break;
    }
    case "call.playback.ended":
    case "call.playback.stopped": {
      if (line && line.vmPlaying) {
        line.vmPlaying = false;
        line.status = "voicemail";
        console.log("Voicemail finished — hanging up", ccid);
        action(ccid, "hangup").catch(() => {});
      }
      break;
    }
    case "call.hangup":
      if (line && line.amdTimer) { clearTimeout(line.amdTimer); line.amdTimer = null; }
      if (line && line.vmBeepTimer) { clearTimeout(line.vmBeepTimer); line.vmBeepTimer = null; }
      if (line) { line.awaitingAMD = false; line.machineDetected = false; }
      if (line && line.status === "connected") {
        line.status = "ended";
        batch.connected = false;
        session.leadOnLine = false;
        session.connectedLeadCcid = null;
        console.log("Lead left the conference — Sean stays on, ready for next lead.");
      } else if (line) {
        line.status = line.status || "dropped";
      }
      if (session.connectedLeadCcid === ccid) {
        session.leadOnLine = false;
        session.connectedLeadCcid = null;
      }
      if (![...batch.lines.values()].some(l => l.status === "dialing" || l.status === "ringing"))
        batch.done = true;
      break;
    default:
      break;
  }
});
// ============================================================================
//  INBOUND SMS WEBHOOK
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
  const { myPhone, to, market } = req.body || {};
  if (!myPhone || !to) return res.status(400).json({ error: "need myPhone and to" });
  const toClean = toE164(to, market || DEFAULT_MARKET);
  if (!toClean) return res.status(400).json({ error: "invalid 'to' number" });
  const from = FROM_NUMBERS[0] || "";
  directCallPending = null;
  try {
    console.log("Direct call: calling your phone", myPhone, "then will dial", toClean);
    const call1 = await telnyx("/calls", {
      connection_id: CONNECTION_ID, to: ringMe(myPhone), from, webhook_url: `${PUBLIC_URL}/webhook`,
    });
    const ccid = call1?.data?.call_control_id;
    directCallPending = { seanCcid: ccid, to: toClean, from };
    console.log("Direct call: your call ID =", ccid);
    res.json({ ok: true, callId: ccid });
  } catch (e) { console.error("Direct call error:", e); res.status(500).json({ error: e.message }); }
});
// ============================================================================
//  CALL BACK & TEXT — use the SAME Telnyx number they saw
// ============================================================================
app.post("/call-back", async (req, res) => {
  const { myPhone, to, fromNumber, market } = req.body || {};
  if (!myPhone || !to) return res.status(400).json({ error: "need myPhone and to" });
  const toClean = toE164(to, market || DEFAULT_MARKET);
  if (!toClean) return res.status(400).json({ error: "invalid 'to' number" });
  const from = fromNumber || FROM_NUMBERS[0] || "";
  if (!from) return res.status(500).json({ error: "no FROM number" });
  try {
    const call1 = await telnyx("/calls", {
      connection_id: CONNECTION_ID, to: ringMe(myPhone), from, webhook_url: `${PUBLIC_URL}/webhook`,
      client_state: b64({ callback: true, callbackTo: toClean, callbackFrom: from }),
    });
    res.json({ ok: true, callId: call1?.data?.call_control_id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post("/webhook-callback-bridge", async (req, res) => { res.sendStatus(200); });
app.post("/send-text", async (req, res) => {
  const { to, fromNumber, body } = req.body || {};
  if (!to || !body) return res.status(400).json({ error: "need to and body" });
  const from = fromNumber || FROM_NUMBERS[0] || "";
  if (!from) return res.status(500).json({ error: "no FROM number" });
  try {
    const result = await telnyx("/messages", { from, to, text: body, type: "SMS" });
    if (result.errors) return res.json({ ok: false, error: result.errors[0]?.detail || "SMS failed" });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
// ============================================================================
//  STATUS + LOGS
// ============================================================================
// Synthetic (non-dialed) line keys we must never send a Telnyx hangup for.
const isSyntheticLine = (ccid) => /^(blocked_|badnum_|dispo_)/.test(String(ccid));
app.get("/batch-status/:id", (req, res) => {
  const batch = batches.get(req.params.id);
  if (!batch) return res.json({ lines: [], done: false });
  res.json({
    done: batch.done,
    connected: batch.connected,
    lines: [...batch.lines.values()].map(l => ({ leadId: l.leadId, status: l.status, fromNumber: l.fromNumber })),
  });
});
app.get("/inbound-log", (req, res) => { res.json({ log: inboundLog }); });
app.post("/hangup-all", async (req, res) => {
  for (const batch of batches.values()) {
    for (const [ccid, l] of batch.lines) {
      if (!isSyntheticLine(ccid)) action(ccid, "hangup").catch(() => {});
      l.status = "ended";
    }
    batch.done = true;
    batch.connected = false;
  }
  res.json({ ok: true });
});
app.post("/session/end", async (req, res) => {
  for (const batch of batches.values()) {
    for (const [ccid] of batch.lines) {
      if (!isSyntheticLine(ccid)) action(ccid, "hangup").catch(() => {});
    }
    batch.done = true; batch.connected = false;
  }
  if (session.seanCallId) action(session.seanCallId, "hangup").catch(() => {});
  session.confId = session.confName = session.seanCallId = null;
  session.seanUp = false; session.pending = null; session.leadOnLine = false; session.connectedLeadCcid = null;
  console.log("Session ended by user — all legs dropped.");
  res.json({ ok: true });
});
app.get("/session/status", (_req, res) => {
  let activeLines = [];
  for (const [, b] of batches) {
    if (!b.done) {
      activeLines = [...b.lines.values()].map(l => ({ to: l.to, status: l.status, fromNumber: l.fromNumber, attempt: l.attempt }));
    }
  }
  res.json({ seanUp: session.seanUp, seanOnCall: Boolean(session.seanCallId), activeLines });
});
app.post("/drop-lead", async (req, res) => {
  let dropped = 0;
  for (const batch of batches.values()) {
    for (const [ccid, l] of batch.lines) {
      if (l.status === "connected" && !isSyntheticLine(ccid)) {
        action(ccid, "hangup").catch(() => {});
        l.status = "ended";
        dropped++;
      }
    }
    batch.connected = false;
  }
  session.leadOnLine = false;
  session.connectedLeadCcid = null;
  console.log("Dropped current lead(s):", dropped, "— Sean stays on the line.");
  res.json({ ok: true, dropped });
});
app.post("/disposition", (req, res) => {
  const { phone, status } = req.body || {};
  if (!phone || !status) return res.status(400).json({ error: "need phone and status" });
  dispositions[phone] = { status, at: new Date().toISOString() };
  console.log("Disposition recorded:", phone, "->", status);
  res.json({ ok: true });
});
app.get("/dispositions", (_req, res) => { res.json({ dispositions }); });
app.get("/call-log", (_req, res) => {
  const log = Object.entries(callHistory).map(([phone, h]) => ({
    phone, name: h.name || "", attempts: h.attempts || 0,
    lastCalledAt: h.lastCalledAt ? new Date(h.lastCalledAt).toISOString() : null,
    lastOutcome: h.lastOutcome || "dialed",
  })).sort((a, b) => (b.lastCalledAt || "").localeCompare(a.lastCalledAt || ""));
  res.json({ totalNumbers: log.length, totalCalls: log.reduce((s, x) => s + x.attempts, 0), log });
});
app.get("/numbers", (_req, res) => { res.json({ numbers: FROM_NUMBERS, reputation: numberReputation() }); });
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
  const balance = await checkBalance();
  const alerts = computeAlerts();
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
    alerts,
    balance: balance,
    apiKeyOk: sysHealth.apiKeyOk,
    consecutiveCallFailures: sysHealth.consecutiveCallFailures,
    lastFailureReason: sysHealth.lastFailureReason,
    lastFailureAt: sysHealth.lastFailureAt,
    reputation: rep,
  });
});
// Manual "clear the locked-number memory" button (GET works, so the URL bar is enough).
function doResetAttempts(req, res) {
  const cleared = Object.keys(callHistory).length;
  for (const k in callHistory) delete callHistory[k];
  resetDayKey = localDayKey();
  console.log(`Manual reset — cleared attempt counts for ${cleared} number(s)`);
  res.json({ ok: true, cleared, message: `Cleared ${cleared} number(s). All leads are callable again.` });
}
app.get("/reset-attempts", doResetAttempts);
app.post("/reset-attempts", doResetAttempts);
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Switchboard v4.1.0-INTL backend listening on :${PORT} — FORCE_MARKET=${FORCE_MARKET || "(unset)"} — DEFAULT_MARKET=${DEFAULT_MARKET || "(unset)"} — daily attempt-reset at local midnight (UTC+${DAILY_RESET_UTC_OFFSET})`));
