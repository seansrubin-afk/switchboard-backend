import express from "express";

const app = express();
app.use(express.json());

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

const TELNYX_API_KEY = process.env.TELNYX_API_KEY;
const CONNECTION_ID = process.env.TELNYX_CONNECTION_ID;
const FROM_NUMBERS = (process.env.FROM_NUMBERS || "").split(",").map(s => s.trim()).filter(Boolean);
const PUBLIC_URL = process.env.PUBLIC_URL;
const DEFAULT_MARKET = (process.env.DEFAULT_MARKET || "US").toUpperCase();
const SAFE_HARBOUR_MSG = process.env.SAFE_HARBOUR_MSG || "Hello, sorry we missed you. This was Sean from Rubin5 about your business website. No need to call back. To stop these calls, just let us know. Thank you.";

const TELNYX = "https://api.telnyx.com/v2";
const session = { confId: null, confName: null, seanCallId: null, seanUp: false, pending: null };
const batches = new Map();
let rrIndex = 0;

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
  if (!res.ok) console.error("Telnyx error", res.status, path, text.slice(0, 400));
  return json;
}

const action = (ccid, name, payload = {}) =>
  telnyx(`/calls/${ccid}/actions/${name}`, payload);

app.post("/dial-batch", async (req, res) => {
  const { myPhone, leads, market } = req.body || {};
  if (!myPhone || !Array.isArray(leads) || leads.length === 0)
    return res.status(400).json({ error: "need myPhone and leads[]" });
  if (!FROM_NUMBERS.length) return res.status(500).json({ error: "no FROM_NUMBERS configured" });

  const mkt = (market || DEFAULT_MARKET).toUpperCase();
  const batchId = "b_" + Date.now().toString(36);

  if (session.seanUp && session.confName) {
    await fireBatch(batchId, leads, mkt);
    return res.json({ batchId });
  }

  session.pending = { batchId, leads, market: mkt };
  session.confName = "switchboard_" + Date.now().toString(36);
  const call = await telnyx("/calls", {
    connection_id: CONNECTION_ID,
    to: myPhone,
    from: pickFrom(),
    webhook_url: `${PUBLIC_URL}/webhook`,
  });
  session.seanCallId = call?.data?.call_control_id || null;

  batches.set(batchId, { lines: new Map(), connected: false, done: false, market: mkt });
  res.json({ batchId });
});

async function fireBatch(batchId, leads, market = DEFAULT_MARKET) {
  const lines = new Map();
  batches.set(batchId, { lines, connected: false, done: false, market });

  await Promise.all(leads.slice(0, 10).map(async (ld) => {
    const call = await telnyx("/calls", {
      connection_id: CONNECTION_ID,
      to: ld.to,
      from: pickFrom(),
      webhook_url: `${PUBLIC_URL}/webhook`,
      answering_machine_detection: "premium",
      client_state: b64({ batchId, leadId: ld.leadId }),
    });
    const ccid = call?.data?.call_control_id;
    if (ccid) lines.set(ccid, { leadId: ld.leadId, to: ld.to, status: "dialing" });
  }));
}

app.post("/webhook", async (req, res) => {
  res.sendStatus(200);
  const ev = req.body?.data;
  if (!ev) return;
  const type = ev.event_type;
  const p = ev.payload || {};
  const ccid = p.call_control_id;
  const state = p.client_state ? unb64(p.client_state) : {};

  if (type === "call.answered" && ccid === session.seanCallId) {
    const conf = await telnyx("/conferences", {
      name: session.confName,
      call_control_id: ccid,
    });
    session.confId = conf?.data?.id || null;
    session.seanUp = true;
    if (session.pending) {
      const { batchId, leads, market } = session.pending;
      session.pending = null;
      await fireBatch(batchId, leads, market);
    }
    return;
  }

  if (type === "call.hangup" && ccid === session.seanCallId) {
    if (session.pending) {
      const b = batches.get(session.pending.batchId);
      if (b) b.done = true;
    }
    session.confId = session.confName = session.seanCallId = null;
    session.seanUp = false; session.pending = null;
    return;
  }

  const batch = state.batchId ? batches.get(state.batchId) : null;
  if (!batch) return;
  const line = batch.lines.get(ccid);

  switch (type) {
    case "call.answered":
      if (line) line.status = "ringing";
      break;

    case "call.machine.premium.detection.ended":
    case "call.machine.detection.ended": {
      const result = (p.result || "").toLowerCase();
      const isHuman = result.includes("human");
      const isMachine = result.includes("machine");

      if (isHuman && !batch.connected) {
        batch.connected = true;
        if (line) line.status = "connected";
        await telnyx(`/conferences/${session.confId}/actions/join`, {
          call_control_id: ccid,
        });
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
        if ((batch.market || DEFAULT_MARKET) === "US") {
          await action(ccid, "speak", {
            payload: SAFE_HARBOUR_MSG,
            voice: "female",
            language: "en-US",
          });
          setTimeout(() => action(ccid, "hangup").catch(() => {}), 9000);
        } else {
          action(ccid, "hangup").catch(() => {});
        }
      }
      break;
    }

    case "call.hangup":
      if (line && line.status !== "connected") line.status = line.status || "dropped";
      if (![...batch.lines.values()].some(l => l.status === "dialing" || l.status === "ringing"))
        batch.done = true;
      break;

    default:
      break;
  }
});

app.get("/batch-status/:id", (req, res) => {
  const batch = batches.get(req.params.id);
  if (!batch) return res.json({ lines: [], done: false });
  res.json({
    done: batch.done,
    connected: batch.connected,
    lines: [...batch.lines.values()].map(l => ({ leadId: l.leadId, status: l.status })),
  });
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

app.get("/health", (_req, res) => res.json({
  ok: true,
  configured: Boolean(TELNYX_API_KEY && CONNECTION_ID && FROM_NUMBERS.length && PUBLIC_URL),
  fromNumbers: FROM_NUMBERS.length,
}));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Switchboard backend listening on :${PORT}`));
