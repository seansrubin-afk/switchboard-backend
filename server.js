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
