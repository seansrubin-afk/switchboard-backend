// Country dial codes for the markets we call. Add a market here to support a new
// country (key = the market code the frontend sends, value = its phone country code).
const DIAL_CODES = { US: "1", CA: "1", AU: "61", NZ: "64", IE: "353", UK: "44", GB: "44" };

// Normalize any phone number to clean +E164 before dialing. Lead lists are messy and
// country-specific: AU/UK/IE/NZ national numbers carry a leading "0" trunk digit that
// must be DROPPED and replaced with the country code, or Telnyx rejects them (D11
// "destination number invalid"). `market` (e.g. "AU") tells us which code to add.
function toE164(raw, market) {
  if (raw == null) return null;
  const s = String(raw).trim();

  // Already +E164 (e.g. a phone_e164 column) — strip formatting, keep the +, trust it.
  if (s.startsWith("+")) {
    const d = s.replace(/[^\d]/g, "");
    return d ? "+" + d : null;
  }

  const digits = s.replace(/[^\d]/g, "");
  if (!digits) return null;

  const mkt  = String(market || "").toUpperCase();
  const code = DIAL_CODES[mkt];

  // North America (or unknown market): keep the original behaviour.
  if (!code || mkt === "US" || mkt === "CA") {
    if (digits.length === 10) return "+1" + digits;
    if (digits.length === 11 && digits[0] === "1") return "+" + digits;
    return "+" + digits;
  }

  // AU / NZ / IE / UK: drop the leading national trunk 0, then prepend the country code.
  const national = digits.replace(/^0+/, "");
  if (national.startsWith(code)) return "+" + national; // already carried its code
  return "+" + code + national;
}
