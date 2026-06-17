# Adding the Alert Banner to switchboard.jsx

Three small edits. The backend (v3.4.3) already returns everything; this just displays it.

---

## EDIT 1 — Add state (near your other useState declarations, anywhere in the top block)

Add this line:

```jsx
const [sysAlerts, setSysAlerts] = useState([]);
```

---

## EDIT 2 — Poll /health (paste this whole block right AFTER the inbound-log useEffect, around line 613)

```jsx
  // Poll system health for alerts (balance, failures, key issues)
  useEffect(() => {
    if (!backendUrl) return;
    const pollHealth = () => {
      fetch(backendUrl.replace(/\/$/, "") + "/health")
        .then(r => r.json())
        .then(d => { setSysAlerts(Array.isArray(d.alerts) ? d.alerts : []); })
        .catch(() => {
          // Backend itself is unreachable — that's its own alert.
          setSysAlerts(["BACKEND UNREACHABLE — the server may be down or redeploying."]);
        });
    };
    pollHealth();
    const t = setInterval(pollHealth, 15000);
    return () => clearInterval(t);
  }, [backendUrl]);
```

---

## EDIT 3 — Render the banner (paste near the TOP of your main returned JSX,
## right after the outermost opening wrapper div, so it sits above everything)

```jsx
        {sysAlerts.length > 0 && (
          <div style={{
            background: "#b00020",
            color: "#fff",
            padding: "12px 16px",
            borderRadius: "8px",
            margin: "0 0 12px 0",
            fontWeight: 600,
            fontSize: "14px",
            lineHeight: 1.5,
            border: "1px solid #ff4d4d",
          }}>
            {sysAlerts.map((a, i) => (
              <div key={i}>⚠ {a}</div>
            ))}
          </div>
        )}
```

---

## What you'll see

A red banner at the top of the app, ONLY when something is wrong. It names the exact problem:

- "TELNYX BALANCE LOW: $0.84 — top up to keep calling."
- "TELNYX API KEY REJECTED — calls cannot be placed. Check the key in Railway."
- "5 CALLS FAILED IN A ROW — Telnyx rejected the call (403)... Upgrade to 10 calls."
- "Bad phone number format — one or more leads have an invalid number."
- "BACKEND UNREACHABLE — the server may be down or redeploying."

When everything is healthy, the banner is invisible. It refreshes every 15 seconds.
