import express from "express";
import fetch from "node-fetch";
import Database from "better-sqlite3";

const app = express();
app.use(express.json());
app.use(express.static("public"));

const db = new Database("cache.db");

// Cache op volledige VAT (incl. prefix)
db.exec(`
  CREATE TABLE IF NOT EXISTS vat_cache (
    vat_number TEXT NOT NULL PRIMARY KEY,
    response_json TEXT NOT NULL,
    checked_at INTEGER NOT NULL
  );
`);

const PORT = process.env.PORT || 3000;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h

// ====== "ZO TRAAG ALS MOGELIJK" instellingen ======
const MIN_GAP_GLOBAL_MS = 4000;        // minimaal 4s tussen elke upstream call (zeer traag)
const MIN_GAP_PER_COUNTRY_MS = 6000;   // minimaal 6s per country (nog trager)
const FETCH_TIMEOUT_MS = 12000;        // hard timeout per attempt
const MAX_ATTEMPTS = 4;               // retries bij VIES-busy
const BACKOFF_BASE_MS = 5000;          // 5s, 10s, 20s (max attempt=4)
const BACKOFF_CAP_MS = 20000;          // cap backoff
const MAX_TOTAL_BUDGET_MS = 45000;     // totale tijd per VAT (voorkomt oneindig hangen)

const RETRYABLE_MARKERS = [
  "MS_MAX_CONCURRENT_REQ",
  "GLOBAL_MAX_CONCURRENT_REQ",
  "SERVICE_UNAVAILABLE",
  "MS_UNAVAILABLE",
  "TIMEOUT",
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function jitter(ms) {
  // kleine jitter om "thundering herd" te vermijden
  return ms + Math.floor(Math.random() * 500);
}

function normalizeVatLine(line) {
  return String(line || "")
    .trim()
    .replace(/\s+/g, "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
}

function extractCountry(vatNumber) {
  if (!vatNumber || vatNumber.length < 3) return null;
  const cc = vatNumber.slice(0, 2);
  if (!/^[A-Z]{2}$/.test(cc)) return null;
  return cc;
}

function looksRetryableBody(text) {
  const t = String(text || "");
  return RETRYABLE_MARKERS.some((m) => t.includes(m));
}

// ====== Globale single-flight + spacing ======
let upstreamLock = Promise.resolve(); // mutex
let nextGlobalAt = 0;
const nextByCountryAt = new Map();

async function withUpstreamLock(fn) {
  let release;
  const p = new Promise((r) => (release = r));
  const prev = upstreamLock;
  upstreamLock = p;
  await prev;
  try {
    return await fn();
  } finally {
    release();
  }
}

async function enforceSpacing(country) {
  const now = Date.now();
  const nextCountry = nextByCountryAt.get(country) || 0;
  const waitUntil = Math.max(nextGlobalAt, nextCountry, now);

  if (waitUntil > now) {
    await sleep(waitUntil - now);
  }

  const after = Date.now();
  nextGlobalAt = after + MIN_GAP_GLOBAL_MS;
  nextByCountryAt.set(country, after + MIN_GAP_PER_COUNTRY_MS);
}

// ====== Upstream call met retries/backoff ======
async function fetchWithTimeout(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const r = await fetch(url, { signal: controller.signal });
    return r;
  } finally {
    clearTimeout(timer);
  }
}

async function callVatcomplyWithRetry(vatNumber, country) {
  const url = `https://api.vatcomply.com/vat?vat_number=${encodeURIComponent(vatNumber)}`;
  const started = Date.now();

  let lastErr = null;

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    // budget guard
    if (Date.now() - started > MAX_TOTAL_BUDGET_MS) {
      return {
        ok: false,
        status: 504,
        body: "TIME_BUDGET_EXCEEDED",
        retryable: true,
      };
    }

    // Enforce single-flight + spacing per attempt
    const resp = await withUpstreamLock(async () => {
      await enforceSpacing(country);
      return await fetchWithTimeout(url);
    });

    // OK
    if (resp.ok) {
      const data = await resp.json();
      return { ok: true, data };
    }

    // Read body (text) for diagnosis / marker detection
    const bodyText = await resp.text().catch(() => "");
    const status = resp.status;

    const retryable =
      status === 429 ||
      status === 408 ||
      status === 500 ||
      status === 502 ||
      status === 503 ||
      status === 504 ||
      (status === 400 && looksRetryableBody(bodyText));

    lastErr = { status, body: bodyText.slice(0, 800), retryable };

    if (!retryable || attempt === MAX_ATTEMPTS) {
      return { ok: false, status, body: lastErr.body, retryable };
    }

    // Exponential backoff (traag)
    const rawBackoff = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * Math.pow(2, attempt - 1));
    await sleep(jitter(rawBackoff));
  }

  // Should not reach
  return { ok: false, status: 500, body: String(lastErr?.body || "UNKNOWN"), retryable: true };
}

// ====== Endpoint ======
app.post("/api/validate-batch", async (req, res) => {
  try {
    const lines = Array.isArray(req.body.vat_numbers) ? req.body.vat_numbers : [];
    if (!lines.length) return res.status(400).json({ error: "vat_numbers array is required" });

    const results = [];

    for (const raw of lines) {
      const vatNumber = normalizeVatLine(raw);
      const cc = extractCountry(vatNumber);

      if (!vatNumber) {
        results.push({ input: raw, error: "EMPTY_LINE" });
        continue;
      }
      if (!cc) {
        results.push({ input: raw, vat_number: vatNumber, error: "MISSING_OR_INVALID_COUNTRY_PREFIX" });
        continue;
      }

      // cache
      const row = db
        .prepare("SELECT response_json, checked_at FROM vat_cache WHERE vat_number=?")
        .get(vatNumber);

      if (row && Date.now() - row.checked_at <= CACHE_TTL_MS) {
        results.push({ input: raw, source: "cache", ...JSON.parse(row.response_json) });
        continue;
      }

      // upstream (with retry/backoff)
      const upstream = await callVatcomplyWithRetry(vatNumber, cc);

      if (!upstream.ok) {
        // belangrijke: markeer als "busy" als retryable, zodat UI dit niet als definitief invalid ziet
        const errorCode = upstream.retryable ? "VIES_BUSY_TRY_LATER" : "UPSTREAM_ERROR";
        results.push({
          input: raw,
          vat_number: vatNumber,
          source: "vatcomply",
          error: errorCode,
          status: upstream.status,
          body: upstream.body,
        });
        continue;
      }

      // cache store
      db.prepare(`
        INSERT INTO vat_cache(vat_number, response_json, checked_at)
        VALUES (?, ?, ?)
        ON CONFLICT(vat_number) DO UPDATE SET
          response_json=excluded.response_json,
          checked_at=excluded.checked_at
      `).run(vatNumber, JSON.stringify(upstream.data), Date.now());

      results.push({ input: raw, source: "vatcomply", ...upstream.data });
    }

    res.json({ count: results.length, results });
  } catch (e) {
    res.status(500).json({ error: "Server error", detail: String(e?.message || e) });
  }
});

app.listen(PORT, () => {
  console.log(`Running on http://localhost:${PORT}`);
});
