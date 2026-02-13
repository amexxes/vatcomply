import express from "express";
import fetch from "node-fetch";
import Database from "better-sqlite3";

const app = express();
app.use(express.json());
app.use(express.static("public"));

const db = new Database("cache.db");

// Success cache (alleen succesvolle responses)
db.exec(`
  CREATE TABLE IF NOT EXISTS vat_cache (
    vat_number TEXT NOT NULL PRIMARY KEY,
    response_json TEXT NOT NULL,
    checked_at INTEGER NOT NULL
  );
`);

// Pending queue (voor VIES busy / MS_MAX_CONCURRENT_REQ etc.)
db.exec(`
  CREATE TABLE IF NOT EXISTS vat_pending (
    vat_number TEXT NOT NULL PRIMARY KEY,
    country TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    next_retry_at INTEGER NOT NULL,
    last_status INTEGER,
    last_body TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  );
`);

const PORT = process.env.PORT || 3000;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h

// ====== EXTREEM TRAAG (max 1 tegelijk) ======
const MIN_GAP_GLOBAL_MS = 8000;        // 8s tussen upstream calls
const MIN_GAP_PER_COUNTRY_MS = 12000;  // 12s per country
const FETCH_TIMEOUT_MS = 15000;        // per attempt
const MAX_ATTEMPTS = 2;               // korte retries (we queue’en pending)
const BACKOFF_BASE_MS = 8000;          // 8s, 16s
const BACKOFF_CAP_MS = 20000;

// Pending retry schedule (automatisch): 30m, 60m, 120m ... capped 24h
const PENDING_BASE_DELAY_MS = 30 * 60 * 1000;
const PENDING_CAP_MS = 24 * 60 * 60 * 1000;

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
  return ms + Math.floor(Math.random() * 700);
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

// ====== single-flight + spacing ======
let upstreamLock = Promise.resolve();
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

  if (waitUntil > now) await sleep(waitUntil - now);

  const after = Date.now();
  nextGlobalAt = after + MIN_GAP_GLOBAL_MS;
  nextByCountryAt.set(country, after + MIN_GAP_PER_COUNTRY_MS);
}

async function fetchWithTimeout(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function pendingNextDelayMs(attempts) {
  const d = PENDING_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempts - 1));
  return Math.min(PENDING_CAP_MS, d);
}

function upsertPending(vatNumber, country, status, body) {
  const now = Date.now();
  const existing = db
    .prepare("SELECT attempts FROM vat_pending WHERE vat_number=?")
    .get(vatNumber);

  const attempts = (existing?.attempts || 0) + 1;
  const next_retry_at = now + pendingNextDelayMs(attempts);

  db.prepare(`
    INSERT INTO vat_pending(vat_number, country, attempts, next_retry_at, last_status, last_body, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(vat_number) DO UPDATE SET
      attempts=excluded.attempts,
      next_retry_at=excluded.next_retry_at,
      last_status=excluded.last_status,
      last_body=excluded.last_body,
      updated_at=excluded.updated_at
  `).run(
    vatNumber,
    country,
    attempts,
    next_retry_at,
    status ?? null,
    String(body || "").slice(0, 800),
    existing ? db.prepare("SELECT created_at FROM vat_pending WHERE vat_number=?").get(vatNumber).created_at : now,
    now
  );

  return { attempts, next_retry_at };
}

function clearPending(vatNumber) {
  db.prepare("DELETE FROM vat_pending WHERE vat_number=?").run(vatNumber);
}

function getPending(vatNumber) {
  return db
    .prepare("SELECT vat_number, country, attempts, next_retry_at, last_status, last_body FROM vat_pending WHERE vat_number=?")
    .get(vatNumber);
}

async function callVatcomplyWithRetry(vatNumber, country) {
  const url = `https://api.vatcomply.com/vat?vat_number=${encodeURIComponent(vatNumber)}`;

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const resp = await withUpstreamLock(async () => {
      await enforceSpacing(country);
      return await fetchWithTimeout(url);
    });

    if (resp.ok) {
      const data = await resp.json();
      return { ok: true, data };
    }

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

    if (!retryable || attempt === MAX_ATTEMPTS) {
      return { ok: false, status, body: bodyText.slice(0, 800), retryable };
    }

    const backoff = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * Math.pow(2, attempt - 1));
    await sleep(jitter(backoff));
  }

  return { ok: false, status: 500, body: "UNKNOWN", retryable: true };
}

// ====== Validate endpoint ======
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

      // success cache
      const cached = db
        .prepare("SELECT response_json, checked_at FROM vat_cache WHERE vat_number=?")
        .get(vatNumber);

      if (cached && Date.now() - cached.checked_at <= CACHE_TTL_MS) {
        results.push({ input: raw, source: "cache", ...JSON.parse(cached.response_json) });
        continue;
      }

      // pending gate (voorkomt eindeloos opnieuw proberen)
      const pending = getPending(vatNumber);
      if (pending && pending.next_retry_at > Date.now()) {
        results.push({
          input: raw,
          vat_number: vatNumber,
          source: "pending",
          error: "PENDING_RETRY",
          status: pending.last_status,
          attempts: pending.attempts,
          next_retry_at: pending.next_retry_at
        });
        continue;
      }

      // upstream (met korte retry + daarna pending)
      const upstream = await callVatcomplyWithRetry(vatNumber, cc);

      if (!upstream.ok) {
        if (upstream.retryable) {
          const p = upsertPending(vatNumber, cc, upstream.status, upstream.body);
          results.push({
            input: raw,
            vat_number: vatNumber,
            source: "pending",
            error: "PENDING_RETRY",
            status: upstream.status,
            attempts: p.attempts,
            next_retry_at: p.next_retry_at,
            body: upstream.body
          });
        } else {
          clearPending(vatNumber);
          results.push({
            input: raw,
            vat_number: vatNumber,
            source: "vatcomply",
            error: "UPSTREAM_ERROR",
            status: upstream.status,
            body: upstream.body
          });
        }
        continue;
      }

      // success: cache + clear pending
      clearPending(vatNumber);
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

// ====== Handmatige retry endpoint ======
// POST /api/retry-pending { vat_numbers?: [...], force?: true }
// - force=true: probeert direct, ook als next_retry_at nog niet bereikt is
app.post("/api/retry-pending", async (req, res) => {
  try {
    const vatNumbers = Array.isArray(req.body.vat_numbers) ? req.body.vat_numbers : null;
    const force = !!req.body.force;

    let rows;
    if (vatNumbers && vatNumbers.length) {
      const norm = vatNumbers.map(normalizeVatLine).filter(Boolean);
      rows = norm.map(v => getPending(v) || { vat_number: v, country: extractCountry(v) }).filter(r => r?.vat_number);
    } else {
      const now = Date.now();
      rows = db.prepare(`
        SELECT vat_number, country, attempts, next_retry_at
        FROM vat_pending
        WHERE next_retry_at <= ?
        ORDER BY next_retry_at ASC
        LIMIT 50
      `).all(now);
    }

    const results = [];

    for (const r of rows) {
      const vat_number = r.vat_number;
      const country = r.country || extractCountry(vat_number);

      if (!country) {
        results.push({ vat_number, error: "MISSING_OR_INVALID_COUNTRY_PREFIX" });
        continue;
      }

      const pending = getPending(vat_number);
      if (!force && pending && pending.next_retry_at > Date.now()) {
        results.push({
          vat_number,
          source: "pending",
          error: "PENDING_RETRY",
          attempts: pending.attempts,
          next_retry_at: pending.next_retry_at
        });
        continue;
      }

      const upstream = await callVatcomplyWithRetry(vat_number, country);

      if (!upstream.ok) {
        if (upstream.retryable) {
          const p = upsertPending(vat_number, country, upstream.status, upstream.body);
          results.push({
            vat_number,
            source: "pending",
            error: "PENDING_RETRY",
            status: upstream.status,
            attempts: p.attempts,
            next_retry_at: p.next_retry_at,
            body: upstream.body
          });
        } else {
          clearPending(vat_number);
          results.push({
            vat_number,
            source: "vatcomply",
            error: "UPSTREAM_ERROR",
            status: upstream.status,
            body: upstream.body
          });
        }
        continue;
      }

      clearPending(vat_number);

      db.prepare(`
        INSERT INTO vat_cache(vat_number, response_json, checked_at)
        VALUES (?, ?, ?)
        ON CONFLICT(vat_number) DO UPDATE SET
          response_json=excluded.response_json,
          checked_at=excluded.checked_at
      `).run(vat_number, JSON.stringify(upstream.data), Date.now());

      results.push({ vat_number, source: "vatcomply", ...upstream.data });
    }

    res.json({ count: results.length, results });
  } catch (e) {
    res.status(500).json({ error: "Server error", detail: String(e?.message || e) });
  }
});

app.listen(PORT, () => {
  console.log(`Running on http://localhost:${PORT}`);
});
