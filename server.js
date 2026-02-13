import express from "express";
import fetch from "node-fetch";
import Database from "better-sqlite3";

const app = express();
app.use(express.json());
app.use(express.static("public"));

const db = new Database("cache.db");

// Cache op volledige VAT (incl. prefix) is het simpelst
db.exec(`
  CREATE TABLE IF NOT EXISTS vat_cache (
    vat_number TEXT NOT NULL PRIMARY KEY,
    response_json TEXT NOT NULL,
    checked_at INTEGER NOT NULL
  );
`);

const PORT = process.env.PORT || 3000;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h

function normalizeVatLine(line) {
  // Verwijder spaties/tekens, maak uppercase
  return String(line || "")
    .trim()
    .replace(/\s+/g, "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
}

function extractCountry(vatNumber) {
  // VIES/VAT prefixes zijn 2 letters (bijv. NL, DE, EL, XI)
  if (!vatNumber || vatNumber.length < 3) return null;
  const cc = vatNumber.slice(0, 2);
  if (!/^[A-Z]{2}$/.test(cc)) return null;
  return cc;
}

async function callVatcomply(vatNumber) {
  const url = `https://api.vatcomply.com/vat?vat_number=${encodeURIComponent(vatNumber)}`;
  const r = await fetch(url);

  if (!r.ok) {
    const text = await r.text().catch(() => "");
    return {
      ok: false,
      status: r.status,
      body: text.slice(0, 500),
    };
  }

  const data = await r.json();
  return { ok: true, data };
}

// Batch validate: { vat_numbers: ["NL...", "DE...", ...] }
app.post("/api/validate-batch", async (req, res) => {
  try {
    const lines = Array.isArray(req.body.vat_numbers) ? req.body.vat_numbers : [];
    if (!lines.length) {
      return res.status(400).json({ error: "vat_numbers array is required" });
    }

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

      // cache check
      const row = db
        .prepare("SELECT response_json, checked_at FROM vat_cache WHERE vat_number=?")
        .get(vatNumber);

      if (row && Date.now() - row.checked_at <= CACHE_TTL_MS) {
        results.push({ input: raw, source: "cache", ...JSON.parse(row.response_json) });
        continue;
      }

      // upstream call
      const upstream = await callVatcomply(vatNumber);
      if (!upstream.ok) {
        results.push({
          input: raw,
          vat_number: vatNumber,
          error: "UPSTREAM_ERROR",
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
