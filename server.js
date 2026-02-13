
import express from "express";
import fetch from "node-fetch";
import Database from "better-sqlite3";

const app = express();
app.use(express.json());
app.use(express.static("public"));

const db = new Database("cache.db");
db.exec(`
  CREATE TABLE IF NOT EXISTS vat_cache (
    country TEXT NOT NULL,
    vat TEXT NOT NULL,
    response_json TEXT NOT NULL,
    checked_at INTEGER NOT NULL,
    PRIMARY KEY(country, vat)
  );
`);

const PORT = process.env.PORT || 3000;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;

app.post("/api/validate", async (req, res) => {
  try {
    const country = String(req.body.country || "").toUpperCase().trim();
    const vat = String(req.body.vat || "").replace(/\s+/g, "").toUpperCase();

    if (!country || country.length !== 2) {
      return res.status(400).json({ error: "Invalid country code." });
    }
    if (!vat) return res.status(400).json({ error: "VAT number required." });

    const row = db.prepare(
      "SELECT response_json, checked_at FROM vat_cache WHERE country=? AND vat=?"
    ).get(country, vat);

    if (row && (Date.now() - row.checked_at <= CACHE_TTL_MS)) {
      return res.json({ source: "cache", ...JSON.parse(row.response_json) });
    }

    const vatClean = String(req.body.vat || "")
  .replace(/\s+/g, "")
  .replace(/[^A-Za-z0-9]/g, "")
  .toUpperCase();

const countryClean = String(req.body.country || "").toUpperCase().trim();

// als vat al begint met landcode (NL..., DE..., XI...), niet nog eens prefixen
const vatNumber =
  vatClean.startsWith(countryClean) ? vatClean : `${countryClean}${vatClean}`;

    const url = `https://api.vatcomply.com/vat?vat_number=${encodeURIComponent(vatNumber)}`;
    const r = await fetch(url);

    if (!r.ok) {
      return res.status(502).json({ error: "Upstream error", status: r.status });
    }

    const data = await r.json();

    db.prepare(`
      INSERT INTO vat_cache(country, vat, response_json, checked_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(country, vat) DO UPDATE SET
        response_json=excluded.response_json,
        checked_at=excluded.checked_at
    `).run(country, vat, JSON.stringify(data), Date.now());

    res.json({ source: "vatcomply", ...data });

  } catch (e) {
    res.status(500).json({ error: "Server error", detail: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`Running on http://localhost:${PORT}`);
});
