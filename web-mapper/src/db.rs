//! SQLite graph database — WAL mode, batch transactions, same schema as Python web_mapper.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, Transaction};
use std::collections::HashSet;
use std::path::Path;

use crate::fetcher::FetchResult;

/// Open (or create) the web_map.db with WAL + busy_timeout.
pub fn open(path: &str) -> Result<Connection> {
    let parent = Path::new(path).parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;

    let conn = Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA busy_timeout=30000;
         PRAGMA cache_size=-64000;",
    )?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS domains (
            domain TEXT PRIMARY KEY,
            fetched BOOLEAN DEFAULT 0,
            fetch_time TEXT,
            title TEXT,
            emails TEXT,
            phones TEXT,
            is_business BOOLEAN,
            category TEXT,
            gnn_score REAL,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS tracking_ids (
            id TEXT PRIMARY KEY,
            type TEXT,
            domain_count INTEGER DEFAULT 0,
            first_seen TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS domain_ids (
            domain TEXT,
            tracking_id TEXT,
            PRIMARY KEY (domain, tracking_id)
        );
        CREATE TABLE IF NOT EXISTS clusters (
            cluster_id INTEGER PRIMARY KEY AUTOINCREMENT,
            anchor_id TEXT,
            domains TEXT,
            emails TEXT,
            owner_signal TEXT,
            size INTEGER,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_di_tid ON domain_ids(tracking_id);
        CREATE INDEX IF NOT EXISTS idx_di_dom ON domain_ids(domain);
        CREATE INDEX IF NOT EXISTS idx_domains_fetched ON domains(fetched);
        CREATE INDEX IF NOT EXISTS idx_domains_gnn ON domains(gnn_score);",
    )?;
    Ok(conn)
}

/// Insert a domain->tracking_id link (idempotent).
pub fn link(conn: &Connection, domain: &str, tid: &str, tid_type: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO tracking_ids(id, type) VALUES(?1, ?2)",
        params![tid, tid_type],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO domain_ids(domain, tracking_id) VALUES(?1, ?2)",
        params![domain, tid],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO domains(domain) VALUES(?1)",
        params![domain],
    )?;
    Ok(())
}

/// Batch-insert fetch results in a single transaction. Returns count of new tracking IDs.
pub fn flush_batch(conn: &mut Connection, results: &[FetchResult]) -> Result<usize> {
    let tx = conn.transaction()?;
    let mut new_ids = 0;

    for r in results {
        let emails_json = if r.emails.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&r.emails)?)
        };
        let phones_json = if r.phones.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&r.phones)?)
        };
        let is_biz = !r.emails.is_empty() || !r.phones.is_empty();

        tx.execute(
            "UPDATE domains SET fetched=1, fetch_time=?1, title=?2, emails=?3,
             phones=?4, is_business=?5, server=?6, redirect_domain=?7 WHERE domain=?8",
            params![r.fetch_time, r.title, emails_json, phones_json, is_biz,
                    r.server, r.redirect_domain, r.domain],
        )?;

        // If redirect detected, add redirect target as new domain + cluster edge
        if let Some(ref redir) = r.redirect_domain {
            tx.execute("INSERT OR IGNORE INTO domains(domain) VALUES(?1)", params![redir])?;
            // Create a synthetic tracking ID for redirect clustering
            let redir_tid = format!("REDIR:{}", redir);
            tx.execute("INSERT OR IGNORE INTO tracking_ids(id, type) VALUES(?1, 'redirect')", params![redir_tid])?;
            tx.execute("INSERT OR IGNORE INTO domain_ids(domain, tracking_id) VALUES(?1, ?2)", params![r.domain, redir_tid])?;
            tx.execute("INSERT OR IGNORE INTO domain_ids(domain, tracking_id) VALUES(?1, ?2)", params![redir, redir_tid])?;
        }

        for (tid, ttype) in &r.tracking_ids {
            let exists: bool = tx
                .query_row(
                    "SELECT 1 FROM tracking_ids WHERE id=?1",
                    params![tid],
                    |_| Ok(true),
                )
                .unwrap_or(false);
            if !exists {
                new_ids += 1;
            }
            // Upgrade phantoms
            let is_phantom: bool = tx
                .query_row(
                    "SELECT 1 FROM tracking_ids WHERE id=?1 AND type='ua_phantom'",
                    params![tid],
                    |_| Ok(true),
                )
                .unwrap_or(false);
            if is_phantom {
                tx.execute(
                    "UPDATE tracking_ids SET type=?1, domain_count=1 WHERE id=?2",
                    params![ttype, tid],
                )?;
            }
            tx.execute(
                "INSERT OR IGNORE INTO tracking_ids(id, type) VALUES(?1, ?2)",
                params![tid, ttype],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO domain_ids(domain, tracking_id) VALUES(?1, ?2)",
                params![r.domain, tid],
            )?;
        }

        // Insert discovered outlinks as new unfetched domains
        for ol in &r.outlinks {
            tx.execute(
                "INSERT OR IGNORE INTO domains(domain) VALUES(?1)",
                params![ol],
            )?;
        }
    }

    update_domain_counts_tx(&tx)?;
    tx.commit()?;
    Ok(new_ids)
}

fn update_domain_counts_tx(tx: &Transaction) -> Result<()> {
    tx.execute_batch(
        "UPDATE tracking_ids SET domain_count = (
            SELECT COUNT(*) FROM domain_ids WHERE domain_ids.tracking_id = tracking_ids.id
        ) WHERE id IN (
            SELECT DISTINCT tracking_id FROM domain_ids
            WHERE domain IN (SELECT domain FROM domains WHERE fetched=1)
        )",
    )?;
    Ok(())
}

/// Get unfetched domains ordered by GNN score (desc), then tracking-ID fanout.
/// If shard/total_shards set, only returns domains where hash(domain) % total == shard.
pub fn get_unfetched(conn: &Connection, limit: usize, shard: Option<usize>, total_shards: usize) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT d.domain FROM domains d
         LEFT JOIN domain_ids di ON d.domain = di.domain
         WHERE d.fetched = 0
         GROUP BY d.domain
         ORDER BY d.gnn_score DESC, COUNT(di.tracking_id) DESC
         LIMIT ?1",
    )?;
    let rows = stmt
        .query_map(params![limit as i64 * total_shards as i64], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Filter by shard if specified
    match shard {
        Some(s) => {
            let filtered: Vec<String> = rows.into_iter()
                .filter(|d| {
                    let hash = d.bytes().fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
                    (hash % total_shards as u64) == s as u64
                })
                .take(limit)
                .collect();
            Ok(filtered)
        }
        None => Ok(rows.into_iter().take(limit).collect()),
    }
}

/// Seed from CC WAT JSON (full_cluster_graph.json).
pub fn seed_cc_wat(conn: &mut Connection, path: &str) -> Result<(usize, usize)> {
    if !Path::new(path).exists() {
        eprintln!("  SKIP {} (not found)", path);
        return Ok((0, 0));
    }
    println!("  Loading {} ...", path);
    let data: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(path)?).context("parse CC WAT JSON")?;

    let tx = conn.transaction()?;
    let mut count_ids = 0usize;
    let mut count_links = 0usize;

    if let Some(clusters) = data.get("clusters").and_then(|v| v.as_object()) {
        for (tid, doms) in clusters {
            if let Some(arr) = doms.as_array() {
                for d in arr {
                    if let Some(dom) = d.as_str() {
                        link_tx(&tx, dom, tid, &id_type(tid))?;
                        count_links += 1;
                    }
                }
                count_ids += 1;
            }
        }
    }

    if let Some(domains) = data.get("domains").and_then(|v| v.as_object()) {
        for (dom, info) in domains {
            if let Some(emails) = info.get("emails").and_then(|v| v.as_array()) {
                let email_strs: Vec<&str> = emails.iter().filter_map(|e| e.as_str()).collect();
                if !email_strs.is_empty() {
                    let ej = serde_json::to_string(&email_strs)?;
                    tx.execute(
                        "UPDATE domains SET emails=?1 WHERE domain=?2",
                        params![ej, dom],
                    )?;
                }
            }
            for key in &["ga", "gtm", "pixel"] {
                if let Some(arr) = info.get(*key).and_then(|v| v.as_array()) {
                    for tid in arr {
                        if let Some(t) = tid.as_str() {
                            link_tx(&tx, dom, t, key)?;
                        }
                    }
                }
            }
        }
    }
    tx.commit()?;
    println!("    CC WAT: {} IDs, {} links", count_ids, count_links);
    Ok((count_ids, count_links))
}

/// Seed from html-fingerprints.csv.
pub fn seed_fingerprints(conn: &mut Connection, path: &str) -> Result<usize> {
    if !Path::new(path).exists() {
        eprintln!("  SKIP {} (not found)", path);
        return Ok(0);
    }
    println!("  Loading {} ...", path);
    let mut count = 0usize;
    let tx = conn.transaction()?;

    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = rdr.headers()?.clone();

    for result in rdr.records() {
        let record = match result {
            Ok(r) => r,
            Err(_) => continue,
        };
        let get = |name: &str| -> String {
            headers
                .iter()
                .position(|h| h == name)
                .and_then(|i| record.get(i))
                .unwrap_or("")
                .trim()
                .to_string()
        };
        let dom = get("domain");
        if dom.is_empty() {
            continue;
        }
        tx.execute(
            "INSERT OR IGNORE INTO domains(domain) VALUES(?1)",
            params![dom],
        )?;

        let mut emails = Vec::new();
        for col in &["schema_email", "html_email"] {
            let v = get(col);
            if !v.is_empty() {
                emails.extend(v.split(';').map(|s| s.trim().to_string()));
            }
        }
        if !emails.is_empty() {
            let ej = serde_json::to_string(&emails)?;
            tx.execute(
                "UPDATE domains SET emails=?1 WHERE domain=?2",
                params![ej, dom],
            )?;
        }

        for col in &["ga_id", "gtm_id", "pixel_id", "adsense_id"] {
            let v = get(col);
            if !v.is_empty() {
                link_tx(&tx, &dom, &v, &id_type(&v))?;
                count += 1;
            }
        }
    }
    tx.commit()?;
    println!("    Fingerprints: {} links", count);
    Ok(count)
}

/// Seed from canada_b2b.db clusters.
pub fn seed_canada_b2b(conn: &mut Connection, path: &str) -> Result<usize> {
    if !Path::new(path).exists() {
        eprintln!("  SKIP {} (not found)", path);
        return Ok(0);
    }
    println!("  Loading {} ...", path);
    let b2b = Connection::open(path)?;
    let mut stmt = b2b.prepare(
        "SELECT cluster_id, ga_id, domains, confirmed_email FROM clusters WHERE ga_id IS NOT NULL",
    )?;

    let tx = conn.transaction()?;
    let mut count = 0usize;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;

    for row in rows {
        let (_cid, ga_id, doms_json, email) = row?;
        let ga_id = match ga_id {
            Some(g) if !g.is_empty() => g,
            _ => continue,
        };
        let doms: Vec<String> = doms_json
            .and_then(|j| serde_json::from_str(&j).ok())
            .unwrap_or_default();

        for d in &doms {
            link_tx(&tx, d, &ga_id, &id_type(&ga_id))?;
            count += 1;
            if let Some(ref em) = email {
                let ej = serde_json::to_string(&vec![em])?;
                tx.execute(
                    "UPDATE domains SET emails=?1 WHERE domain=?2",
                    params![ej, d],
                )?;
            }
        }
    }
    tx.commit()?;
    println!("    B2B clusters: {} links", count);
    Ok(count)
}

/// Finalize seed — update domain_count on all tracking_ids.
pub fn finalize_seed(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "UPDATE tracking_ids SET domain_count = (
            SELECT COUNT(*) FROM domain_ids WHERE domain_ids.tracking_id = tracking_ids.id
        )",
    )?;
    Ok(())
}

/// Expand: cross-ref tracking IDs against CC WAT to discover new domains.
pub fn expand(conn: &mut Connection, cc_wat_path: &str) -> Result<usize> {
    if !Path::new(cc_wat_path).exists() {
        println!("  No CC WAT data available for expansion.");
        return Ok(0);
    }
    println!("  Loading CC WAT for expansion ...");
    let data: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(cc_wat_path)?)?;

    let known_tids: HashSet<String> = {
        let mut stmt = conn.prepare("SELECT DISTINCT tracking_id FROM domain_ids")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.filter_map(|r| r.ok()).collect()
    };
    let mut known_doms: HashSet<String> = {
        let mut stmt = conn.prepare("SELECT domain FROM domains")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.filter_map(|r| r.ok()).collect()
    };

    let tx = conn.transaction()?;
    let mut new_doms = 0usize;

    if let Some(clusters) = data.get("clusters").and_then(|v| v.as_object()) {
        for (tid, doms) in clusters {
            if !known_tids.contains(tid.as_str()) {
                continue;
            }
            if let Some(arr) = doms.as_array() {
                for d in arr {
                    if let Some(dom) = d.as_str() {
                        if !known_doms.contains(dom) {
                            tx.execute(
                                "INSERT OR IGNORE INTO domains(domain) VALUES(?1)",
                                params![dom],
                            )?;
                            known_doms.insert(dom.to_string());
                            new_doms += 1;
                        }
                        link_tx(&tx, dom, tid, &id_type(tid))?;
                    }
                }
            }
        }
    }

    if let Some(domains) = data.get("domains").and_then(|v| v.as_object()) {
        for (dom, info) in domains {
            for key in &["ga", "gtm", "pixel"] {
                if let Some(arr) = info.get(*key).and_then(|v| v.as_array()) {
                    for tid in arr {
                        if let Some(t) = tid.as_str() {
                            if known_tids.contains(t) {
                                if !known_doms.contains(dom.as_str()) {
                                    tx.execute(
                                        "INSERT OR IGNORE INTO domains(domain) VALUES(?1)",
                                        params![dom],
                                    )?;
                                    known_doms.insert(dom.clone());
                                    new_doms += 1;
                                }
                                link_tx(&tx, dom, t, key)?;
                            }
                        }
                    }
                }
            }
        }
    }

    tx.execute_batch(
        "UPDATE tracking_ids SET domain_count = (
            SELECT COUNT(*) FROM domain_ids WHERE domain_ids.tracking_id = tracking_ids.id
        )",
    )?;
    tx.commit()?;
    println!("  Expand: {} new domains discovered", new_doms);
    Ok(new_doms)
}

/// Print graph statistics.
pub fn stats(conn: &Connection) -> Result<()> {
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM domains", [], |r| r.get(0))?;
    let fetched: i64 = conn.query_row("SELECT COUNT(*) FROM domains WHERE fetched=1", [], |r| r.get(0))?;
    let unfetched = total - fetched;
    let total_ids: i64 = conn.query_row("SELECT COUNT(*) FROM tracking_ids", [], |r| r.get(0))?;
    let total_clusters: i64 = conn.query_row("SELECT COUNT(*) FROM clusters", [], |r| r.get(0))?;
    let emails_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM domains WHERE emails IS NOT NULL AND emails != '[]'",
        [],
        |r| r.get(0),
    )?;
    let links: i64 = conn.query_row("SELECT COUNT(*) FROM domain_ids", [], |r| r.get(0))?;
    let fanout = links as f64 / total.max(1) as f64;

    let mut stmt = conn.prepare("SELECT type, COUNT(*) FROM tracking_ids GROUP BY type")?;
    let id_types: Vec<(String, i64)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    println!("\n{}", "=".repeat(50));
    println!("  WEB MAPPER GRAPH STATS");
    println!("{}", "=".repeat(50));
    println!("  Domains:        {} total ({} fetched, {} queued)", total, fetched, unfetched);
    println!("  Tracking IDs:   {}", total_ids);
    for (ttype, cnt) in &id_types {
        println!("    {:12}  {}", ttype, cnt);
    }
    println!("  Links:          {}", links);
    println!("  Clusters:       {}", total_clusters);
    println!("  With emails:    {}", emails_count);
    println!("  Fan-out rate:   {:.2} IDs/domain", fanout);
    println!("  Growth potential: ~{} new domains (est)", unfetched * 3);
    println!("{}\n", "=".repeat(50));
    Ok(())
}

// ── helpers ──

fn link_tx(tx: &Transaction, domain: &str, tid: &str, tid_type: &str) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO tracking_ids(id, type) VALUES(?1, ?2)",
        params![tid, tid_type],
    )?;
    tx.execute(
        "INSERT OR IGNORE INTO domain_ids(domain, tracking_id) VALUES(?1, ?2)",
        params![domain, tid],
    )?;
    tx.execute(
        "INSERT OR IGNORE INTO domains(domain) VALUES(?1)",
        params![domain],
    )?;
    Ok(())
}

/// Classify a tracking ID string into its type.
pub fn id_type(tid: &str) -> String {
    if tid.starts_with("UA-") {
        "ua".into()
    } else if tid.starts_with("G-") {
        "g4".into()
    } else if tid.starts_with("GTM-") {
        "gtm".into()
    } else if tid.starts_with("ca-pub-") {
        "adsense".into()
    } else if tid.starts_with("AW-") {
        "google_ads".into()
    } else if tid.chars().all(|c| c.is_ascii_digit()) && tid.len() >= 10 {
        "pixel".into()
    } else {
        "ua".into()
    }
}
