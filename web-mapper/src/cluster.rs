//! Union-Find clustering + email propagation across shared tracking IDs.

use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json;
use std::collections::{HashMap, HashSet};

/// Path-compressed union-find with union by rank.
struct UnionFind {
    parent: HashMap<String, String>,
    rank: HashMap<String, usize>,
}

impl UnionFind {
    fn new() -> Self {
        Self {
            parent: HashMap::new(),
            rank: HashMap::new(),
        }
    }

    fn make_set(&mut self, x: &str) {
        if !self.parent.contains_key(x) {
            self.parent.insert(x.to_string(), x.to_string());
            self.rank.insert(x.to_string(), 0);
        }
    }

    fn find(&mut self, x: &str) -> String {
        let p = self.parent.get(x).cloned().unwrap_or_else(|| x.to_string());
        if p == x {
            return x.to_string();
        }
        let root = self.find(&p);
        self.parent.insert(x.to_string(), root.clone());
        root
    }

    fn union(&mut self, a: &str, b: &str) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        let rank_a = *self.rank.get(&ra).unwrap_or(&0);
        let rank_b = *self.rank.get(&rb).unwrap_or(&0);
        if rank_a < rank_b {
            self.parent.insert(ra, rb);
        } else if rank_a > rank_b {
            self.parent.insert(rb, ra);
        } else {
            self.parent.insert(rb, ra.clone());
            self.rank.insert(ra, rank_a + 1);
        }
    }
}

/// Classify the owner signal based on email patterns.
fn classify_owner(emails: &[String]) -> &'static str {
    for e in emails {
        if e.contains("gmail.com") || e.contains("yahoo.com") || e.contains("hotmail.com") {
            return "gmail";
        }
        if e.contains("agency")
            || e.contains("media")
            || e.contains("digital")
            || e.contains("marketing")
        {
            return "agency";
        }
        return "corporate";
    }
    "unknown"
}

/// Build connected components from shared tracking IDs, propagate emails.
pub fn cluster(conn: &mut Connection) -> Result<usize> {
    // Query: tracking IDs shared by multiple domains
    let rows: Vec<(String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT tracking_id, GROUP_CONCAT(domain) FROM domain_ids
             GROUP BY tracking_id HAVING COUNT(*) > 1",
        )?;
        let mapped = stmt
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
        mapped.filter_map(|r| r.ok()).collect()
    };

    let mut uf = UnionFind::new();

    for (_tid, dom_str) in &rows {
        let doms: Vec<&str> = dom_str.split(',').collect();
        for d in &doms {
            uf.make_set(d);
        }
        for i in 1..doms.len() {
            uf.union(doms[0], doms[i]);
        }
    }

    // Collect clusters: root -> list of domains
    let all_domains: Vec<String> = uf.parent.keys().cloned().collect();
    let mut groups: HashMap<String, Vec<String>> = HashMap::new();
    for d in &all_domains {
        let root = uf.find(d);
        groups.entry(root).or_default().push(d.clone());
    }

    // Clear old clusters and rebuild
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM clusters", [])?;

    let mut formed = 0usize;
    let mut total_clustered = 0usize;

    for (anchor, doms) in &groups {
        if doms.len() < 2 {
            continue;
        }

        // Collect all emails from cluster members
        let mut all_emails: HashSet<String> = HashSet::new();
        for d in doms {
            let row: Option<Option<String>> = tx
                .query_row(
                    "SELECT emails FROM domains WHERE domain=?1",
                    params![d],
                    |r| r.get(0),
                )
                .ok();
            if let Some(Some(ej)) = row {
                if let Ok(emails) = serde_json::from_str::<Vec<String>>(&ej) {
                    all_emails.extend(emails);
                }
            }
        }

        let emails_list: Vec<String> = all_emails.into_iter().collect();
        let signal = classify_owner(&emails_list);

        // Find anchor tracking ID
        let anchor_tid: Option<String> = tx
            .query_row(
                "SELECT tracking_id FROM domain_ids WHERE domain=?1 LIMIT 1",
                params![anchor],
                |r| r.get(0),
            )
            .ok();

        let doms_json = serde_json::to_string(doms)?;
        let emails_json = serde_json::to_string(&emails_list)?;

        tx.execute(
            "INSERT INTO clusters(anchor_id, domains, emails, owner_signal, size) VALUES(?1,?2,?3,?4,?5)",
            params![anchor_tid, doms_json, emails_json, signal, doms.len() as i64],
        )?;

        // Propagate emails back to cluster members that lack them
        if !emails_list.is_empty() {
            let ej = serde_json::to_string(&emails_list)?;
            for d in doms {
                tx.execute(
                    "UPDATE domains SET emails=?1 WHERE domain=?2 AND (emails IS NULL OR emails='[]')",
                    params![ej, d],
                )?;
            }
        }

        formed += 1;
        total_clustered += doms.len();
    }

    tx.commit()?;
    println!(
        "  Clustered: {} clusters ({} domains)",
        formed, total_clustered
    );
    Ok(formed)
}
