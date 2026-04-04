//! web-mapper — Chain-reaction web mapper in Rust. 50 concurrent fetchers, compiled regex, GNN scoring.
//!
//! Usage:
//!   web-mapper --seed                  # init from existing data sources
//!   web-mapper --run 5000 --cycles 3   # full cycles
//!   web-mapper --fetch 2000            # just fetch
//!   web-mapper --cluster               # just cluster
//!   web-mapper --stats                 # show graph stats
//!   web-mapper --gnn-train             # train GNN on fetched data

mod cluster;
mod db;
mod extractor;
mod fetcher;
mod gnn;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

/// Chain-reaction web mapper — fans out from seed tracking IDs to map the web.
#[derive(Parser, Debug)]
#[command(name = "web-mapper", version, about)]
struct Cli {
    /// Initialize from existing data sources (CC WAT, fingerprints, canada_b2b)
    #[arg(long)]
    seed: bool,

    /// Fetch N unfetched domains
    #[arg(long, value_name = "N")]
    fetch: Option<usize>,

    /// Expand graph via CC WAT cross-reference
    #[arg(long)]
    expand: bool,

    /// Build clusters from shared tracking IDs
    #[arg(long)]
    cluster: bool,

    /// Full cycle: fetch N domains per cycle
    #[arg(long, value_name = "N")]
    run: Option<usize>,

    /// Number of full cycles (used with --run)
    #[arg(long, default_value_t = 1)]
    cycles: usize,

    /// Show graph statistics
    #[arg(long)]
    stats: bool,

    /// Train GNN on fetched data
    #[arg(long)]
    gnn_train: bool,

    /// Path to the database file
    #[arg(long, default_value = "data/web_map.db")]
    db: String,

    /// Path to the email-velocity data directory (for seed sources)
    #[arg(long)]
    data_dir: Option<String>,

    /// Shard index for parallel Actions runs (0-based)
    #[arg(long)]
    shard: Option<usize>,

    /// Total number of shards
    #[arg(long, default_value_t = 1)]
    total_shards: usize,

    /// Export shard results to JSON file
    #[arg(long)]
    export_shard: Option<String>,

    /// Merge shard result files into DB
    #[arg(long)]
    merge: Option<String>,

    /// Propagate emails across clusters
    #[arg(long)]
    propagate_emails: bool,
}

impl Cli {
    fn has_any_command(&self) -> bool {
        self.seed
            || self.merge.is_some()
            || self.propagate_emails
            || self.fetch.is_some()
            || self.expand
            || self.cluster
            || self.run.is_some()
            || self.stats
            || self.gnn_train
    }
}

/// Resolve data paths relative to data_dir or defaults.
struct Paths {
    db: String,
    cc_wat: String,
    fingerprints: String,
    canada_b2b: String,
    gnn_weights: String,
}

impl Paths {
    fn new(cli: &Cli) -> Self {
        let data_dir = cli.data_dir.clone().unwrap_or_else(|| {
            // Try to find the email-velocity data dir relative to CWD or binary
            let candidates = [
                PathBuf::from("../email-velocity/data"),
                PathBuf::from("data"),
            ];
            for c in &candidates {
                if c.exists() {
                    return c.to_string_lossy().to_string();
                }
            }
            "data".to_string()
        });

        Self {
            db: cli.db.clone(),
            cc_wat: "/tmp/yt-data-results/results/full_cluster_graph.json".to_string(),
            fingerprints: format!("{}/html-fingerprints.csv", data_dir),
            canada_b2b: format!("{}/canada_b2b.db", data_dir),
            gnn_weights: format!("{}/mapper_gnn_weights.json", data_dir),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if !cli.has_any_command() {
        eprintln!("No command specified. Use --help for usage.");
        std::process::exit(1);
    }

    let paths = Paths::new(&cli);
    let mut conn = db::open(&paths.db)?;

    if cli.seed {
        do_seed(&mut conn, &paths)?;
    }
    if let Some(limit) = cli.fetch {
        do_fetch(&mut conn, limit, cli.shard, cli.total_shards).await?;
    }
    if cli.expand {
        db::expand(&mut conn, &paths.cc_wat)?;
    }
    if cli.cluster {
        cluster::cluster(&mut conn)?;
    }
    if cli.gnn_train {
        do_gnn(&mut conn, &paths)?;
    }
    if let Some(fetch_limit) = cli.run {
        run_cycle(&mut conn, &paths, fetch_limit, cli.cycles, cli.shard, cli.total_shards).await?;
    }
    if cli.stats {
        db::stats(&conn)?;
    }

    Ok(())
}

fn do_seed(conn: &mut rusqlite::Connection, paths: &Paths) -> Result<()> {
    println!("[SEED]");
    db::seed_cc_wat(conn, &paths.cc_wat)?;
    db::seed_fingerprints(conn, &paths.fingerprints)?;
    db::seed_canada_b2b(conn, &paths.canada_b2b)?;
    db::finalize_seed(conn)?;

    let total_ids: i64 =
        conn.query_row("SELECT COUNT(*) FROM tracking_ids", [], |r| r.get(0))?;
    let total_doms: i64 =
        conn.query_row("SELECT COUNT(*) FROM domains", [], |r| r.get(0))?;
    println!("  Seed complete: {} tracking IDs, {} domains", total_ids, total_doms);
    Ok(())
}

async fn do_fetch(conn: &mut rusqlite::Connection, limit: usize, shard: Option<usize>, total_shards: usize) -> Result<usize> {
    let domains = db::get_unfetched(conn, limit, shard, total_shards)?;
    if domains.is_empty() {
        println!("  No unfetched domains.");
        return Ok(0);
    }
    let total = domains.len();
    println!("  Fetching {} domains (50 concurrent) ...", total);

    let batch_size = 500;
    let mut total_new_ids = 0usize;
    let mut total_done = 0usize;

    for chunk in domains.chunks(batch_size) {
        let results = fetcher::fetch_batch(chunk.to_vec()).await?;
        let success_count = results.iter().filter(|r| r.success).count();
        let new_ids = db::flush_batch(conn, &results)?;
        total_new_ids += new_ids;
        total_done += chunk.len();
        println!(
            "    {}/{} fetched ({} OK), {} new IDs",
            total_done, total, success_count, new_ids
        );
    }

    let queued: i64 =
        conn.query_row("SELECT COUNT(*) FROM domains WHERE fetched=0", [], |r| r.get(0))?;
    println!(
        "  Fetch done: {} domains, {} new tracking IDs, {} queued",
        total_done, total_new_ids, queued
    );
    Ok(total_new_ids)
}

fn do_gnn(conn: &mut rusqlite::Connection, paths: &Paths) -> Result<()> {
    gnn::train(conn, &paths.gnn_weights)?;
    let scored = gnn::score_unfetched(conn, &paths.gnn_weights)?;
    if scored > 0 {
        println!("  GNN: scored {} unfetched domains for priority", scored);
    }
    Ok(())
}

async fn run_cycle(
    conn: &mut rusqlite::Connection,
    paths: &Paths,
    fetch_limit: usize,
    cycles: usize,
    shard: Option<usize>,
    total_shards: usize,
) -> Result<()> {
    // Auto-seed if empty
    let total_ids: i64 =
        conn.query_row("SELECT COUNT(*) FROM tracking_ids", [], |r| r.get(0))?;
    if total_ids == 0 {
        do_seed(conn, paths)?;
    }

    for i in 0..cycles {
        println!("\n[CYCLE {}/{}]", i + 1, cycles);
        let before: i64 =
            conn.query_row("SELECT COUNT(*) FROM domains", [], |r| r.get(0))?;
        let before_emails: i64 = conn.query_row(
            "SELECT COUNT(*) FROM domains WHERE emails IS NOT NULL AND emails != '[]'",
            [],
            |r| r.get(0),
        )?;

        println!("[FETCH]");
        let new_ids = do_fetch(conn, fetch_limit, shard, total_shards).await?;

        println!("[EXPAND]");
        let new_doms = db::expand(conn, &paths.cc_wat)?;

        println!("[CLUSTER]");
        let formed = cluster::cluster(conn)?;

        println!("[GNN TRAIN]");
        do_gnn(conn, paths)?;

        let after: i64 =
            conn.query_row("SELECT COUNT(*) FROM domains", [], |r| r.get(0))?;
        let after_emails: i64 = conn.query_row(
            "SELECT COUNT(*) FROM domains WHERE emails IS NOT NULL AND emails != '[]'",
            [],
            |r| r.get(0),
        )?;

        println!("\n  Cycle {} results:", i + 1);
        println!("    Domains: {} -> {} (+{})", before, after, after - before);
        println!(
            "    Emails: {} -> {} (+{})",
            before_emails,
            after_emails,
            after_emails - before_emails
        );
        println!("    New IDs: {}", new_ids);
        println!("    New domains from expand: {}", new_doms);
        println!("    Clusters: {}", formed);
    }

    db::stats(conn)?;
    Ok(())
}
