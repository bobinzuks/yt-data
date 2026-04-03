//! Inline GNN scorer — 2-layer neural network, pure Rust, no numpy.
//! Architecture: 8 features -> 12 hidden (ReLU) -> 1 output (sigmoid)

use anyhow::Result;
use rand::Rng;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;

const N_FEATURES: usize = 8;
const N_HIDDEN: usize = 12;
const LEARNING_RATE: f64 = 0.01;
const MAX_TRAIN_SAMPLES: usize = 2000;

#[derive(Debug, Serialize, Deserialize)]
pub struct GnnWeights {
    pub w1: Vec<Vec<f64>>,  // [N_HIDDEN][N_FEATURES]
    pub b1: Vec<f64>,       // [N_HIDDEN]
    pub w2: Vec<f64>,       // [N_HIDDEN]
    pub b2: f64,
    pub accuracy: f64,
    pub samples: usize,
}

impl GnnWeights {
    /// Initialize with random weights (Xavier-ish).
    pub fn random() -> Self {
        let mut rng = rand::thread_rng();
        let scale = (2.0 / N_FEATURES as f64).sqrt();
        Self {
            w1: (0..N_HIDDEN)
                .map(|_| (0..N_FEATURES).map(|_| rng.gen_range(-scale..scale)).collect())
                .collect(),
            b1: vec![0.0; N_HIDDEN],
            w2: (0..N_HIDDEN).map(|_| rng.gen_range(-scale..scale)).collect(),
            b2: 0.0,
            accuracy: 0.0,
            samples: 0,
        }
    }

    /// Load from JSON file, or return fresh random weights.
    pub fn load(path: &str) -> Self {
        if let Ok(data) = std::fs::read_to_string(path) {
            if let Ok(w) = serde_json::from_str(&data) {
                return w;
            }
        }
        Self::random()
    }

    /// Save to JSON file.
    pub fn save(&self, path: &str) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, json)?;
        Ok(())
    }
}

fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x.clamp(-20.0, 20.0)).exp())
}

fn relu(x: f64) -> f64 {
    x.max(0.0)
}

/// Forward pass. Returns (output, hidden_activations).
fn forward(w: &GnnWeights, x: &[f64]) -> (f64, Vec<f64>) {
    let h: Vec<f64> = (0..N_HIDDEN)
        .map(|j| {
            let sum: f64 = (0..N_FEATURES).map(|k| w.w1[j][k] * x[k]).sum::<f64>() + w.b1[j];
            relu(sum)
        })
        .collect();

    let out: f64 = (0..N_HIDDEN).map(|j| w.w2[j] * h[j]).sum::<f64>() + w.b2;
    (sigmoid(out), h)
}

/// Predict email probability for a feature vector.
pub fn predict(w: &GnnWeights, features: &[f64]) -> f64 {
    forward(w, features).0
}

/// Extract 8-feature vector for a domain from the DB.
pub fn domain_features(conn: &Connection, domain: &str) -> Vec<f64> {
    let row: Option<(Option<String>, Option<String>, Option<String>, Option<bool>)> = conn
        .query_row(
            "SELECT title, emails, phones, is_business FROM domains WHERE domain=?1",
            params![domain],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .ok();

    let id_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM domain_ids WHERE domain=?1",
            params![domain],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let d = domain.to_lowercase();
    let title_len = row
        .as_ref()
        .and_then(|r| r.0.as_ref())
        .map(|t| t.len())
        .unwrap_or(0);

    let has_email = row
        .as_ref()
        .and_then(|r| r.1.as_ref())
        .map(|e| !e.is_empty() && e != "[]" && e != "null")
        .unwrap_or(false);

    let is_biz = row.as_ref().and_then(|r| r.3).unwrap_or(false);

    vec![
        (d.len().min(40) as f64) / 40.0,                  // domain length
        (d.matches('.').count() as f64) / 5.0,             // dot count
        if d.ends_with(".ca") { 1.0 } else { 0.0 },       // is .ca
        if d.ends_with(".com") { 1.0 } else { 0.0 },      // is .com
        (id_count.min(10) as f64) / 10.0,                  // tracking ID count
        if is_biz { 1.0 } else { 0.0 },                   // is_business flag
        (title_len.min(100) as f64) / 100.0,               // title length
        if has_email { 1.0 } else { 0.0 },                 // has email already
    ]
}

/// Train on fetched domains with known outcomes. Returns accuracy.
pub fn train(conn: &Connection, weights_path: &str) -> Result<Option<f64>> {
    let mut w = GnnWeights::load(weights_path);

    // Collect training data: fetched domains with known email outcome
    let mut stmt = conn.prepare(
        "SELECT domain, CASE WHEN emails IS NOT NULL AND emails != '[]' AND emails != ''
         THEN 1 ELSE 0 END as has_email
         FROM domains WHERE fetched=1",
    )?;
    let mut samples: Vec<(String, f64)> = stmt
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    if samples.len() < 100 {
        println!(
            "  GNN: need 100+ samples, have {}. Skipping.",
            samples.len()
        );
        return Ok(None);
    }

    // Shuffle and cap at MAX_TRAIN_SAMPLES
    use rand::seq::SliceRandom;
    let mut rng = rand::thread_rng();
    samples.shuffle(&mut rng);
    let n = samples.len().min(MAX_TRAIN_SAMPLES);
    let mut correct = 0usize;

    for (domain, target) in &samples[..n] {
        let x = domain_features(conn, domain);
        let (pred, h) = forward(&w, &x);
        let error = pred - target;

        // Backprop through output layer
        for j in 0..N_HIDDEN {
            w.w2[j] -= LEARNING_RATE * error * h[j];

            // Backprop through hidden layer (ReLU derivative)
            let pre_act: f64 =
                (0..N_FEATURES).map(|k| w.w1[j][k] * x[k]).sum::<f64>() + w.b1[j];
            if pre_act > 0.0 {
                for k in 0..N_FEATURES {
                    w.w1[j][k] -= LEARNING_RATE * error * w.w2[j] * x[k];
                }
                w.b1[j] -= LEARNING_RATE * error * w.w2[j];
            }
        }
        w.b2 -= LEARNING_RATE * error;

        if (pred > 0.5) == (*target > 0.5) {
            correct += 1;
        }
    }

    let acc = correct as f64 / n as f64;
    w.accuracy = acc;
    w.samples = samples.len();
    w.save(weights_path)?;

    println!(
        "  GNN: trained on {} samples, accuracy={:.1}%, scored unfetched domains",
        n,
        acc * 100.0
    );
    Ok(Some(acc))
}

/// Score all unfetched domains and write gnn_score to DB.
pub fn score_unfetched(conn: &mut Connection, weights_path: &str) -> Result<usize> {
    let w = GnnWeights::load(weights_path);
    if w.samples == 0 {
        return Ok(0);
    }

    let domains: Vec<String> = {
        let mut stmt = conn.prepare("SELECT domain FROM domains WHERE fetched=0")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.filter_map(|r| r.ok()).collect()
    };

    let tx = conn.transaction()?;
    let mut scored = 0usize;

    for domain in &domains {
        let x = domain_features(&tx, domain);
        let pred = predict(&w, &x);
        tx.execute(
            "UPDATE domains SET gnn_score=?1, is_business=?2 WHERE domain=?3",
            params![pred, pred > 0.4, domain],
        )?;
        scored += 1;
    }
    tx.commit()?;
    Ok(scored)
}
