//! Async HTTP fetcher — 50 concurrent connections, 20KB streaming, connection pooling.

use anyhow::Result;
use rand::seq::SliceRandom;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::extractor;

const MAX_CONCURRENT: usize = 50;
const FETCH_TIMEOUT: Duration = Duration::from_secs(3);
const DELAY_BETWEEN: Duration = Duration::from_millis(200);
const MAX_BYTES: usize = 20_480; // 20KB

static USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (compatible; WebMapper/1.0; +https://example.com/bot)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
];

#[derive(Debug, Clone)]
pub struct FetchResult {
    pub domain: String,
    pub title: Option<String>,
    pub tracking_ids: HashMap<String, String>,
    pub emails: Vec<String>,
    pub phones: Vec<String>,
    pub outlinks: Vec<String>,
    pub fetch_time: String,
    pub success: bool,
}

impl FetchResult {
    fn failed(domain: String) -> Self {
        Self {
            domain,
            title: None,
            tracking_ids: HashMap::new(),
            emails: Vec::new(),
            phones: Vec::new(),
            outlinks: Vec::new(),
            fetch_time: chrono_now(),
            success: false,
        }
    }
}

fn chrono_now() -> String {
    // Simple UTC timestamp without chrono dependency
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    // Good enough ISO-ish format
    format!("{}Z", secs)
}

fn random_ua() -> &'static str {
    let mut rng = rand::thread_rng();
    USER_AGENTS.choose(&mut rng).unwrap_or(&USER_AGENTS[0])
}

/// Build a shared reqwest client with connection pooling.
fn build_client() -> Result<Client> {
    Ok(Client::builder()
        .timeout(FETCH_TIMEOUT)
        .connect_timeout(Duration::from_secs(2))
        .pool_max_idle_per_host(10)
        .redirect(reqwest::redirect::Policy::limited(3))
        .danger_accept_invalid_certs(true)
        .build()?)
}

/// Fetch a single domain — try HTTPS then HTTP, read first 20KB.
async fn fetch_one_inner(client: &Client, domain: &str) -> FetchResult {
    let urls = [
        format!("https://{}", domain),
        format!("http://{}", domain),
    ];

    for url in &urls {
        let ua = random_ua();
        let result = async {
            let resp = client
                .get(url)
                .header("User-Agent", ua)
                .send()
                .await?;

            if !resp.status().is_success() && !resp.status().is_redirection() {
                return Err(anyhow::anyhow!("bad status: {}", resp.status()));
            }

            // Read up to MAX_BYTES via bytes() with content-length hint
            let bytes = resp.bytes().await?;
            let truncated = if bytes.len() > MAX_BYTES {
                &bytes[..MAX_BYTES]
            } else {
                &bytes
            };
            let html = String::from_utf8_lossy(truncated).to_string();
            Ok::<String, anyhow::Error>(html)
        }
        .await;

        match result {
            Ok(html) => {
                let ext = extractor::extract_all(&html, domain);
                return FetchResult {
                    domain: domain.to_string(),
                    title: ext.title,
                    tracking_ids: ext.tracking_ids,
                    emails: ext.emails,
                    phones: ext.phones,
                    outlinks: ext.outlinks,
                    fetch_time: chrono_now(),
                    success: true,
                };
            }
            Err(_) => continue,
        }
    }

    FetchResult::failed(domain.to_string())
}

/// Fetch many domains concurrently (up to 50 at a time).
/// Returns all results (success and failure).
pub async fn fetch_batch(domains: Vec<String>) -> Result<Vec<FetchResult>> {
    let client = build_client()?;
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let client = Arc::new(client);

    let mut handles = Vec::with_capacity(domains.len());

    for (i, domain) in domains.into_iter().enumerate() {
        let sem = semaphore.clone();
        let cli = client.clone();

        let handle = tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            // Stagger requests slightly to avoid thundering herd
            if i > 0 && i % MAX_CONCURRENT == 0 {
                sleep(DELAY_BETWEEN).await;
            }
            fetch_one_inner(&cli, &domain).await
        });
        handles.push(handle);

        // Small delay every batch to be polite
        if i > 0 && i % MAX_CONCURRENT == 0 {
            sleep(DELAY_BETWEEN).await;
        }
    }

    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(r) => results.push(r),
            Err(e) => eprintln!("    task join error: {}", e),
        }
    }

    Ok(results)
}
