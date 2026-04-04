//! HTML signal extraction — compiled regex, zero-copy where possible.

use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::{HashMap, HashSet};

// ── Compiled regex patterns (initialized once) ──

static RE_UA: Lazy<Regex> = Lazy::new(|| Regex::new(r"UA-\d{4,10}-\d{1,4}").unwrap());
static RE_G4: Lazy<Regex> = Lazy::new(|| Regex::new(r"G-[A-Z0-9]{8,12}").unwrap());
static RE_GTM: Lazy<Regex> = Lazy::new(|| Regex::new(r"GTM-[A-Z0-9]{4,10}").unwrap());
static RE_PIXEL: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"fbq\s*\(\s*['"]init['"]\s*,\s*['"](\d{10,20})['"]"#).unwrap());
static RE_ADSENSE: Lazy<Regex> = Lazy::new(|| Regex::new(r"ca-pub-\d{10,20}").unwrap());
static RE_ADS: Lazy<Regex> = Lazy::new(|| Regex::new(r"AW-\d{8,12}").unwrap());
static RE_EMAIL: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}").unwrap());
static RE_PHONE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[\+]?1?\s*[\(\-]?\d{3}[\)\-\s.]?\s*\d{3}[\-\s.]?\d{4}").unwrap());
static RE_TITLE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<title[^>]*>(.*?)</title>").unwrap());
static RE_HREF: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"href=["']https?://([^/"':]+)"#).unwrap());

/// Junk domains to filter from outlinks.
static JUNK_DOMAINS: Lazy<Vec<&str>> = Lazy::new(|| {
    vec![
        "google.", "facebook.", "twitter.", "youtube.", "instagram.",
        "linkedin.", "pinterest.", "tiktok.", "apple.com", "microsoft.com",
        "amazonaws.", "cloudflare.", "cdn.", "jquery.", "bootstrap.",
        "fonts.g", "wp.com", "w3.org", "schema.org", "gstatic.",
        "gravatar.", "github.", "reddit.", "whatsapp.", "t.co",
        "bit.ly", "maps.google", "play.google", "apps.apple",
    ]
});

/// Parked-domain indicator phrases.
static PARKED_PHRASES: Lazy<Vec<&str>> = Lazy::new(|| {
    vec![
        "domain is for sale",
        "buy this domain",
        "parked free",
        "this domain has expired",
        "godaddy",
        "hugedomains",
        "sedo.com",
        "afternic",
        "domain parking",
    ]
});

#[derive(Debug, Clone, Default)]
pub struct ExtractionResult {
    /// tracking_id -> type (e.g. "UA-12345-1" -> "ua")
    pub tracking_ids: HashMap<String, String>,
    pub emails: Vec<String>,
    pub phones: Vec<String>,
    pub title: Option<String>,
    pub outlinks: Vec<String>,
    pub is_parked: bool,
}

/// Extract all signals from an HTML fragment (first ~20KB).
pub fn extract_all(html: &str, source_domain: &str) -> ExtractionResult {
    let mut ids = HashMap::new();

    for m in RE_UA.find_iter(html) {
        ids.insert(m.as_str().to_string(), "ua".to_string());
    }
    for m in RE_G4.find_iter(html) {
        ids.insert(m.as_str().to_string(), "g4".to_string());
    }
    for m in RE_GTM.find_iter(html) {
        ids.insert(m.as_str().to_string(), "gtm".to_string());
    }
    for cap in RE_PIXEL.captures_iter(html) {
        if let Some(g) = cap.get(1) {
            ids.insert(g.as_str().to_string(), "pixel".to_string());
        }
    }
    for m in RE_ADSENSE.find_iter(html) {
        ids.insert(m.as_str().to_string(), "adsense".to_string());
    }
    for m in RE_ADS.find_iter(html) {
        ids.insert(m.as_str().to_string(), "google_ads".to_string());
    }

    let emails: Vec<String> = {
        let mut set: HashSet<String> = HashSet::new();
        for m in RE_EMAIL.find_iter(html) {
            let e = m.as_str().to_lowercase();
            if !is_junk_email(&e) {
                set.insert(e);
            }
        }
        set.into_iter().take(20).collect()
    };

    let phones: Vec<String> = {
        let mut set: HashSet<String> = HashSet::new();
        for m in RE_PHONE.find_iter(html) {
            set.insert(m.as_str().trim().to_string());
        }
        set.into_iter().take(10).collect()
    };

    let title = RE_TITLE
        .captures(html)
        .and_then(|c| c.get(1))
        .map(|m| {
            let t = m.as_str().trim();
            let end = t.char_indices().nth(200).map(|(i,_)| i).unwrap_or(t.len());
            &t[..end]
        })
        .map(|s| s.to_string());

    let outlinks = extract_outlinks(html, source_domain);

    let html_lower = html.to_lowercase();
    let is_parked = PARKED_PHRASES.iter().any(|p| html_lower.contains(p));

    ExtractionResult {
        tracking_ids: ids,
        emails,
        phones,
        title,
        outlinks,
        is_parked,
    }
}

/// Check if page looks parked (standalone, used by fetcher to skip).
pub fn is_parked(html: &str) -> bool {
    let lower = html.to_lowercase();
    PARKED_PHRASES.iter().any(|p| lower.contains(p))
}

fn extract_outlinks(html: &str, source_domain: &str) -> Vec<String> {
    let mut links = HashSet::new();
    for cap in RE_HREF.captures_iter(html) {
        if let Some(m) = cap.get(1) {
            let mut d = m.as_str().to_lowercase();
            if d.starts_with("www.") {
                d = d[4..].to_string();
            }
            if d == source_domain || !d.contains('.') {
                continue;
            }
            if JUNK_DOMAINS.iter().any(|j| d.contains(j)) {
                continue;
            }
            links.insert(d);
        }
    }
    links.into_iter().take(50).collect()
}

fn is_junk_email(email: &str) -> bool {
    let junk = [
        "example.com", "sentry.io", "wixpress.com", "wpengine.com",
        "wordpress.com", "gravatar.com", "schema.org",
    ];
    junk.iter().any(|j| email.ends_with(j))
        || email.contains("noreply")
        || email.contains("no-reply")
        || email.starts_with("info@sentry")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_ua() {
        let html = r#"<script>ga('create', 'UA-12345678-1', 'auto');</script>"#;
        let r = extract_all(html, "example.com");
        assert!(r.tracking_ids.contains_key("UA-12345678-1"));
    }

    #[test]
    fn test_extract_email() {
        let html = r#"<a href="mailto:bob@acme.ca">Contact</a>"#;
        let r = extract_all(html, "acme.ca");
        assert!(r.emails.contains(&"bob@acme.ca".to_string()));
    }

    #[test]
    fn test_parked() {
        assert!(is_parked("This domain is for sale at GoDaddy"));
        assert!(!is_parked("<html><body>Welcome to Acme Corp</body></html>"));
    }
}
