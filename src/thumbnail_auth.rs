//! Service credential for the public `/thumbnail/<hash>` route.
//!
//! divine-blossom decides who may see a thumbnail (moderation status, owner,
//! age gate, admin) before it asks this service to read or generate one, and
//! then calls `/thumbnail/<hash>` on the public upload host. Nothing here can
//! read that moderation state, so a direct caller who knows a banned video's
//! hash could fetch its frame. A shared token that only Blossom holds closes
//! that path without duplicating Blossom's access rules (divine-upload-server#52).
//!
//! Rollout: deploy with no token (unchanged), then with the token and
//! `THUMBNAIL_TOKEN_REQUIRED=false` (accept everything, count what arrives
//! without it), then flip the flag once Blossom sends it everywhere.

use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::atomic::{AtomicU64, Ordering};

/// Header divine-blossom sets on its `/thumbnail/<hash>` calls.
pub const HEADER: &str = "x-divine-thumbnail-token";
const TOKEN_ENV: &str = "THUMBNAIL_SERVICE_TOKEN";
const REQUIRED_ENV: &str = "THUMBNAIL_TOKEN_REQUIRED";

#[derive(Clone, PartialEq, Eq)]
pub enum Policy {
    /// No token configured: every request is served, as before this module.
    Open,
    /// Token configured, not yet required: serve everything, count requests
    /// that lack a valid token so the flip can wait for that count to reach zero.
    Observe(String),
    /// Token required: requests without a valid token get 404.
    Required(String),
}

// The token must never reach a log line, so `Debug` names the mode only.
impl std::fmt::Debug for Policy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.mode())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    Allow,
    /// Served only because the token is not required yet.
    AllowUnauthenticated,
    Reject,
}

static UNAUTHENTICATED: AtomicU64 = AtomicU64::new(0);
static REJECTED: AtomicU64 = AtomicU64::new(0);

impl Policy {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let token = match lookup(TOKEN_ENV) {
            None => None,
            Some(raw) => {
                let token = raw.trim().to_string();
                if token.is_empty() {
                    anyhow::bail!("{TOKEN_ENV} is empty");
                }
                if axum::http::HeaderValue::from_str(&token).is_err() {
                    anyhow::bail!("{TOKEN_ENV} is not a valid header value");
                }
                Some(token)
            }
        };
        let required = match lookup(REQUIRED_ENV).as_deref().map(str::trim) {
            None | Some("") | Some("false") => false,
            Some("true") => true,
            Some(_) => anyhow::bail!("{REQUIRED_ENV} must be true or false"),
        };
        Ok(match (token, required) {
            (None, false) => Policy::Open,
            (None, true) => anyhow::bail!("{REQUIRED_ENV}=true requires {TOKEN_ENV}"),
            (Some(token), false) => Policy::Observe(token),
            (Some(token), true) => Policy::Required(token),
        })
    }

    pub fn mode(&self) -> &'static str {
        match self {
            Policy::Open => "open",
            Policy::Observe(_) => "observe",
            Policy::Required(_) => "required",
        }
    }

    pub fn decide(&self, headers: &HeaderMap) -> Decision {
        let expected = match self {
            Policy::Open => return Decision::Allow,
            Policy::Observe(token) | Policy::Required(token) => token,
        };
        if presented_matches(headers, expected) {
            return Decision::Allow;
        }
        match self {
            Policy::Required(_) => {
                REJECTED.fetch_add(1, Ordering::Relaxed);
                Decision::Reject
            }
            _ => {
                UNAUTHENTICATED.fetch_add(1, Ordering::Relaxed);
                Decision::AllowUnauthenticated
            }
        }
    }

    /// One startup line describing the mode. Never includes the token.
    pub fn startup_message(&self) -> &'static str {
        match self {
            Policy::Open => "/thumbnail is unauthenticated: THUMBNAIL_SERVICE_TOKEN is not set",
            Policy::Observe(_) => {
                "/thumbnail accepts requests without the service token and counts them \
                 (THUMBNAIL_TOKEN_REQUIRED=false)"
            }
            Policy::Required(_) => "/thumbnail requires the service token",
        }
    }
}

fn presented_matches(headers: &HeaderMap, expected: &str) -> bool {
    let Some(candidate) = headers.get(HEADER).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    // Compare fixed-size MACs rather than the strings, so neither the content
    // nor the length of the configured token shapes the comparison time.
    let mac = |value: &str| {
        let mut mac = Hmac::<Sha256>::new_from_slice(value.as_bytes())
            .expect("HMAC accepts keys of any length");
        mac.update(b"divine-thumbnail-service-token-v1");
        mac
    };
    mac(expected)
        .verify_slice(&mac(candidate).finalize().into_bytes())
        .is_ok()
}

pub fn metrics() -> String {
    format!(
        "# HELP divine_thumbnail_unauthenticated_total Thumbnail requests served without \
         a valid service token while the token is not required.\n\
         # TYPE divine_thumbnail_unauthenticated_total counter\n\
         divine_thumbnail_unauthenticated_total {}\n\
         # HELP divine_thumbnail_rejected_total Thumbnail requests refused for a missing \
         or invalid service token.\n\
         # TYPE divine_thumbnail_rejected_total counter\n\
         divine_thumbnail_rejected_total {}\n",
        UNAUTHENTICATED.load(Ordering::Relaxed),
        REJECTED.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
pub fn counts() -> (u64, u64) {
    (
        UNAUTHENTICATED.load(Ordering::Relaxed),
        REJECTED.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn policy(pairs: &[(&str, &str)]) -> anyhow::Result<Policy> {
        let env: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Policy::from_lookup(|key| env.get(key).cloned())
    }

    fn headers(token: Option<&str>) -> HeaderMap {
        let mut map = HeaderMap::new();
        if let Some(token) = token {
            map.insert(HEADER, token.parse().unwrap());
        }
        map
    }

    #[test]
    fn no_token_leaves_the_route_open() {
        assert_eq!(policy(&[]).unwrap(), Policy::Open);
        assert_eq!(policy(&[(REQUIRED_ENV, "false")]).unwrap(), Policy::Open);
    }

    #[test]
    fn token_without_the_flag_observes() {
        assert_eq!(
            policy(&[(TOKEN_ENV, " s3cret ")]).unwrap(),
            Policy::Observe("s3cret".into())
        );
    }

    #[test]
    fn token_with_the_flag_requires() {
        assert_eq!(
            policy(&[(TOKEN_ENV, "s3cret"), (REQUIRED_ENV, "true")]).unwrap(),
            Policy::Required("s3cret".into())
        );
    }

    #[test]
    fn requiring_without_a_token_refuses_to_start() {
        assert!(policy(&[(REQUIRED_ENV, "true")]).is_err());
    }

    #[test]
    fn malformed_settings_refuse_to_start() {
        assert!(policy(&[(TOKEN_ENV, "   ")]).is_err());
        assert!(policy(&[(TOKEN_ENV, "bad\u{7f}token")]).is_err());
        assert!(policy(&[(TOKEN_ENV, "s3cret"), (REQUIRED_ENV, "yes")]).is_err());
    }

    #[test]
    fn open_allows_everything() {
        assert_eq!(Policy::Open.decide(&headers(None)), Decision::Allow);
        assert_eq!(Policy::Open.decide(&headers(Some("x"))), Decision::Allow);
    }

    #[test]
    fn observe_allows_and_flags_missing_or_wrong_tokens() {
        let p = Policy::Observe("s3cret".into());
        assert_eq!(p.decide(&headers(Some("s3cret"))), Decision::Allow);
        assert_eq!(p.decide(&headers(None)), Decision::AllowUnauthenticated);
        assert_eq!(
            p.decide(&headers(Some("s3cre"))),
            Decision::AllowUnauthenticated
        );
    }

    #[test]
    fn required_rejects_missing_wrong_and_prefixed_tokens() {
        let p = Policy::Required("s3cret".into());
        assert_eq!(p.decide(&headers(Some("s3cret"))), Decision::Allow);
        assert_eq!(p.decide(&headers(None)), Decision::Reject);
        assert_eq!(p.decide(&headers(Some("s3cret2"))), Decision::Reject);
        assert_eq!(p.decide(&headers(Some("Bearer s3cret"))), Decision::Reject);
        assert_eq!(p.decide(&headers(Some(""))), Decision::Reject);
    }

    #[test]
    fn the_token_never_appears_in_logs_or_metrics() {
        let p = Policy::Required("s3cret-value".into());
        assert!(!format!("{p:?}").contains("s3cret"));
        assert!(!p.startup_message().contains("s3cret"));
        assert!(!metrics().contains("s3cret"));
    }

    #[test]
    fn counters_track_unauthenticated_and_rejected_requests() {
        let (unauth, rejected) = counts();
        Policy::Observe("t".into()).decide(&headers(None));
        Policy::Required("t".into()).decide(&headers(None));
        let (unauth2, rejected2) = counts();
        assert!(unauth2 > unauth);
        assert!(rejected2 > rejected);
        let body = metrics();
        assert!(body.contains("# TYPE divine_thumbnail_unauthenticated_total counter\n"));
        assert!(body.contains("# TYPE divine_thumbnail_rejected_total counter\n"));
    }
}
