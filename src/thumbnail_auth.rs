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

/// Why a request didn't authenticate. Split so the flip can tell a caller
/// that never sends the token (`missing`) from a mismatched copy of the secret
/// (`invalid`), which must be zero before the token is required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Missing,
    Invalid,
}

impl Reason {
    pub fn label(self) -> &'static str {
        match self {
            Reason::Missing => "missing",
            Reason::Invalid => "invalid",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    Allow,
    /// Served only because the token is not required yet.
    AllowUnauthenticated(Reason),
    Reject(Reason),
}

static AUTHENTICATED: AtomicU64 = AtomicU64::new(0);
static UNAUTHENTICATED_MISSING: AtomicU64 = AtomicU64::new(0);
static UNAUTHENTICATED_INVALID: AtomicU64 = AtomicU64::new(0);
static REJECTED_MISSING: AtomicU64 = AtomicU64::new(0);
static REJECTED_INVALID: AtomicU64 = AtomicU64::new(0);

impl Policy {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_var_lookup(|key| std::env::var(key))
    }

    #[cfg(test)]
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        Self::from_values(lookup(TOKEN_ENV), lookup(REQUIRED_ENV))
    }

    fn from_var_lookup(
        lookup: impl Fn(&str) -> Result<String, std::env::VarError>,
    ) -> anyhow::Result<Self> {
        let read = |key: &str| match lookup(key) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                anyhow::bail!("{key} must be valid UTF-8")
            }
        };
        Self::from_values(read(TOKEN_ENV)?, read(REQUIRED_ENV)?)
    }

    fn from_values(token: Option<String>, required: Option<String>) -> anyhow::Result<Self> {
        let token = match token {
            None => None,
            Some(raw) => {
                let token = raw.trim().to_string();
                if token.is_empty() {
                    anyhow::bail!("{TOKEN_ENV} is empty");
                }
                // Visible ASCII only: anything else either can't travel in a
                // header or can never match the header text a request carries.
                if !token.bytes().all(|b| b.is_ascii_graphic()) {
                    anyhow::bail!("{TOKEN_ENV} must be visible ASCII with no spaces");
                }
                Some(token)
            }
        };
        let required = match required.as_deref().map(str::trim) {
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
        let mut values = headers.get_all(HEADER).iter();
        let reason = match (values.next(), values.next()) {
            (None, None) => Reason::Missing,
            (Some(value), None) if presented_matches(value, expected) => {
                AUTHENTICATED.fetch_add(1, Ordering::Relaxed);
                return Decision::Allow;
            }
            _ => Reason::Invalid,
        };
        let required = matches!(self, Policy::Required(_));
        let counter = match (required, reason) {
            (true, Reason::Missing) => &REJECTED_MISSING,
            (true, Reason::Invalid) => &REJECTED_INVALID,
            (false, Reason::Missing) => &UNAUTHENTICATED_MISSING,
            (false, Reason::Invalid) => &UNAUTHENTICATED_INVALID,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if required {
            Decision::Reject(reason)
        } else {
            Decision::AllowUnauthenticated(reason)
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

fn presented_matches(value: &axum::http::HeaderValue, expected: &str) -> bool {
    let Ok(candidate) = value.to_str() else {
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
    let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
    format!(
        "# HELP divine_thumbnail_authenticated_total Thumbnail requests carrying a valid \
         service token.\n\
         # TYPE divine_thumbnail_authenticated_total counter\n\
         divine_thumbnail_authenticated_total {}\n\
         # HELP divine_thumbnail_unauthenticated_total Thumbnail requests served without \
         a valid service token while the token is not required.\n\
         # TYPE divine_thumbnail_unauthenticated_total counter\n\
         divine_thumbnail_unauthenticated_total{{reason=\"missing\"}} {}\n\
         divine_thumbnail_unauthenticated_total{{reason=\"invalid\"}} {}\n\
         # HELP divine_thumbnail_rejected_total Thumbnail requests refused for a missing \
         or invalid service token.\n\
         # TYPE divine_thumbnail_rejected_total counter\n\
         divine_thumbnail_rejected_total{{reason=\"missing\"}} {}\n\
         divine_thumbnail_rejected_total{{reason=\"invalid\"}} {}\n",
        load(&AUTHENTICATED),
        load(&UNAUTHENTICATED_MISSING),
        load(&UNAUTHENTICATED_INVALID),
        load(&REJECTED_MISSING),
        load(&REJECTED_INVALID),
    )
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub struct Counts {
    pub authenticated: u64,
    pub unauthenticated_missing: u64,
    pub unauthenticated_invalid: u64,
    pub rejected_missing: u64,
    pub rejected_invalid: u64,
}

#[cfg(test)]
pub fn counts() -> Counts {
    let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
    Counts {
        authenticated: load(&AUTHENTICATED),
        unauthenticated_missing: load(&UNAUTHENTICATED_MISSING),
        unauthenticated_invalid: load(&UNAUTHENTICATED_INVALID),
        rejected_missing: load(&REJECTED_MISSING),
        rejected_invalid: load(&REJECTED_INVALID),
    }
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
        // Header values may carry bytes a request can never match as text, so
        // only visible ASCII is accepted.
        assert!(policy(&[(TOKEN_ENV, "t\u{f6}ken")]).is_err());
        assert!(policy(&[(TOKEN_ENV, "two words")]).is_err());
        assert!(policy(&[(TOKEN_ENV, "s3cret"), (REQUIRED_ENV, "yes")]).is_err());
    }

    #[test]
    fn open_allows_everything() {
        assert_eq!(Policy::Open.decide(&headers(None)), Decision::Allow);
        assert_eq!(Policy::Open.decide(&headers(Some("x"))), Decision::Allow);
    }

    #[test]
    fn observe_allows_and_says_why_a_token_was_missing_or_wrong() {
        let p = Policy::Observe("s3cret".into());
        assert_eq!(p.decide(&headers(Some("s3cret"))), Decision::Allow);
        assert_eq!(
            p.decide(&headers(None)),
            Decision::AllowUnauthenticated(Reason::Missing)
        );
        assert_eq!(
            p.decide(&headers(Some("s3cre"))),
            Decision::AllowUnauthenticated(Reason::Invalid)
        );
    }

    #[test]
    fn required_rejects_missing_wrong_and_prefixed_tokens() {
        let p = Policy::Required("s3cret".into());
        assert_eq!(p.decide(&headers(Some("s3cret"))), Decision::Allow);
        assert_eq!(p.decide(&headers(None)), Decision::Reject(Reason::Missing));
        for wrong in ["s3cret2", "Bearer s3cret", ""] {
            assert_eq!(
                p.decide(&headers(Some(wrong))),
                Decision::Reject(Reason::Invalid),
                "{wrong:?}"
            );
        }
    }

    #[test]
    fn duplicate_token_headers_are_rejected_as_invalid() {
        let mut mixed = headers(Some("s3cret"));
        mixed.append(HEADER, "wrong".parse().unwrap());
        assert_eq!(
            Policy::Required("s3cret".into()).decide(&mixed),
            Decision::Reject(Reason::Invalid)
        );
        assert_eq!(
            Policy::Observe("s3cret".into()).decide(&mixed),
            Decision::AllowUnauthenticated(Reason::Invalid)
        );

        let mut repeated = headers(Some("s3cret"));
        repeated.append(HEADER, "s3cret".parse().unwrap());
        assert_eq!(
            Policy::Required("s3cret".into()).decide(&repeated),
            Decision::Reject(Reason::Invalid)
        );
        assert_eq!(
            Policy::Observe("s3cret".into()).decide(&repeated),
            Decision::AllowUnauthenticated(Reason::Invalid)
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_env_vars_refuse_to_start() {
        use std::os::unix::ffi::OsStringExt;
        let bad_token = std::ffi::OsString::from_vec(vec![b's', b'3', b'c', 0xff]);
        let err = Policy::from_var_lookup(|key| match key {
            TOKEN_ENV => Err(std::env::VarError::NotUnicode(bad_token.clone())),
            _ => Err(std::env::VarError::NotPresent),
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(TOKEN_ENV), "{msg}");
        assert!(!msg.contains("s3c"), "{msg}");

        let bad_flag = std::ffi::OsString::from_vec(vec![b'f', b'l', b'g', 0xff]);
        let err = Policy::from_var_lookup(|key| match key {
            TOKEN_ENV => Ok("s3cret".into()),
            REQUIRED_ENV => Err(std::env::VarError::NotUnicode(bad_flag.clone())),
            _ => Err(std::env::VarError::NotPresent),
        })
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(REQUIRED_ENV), "{msg}");
        assert!(!msg.contains("flg"), "{msg}");
    }

    #[test]
    fn the_token_never_appears_in_logs_or_metrics() {
        let p = Policy::Required("s3cret-value".into());
        assert!(!format!("{p:?}").contains("s3cret"));
        assert!(!p.startup_message().contains("s3cret"));
        assert!(!metrics().contains("s3cret"));
    }

    #[test]
    fn counters_separate_authenticated_missing_and_invalid_requests() {
        let before = counts();
        Policy::Observe("t".into()).decide(&headers(Some("t")));
        Policy::Observe("t".into()).decide(&headers(None));
        Policy::Observe("t".into()).decide(&headers(Some("x")));
        Policy::Required("t".into()).decide(&headers(None));
        Policy::Required("t".into()).decide(&headers(Some("x")));
        Policy::Open.decide(&headers(None));
        let after = counts();
        assert!(after.authenticated > before.authenticated);
        assert!(after.unauthenticated_missing > before.unauthenticated_missing);
        assert!(after.unauthenticated_invalid > before.unauthenticated_invalid);
        assert!(after.rejected_missing > before.rejected_missing);
        assert!(after.rejected_invalid > before.rejected_invalid);
        let body = metrics();
        for line in [
            "# TYPE divine_thumbnail_authenticated_total counter\n",
            "# TYPE divine_thumbnail_unauthenticated_total counter\n",
            "# TYPE divine_thumbnail_rejected_total counter\n",
            "\ndivine_thumbnail_authenticated_total ",
            "divine_thumbnail_unauthenticated_total{reason=\"missing\"} ",
            "divine_thumbnail_unauthenticated_total{reason=\"invalid\"} ",
            "divine_thumbnail_rejected_total{reason=\"missing\"} ",
            "divine_thumbnail_rejected_total{reason=\"invalid\"} ",
        ] {
            assert!(body.contains(line), "{line}");
        }
    }
}
