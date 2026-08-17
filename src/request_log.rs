// ABOUTME: Structured per-request access log with the edge's correlation ID
// ABOUTME: Pure formatting so the join with edge upload records is unit-tested

use axum::http::{header, HeaderMap};

/// Header the divine-blossom edge sets on every proxied upload request.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Placeholder for a value that is absent, so every line has every field.
const ABSENT: &str = "-";

/// Cap on the correlation ID copied into a log line. The edge sanitizes to 64
/// characters; this bounds the value independently because the header is
/// attacker-controllable on requests that do not come through the edge.
pub const MAX_REQUEST_ID_LEN: usize = 64;

/// Everything logged for one request.
pub struct RequestLogFields {
    pub req_id: String,
    pub method: String,
    pub path: String,
    /// Response status, or `None` when the request was dropped before one
    /// existed.
    ///
    /// `None` is what the edge giving up at its timeout looks like from here:
    /// the connection goes away, hyper drops the in-flight future, and no
    /// response is ever produced. Those requests are the population under
    /// investigation, so the line records them rather than omitting them.
    pub status: Option<u16>,
    /// Time from the request's headers arriving to its response being ready,
    /// or to the request being dropped when no response was produced.
    ///
    /// This spans receiving the request body as well as handling it, so it is
    /// not by itself a measure of processing cost. Its value is comparative:
    /// a `status=-` line means origin had the request in hand and was still
    /// working on it after this long, while no line at all means the request
    /// never reached origin.
    pub duration_ms: u64,
    pub content_length: Option<u64>,
}

/// Correlation ID for this request, matching `req_id` in the edge's upload
/// records so the two datasets can be joined.
///
/// Requests that do not arrive through the edge — resumable chunk appends go
/// straight to this service — have no such header and log the placeholder.
pub fn correlation_id(headers: &HeaderMap) -> String {
    let sanitized = headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(sanitize)
        .unwrap_or_default();

    if sanitized.is_empty() {
        ABSENT.to_string()
    } else {
        sanitized
    }
}

/// Restrict a correlation ID to a charset that cannot forge a log line.
///
/// Not a newline guard: `http::HeaderValue` refuses to hold control bytes, so
/// a header carrying `\r\n` cannot reach this function. The real hazard is
/// subtler — spaces and `=` are perfectly legal in a header value, and this
/// line is `key=value` separated by spaces, so an unsanitized
/// `X-Request-Id: a status=200` would forge a field in a joined dataset.
fn sanitize(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(MAX_REQUEST_ID_LEN)
        .collect()
}

/// Declared body size. Never a measured one — reading the body to size it
/// would defeat streaming on exactly the large uploads under investigation.
pub fn declared_content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

/// Render one request as a single log line.
pub fn format_request_log(fields: &RequestLogFields) -> String {
    format!(
        "[REQUEST] req_id={} method={} path={} status={} duration_ms={} content_length={}",
        fields.req_id,
        fields.method,
        fields.path,
        or_absent(fields.status),
        fields.duration_ms,
        or_absent(fields.content_length),
    )
}

/// Render an optional field, so every line carries every field.
fn or_absent<T: std::fmt::Display>(value: Option<T>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| ABSENT.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        map
    }

    #[test]
    fn correlation_id_comes_from_the_edge_header() {
        let map = headers(&[("x-request-id", "5602d9b23c24")]);
        assert_eq!(correlation_id(&map), "5602d9b23c24");
    }

    #[test]
    fn a_missing_correlation_id_renders_as_a_placeholder() {
        // Chunk appends come straight from clients and carry no edge ID. They
        // must still log, or the one path with no edge record also has no
        // origin record.
        assert_eq!(correlation_id(&HeaderMap::new()), "-");
    }

    #[test]
    fn correlation_ids_cannot_forge_log_fields() {
        // Control bytes are impossible here — HeaderValue rejects them. What is
        // possible, and legal HTTP, is a value carrying spaces and `=`, which
        // are exactly the separators this line format uses.
        let map = headers(&[("x-request-id", "abc status=200 duration_ms=1")]);
        let id = correlation_id(&map);

        assert!(!id.contains(' '));
        assert!(!id.contains('='));
        assert_eq!(id, "abcstatus200duration_ms1");
    }

    #[test]
    fn correlation_ids_are_length_capped() {
        let map = headers(&[("x-request-id", &"a".repeat(200))]);
        assert_eq!(correlation_id(&map).len(), MAX_REQUEST_ID_LEN);
    }

    #[test]
    fn an_all_invalid_correlation_id_falls_back_to_the_placeholder() {
        let map = headers(&[("x-request-id", "   ===   ")]);
        assert_eq!(correlation_id(&map), "-");
    }

    #[test]
    fn a_completed_request_renders_every_join_field() {
        let line = format_request_log(&RequestLogFields {
            req_id: "5602d9b23c24".into(),
            method: "PUT".into(),
            path: "/upload".into(),
            status: Some(200),
            duration_ms: 3200,
            content_length: Some(6_517_922),
        });

        assert!(line.starts_with("[REQUEST] "));
        assert!(line.contains("req_id=5602d9b23c24"));
        assert!(line.contains("method=PUT"));
        assert!(line.contains("path=/upload"));
        assert!(line.contains("status=200"));
        assert!(line.contains("duration_ms=3200"));
        assert!(line.contains("content_length=6517922"));
    }

    #[test]
    fn a_slow_request_records_its_duration() {
        // This is the field that answers whether origin was still working when
        // the edge gave up at its timeout.
        let line = format_request_log(&RequestLogFields {
            req_id: "abc".into(),
            method: "PUT".into(),
            path: "/upload".into(),
            status: Some(200),
            duration_ms: 119_500,
            content_length: Some(6_517_922),
        });

        assert!(line.contains("duration_ms=119500"));
    }

    #[test]
    fn an_absent_content_length_renders_as_a_placeholder() {
        let line = format_request_log(&RequestLogFields {
            req_id: "abc".into(),
            method: "POST".into(),
            path: "/upload/init".into(),
            status: Some(200),
            duration_ms: 12,
            content_length: None,
        });

        assert!(line.contains("content_length=-"));
    }

    #[test]
    fn the_line_is_a_single_line() {
        let line = format_request_log(&RequestLogFields {
            req_id: "abc".into(),
            method: "PUT".into(),
            path: "/upload".into(),
            status: Some(500),
            duration_ms: 1,
            content_length: None,
        });

        assert_eq!(line.lines().count(), 1);
    }

    #[test]
    fn an_abandoned_request_renders_a_placeholder_status() {
        // This is the shape a request the edge gave up on produces: hyper drops
        // the in-flight future, so there is no response and no status, but the
        // line still shows origin had the request and how long it had held it.
        let line = format_request_log(&RequestLogFields {
            req_id: "5602d9b23c24".into(),
            method: "PUT".into(),
            path: "/upload".into(),
            status: None,
            duration_ms: 120_000,
            content_length: Some(6_517_922),
        });

        assert!(line.contains("status=-"));
        assert!(line.contains("duration_ms=120000"));
        assert!(line.contains("content_length=6517922"));
    }

    #[test]
    fn declared_content_length_is_read_from_headers() {
        let map = headers(&[("content-length", "6517922")]);
        assert_eq!(declared_content_length(&map), Some(6_517_922));
    }

    #[test]
    fn a_missing_or_unparseable_content_length_is_none() {
        assert_eq!(declared_content_length(&HeaderMap::new()), None);
        assert_eq!(
            declared_content_length(&headers(&[("content-length", "not-a-number")])),
            None
        );
    }
}
