// ABOUTME: Canonical form for a declared Content-Type before it is matched
// ABOUTME: Lowercases the type/subtype and drops any parameters and whitespace

/// Reduce a declared `Content-Type` to its bare, lowercase type/subtype.
///
/// Media types are case-insensitive and may carry parameters, so `VIDEO/MP4`
/// and `video/mp4;codecs="avc1.42E01E"` name the same type as `video/mp4`.
/// Matching a raw header value against a fixed list misses both forms.
///
/// The declared value is only ever a claim by the client. Normalising it makes
/// the claim comparable; it does not make it trustworthy.
pub fn normalize(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passes_through_an_already_canonical_type() {
        assert_eq!(normalize("video/mp4"), "video/mp4");
    }

    #[test]
    fn lowercases_type_and_subtype() {
        assert_eq!(normalize("VIDEO/MP4"), "video/mp4");
        assert_eq!(normalize("Video/QuickTime"), "video/quicktime");
    }

    #[test]
    fn drops_parameters() {
        assert_eq!(normalize("video/mp4;codecs=\"avc1.42E01E\""), "video/mp4");
        assert_eq!(normalize("video/webm; codecs=vp9"), "video/webm");
    }

    #[test]
    fn trims_surrounding_whitespace() {
        assert_eq!(normalize("  video/mp4  "), "video/mp4");
        assert_eq!(normalize("video/mp4 ; codecs=avc1"), "video/mp4");
    }

    #[test]
    fn malformed_values_normalize_to_something_harmless() {
        assert_eq!(normalize(""), "");
        assert_eq!(normalize(";codecs=avc1"), "");
        assert_eq!(normalize("   "), "");
    }
}
