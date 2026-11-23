//! Restrictive upload hint, never authority to encode or change durable policy.
use axum::http::HeaderMap;

pub(crate) const HEADER: &str = "x-divine-video-processing";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VideoProcessing {
    Default,
    CopyOnly,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("X-Divine-Video-Processing must contain exactly one copy-only value")]
    Invalid,
    #[error("Set X-Divine-Video-Processing on completion, not session initialization")]
    Initialization,
    #[error("copy-only is unsupported with durable processing configured")]
    Unsupported,
}

impl VideoProcessing {
    pub(crate) fn parse(headers: &HeaderMap) -> Result<Self, Error> {
        let mut values = headers.get_all(HEADER).iter();
        match (values.next(), values.next()) {
            (None, None) => Ok(Self::Default),
            (Some(value), None) if value.as_bytes() == b"copy-only" => Ok(Self::CopyOnly),
            _ => Err(Error::Invalid),
        }
    }

    pub(crate) fn for_init(headers: &HeaderMap) -> Result<(), Error> {
        match Self::parse(headers)? {
            Self::Default => Ok(()),
            Self::CopyOnly => Err(Error::Initialization),
        }
    }

    pub(crate) async fn for_upload(
        headers: &HeaderMap,
        state: &crate::AppState,
    ) -> Result<Self, Error> {
        let policy = Self::parse(headers)?;
        // Existing outbox/journal contracts cannot carry this restriction. A
        // retained coordination binding can resume a historical pending intent
        // even when new uploads have no configured outbox. Reject before any
        // admission, body consumption, or session mutation in all these modes.
        policy.ensure_supported(
            state.processing_outbox.is_some(),
            state.media_storage.coordination_url().await.is_some(),
            state.original_write.is_some(),
        )?;
        Ok(policy)
    }

    fn ensure_supported(self, outbox: bool, history: bool, journal: bool) -> Result<(), Error> {
        if self == Self::CopyOnly && (outbox || history || journal) {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_copy_only_is_restricted_by_each_durable_mode() {
        for (outbox, history, journal) in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
            (true, true, true),
        ] {
            assert!(matches!(
                VideoProcessing::CopyOnly.ensure_supported(outbox, history, journal),
                Err(Error::Unsupported)
            ));
            assert!(VideoProcessing::Default
                .ensure_supported(outbox, history, journal)
                .is_ok());
        }
        assert!(VideoProcessing::CopyOnly
            .ensure_supported(false, false, false)
            .is_ok());
    }
}
