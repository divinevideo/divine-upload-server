#[cfg(test)]
mod tests {
    #[test]
    fn maintenance_and_upload_admission_use_distinct_pools() {
        assert!(!std::sync::Arc::ptr_eq(
            &super::SLOTS,
            &super::THUMBNAIL_SLOTS
        ));
        assert!(!std::sync::Arc::ptr_eq(&super::SLOTS, &super::MIRROR_SLOTS));
        assert!(!std::sync::Arc::ptr_eq(
            &super::THUMBNAIL_SLOTS,
            &super::MIRROR_SLOTS
        ));
    }
    #[test]
    fn upload_size_limit_rejects_excess_without_allocation() {
        assert!(super::check_size_with_limit(1024, 1024).is_ok());
        assert!(super::check_size_with_limit(1025, 1024).is_err());
        assert!(super::check_size_with_limit(u64::MAX, 1024).is_err());
    }
    #[tokio::test]
    async fn exhausted_capacity_is_retryable() {
        let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let permit = super::acquire_from(slots.clone()).unwrap();
        assert!(super::acquire_from(slots.clone()).is_err());
        drop(permit);
        assert!(super::acquire_from(slots).is_ok());
    }
}

use anyhow::{anyhow, Result};
use std::sync::{Arc, LazyLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, thiserror::Error)]
pub enum ResourceError {
    #[error("Upload exceeds MAX_UPLOAD_BYTES")]
    TooLarge,
    #[error("Upload capacity exhausted; retry later")]
    Busy,
}
fn positive_setting(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| anyhow!("{name} must be a positive integer")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(anyhow!("Invalid {name}")),
    }
}
pub fn validate() -> Result<()> {
    positive_setting("MAX_UPLOAD_BYTES", 1024 * 1024 * 1024)?;
    for (name, default) in [
        ("MAX_CONCURRENT_UPLOADS", 4),
        ("MAX_CONCURRENT_THUMBNAILS", 2),
        ("MAX_CONCURRENT_MIRRORS", 1),
    ] {
        if positive_setting(name, default)? > 1024 {
            return Err(anyhow!("{name} must not exceed 1024"));
        }
    }
    if positive_setting("MIRROR_FETCH_TIMEOUT_SECS", 120)? > 3600 {
        return Err(anyhow!("MIRROR_FETCH_TIMEOUT_SECS must not exceed 3600"));
    }
    Ok(())
}
pub fn max_upload_bytes() -> u64 {
    positive_setting("MAX_UPLOAD_BYTES", 1024 * 1024 * 1024).unwrap_or(1024 * 1024 * 1024)
}
pub fn check_size(size: u64) -> Result<()> {
    check_size_with_limit(size, max_upload_bytes())
}
fn check_size_with_limit(size: u64, limit: u64) -> Result<()> {
    if size > limit {
        return Err(ResourceError::TooLarge.into());
    }
    Ok(())
}
static SLOTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    Arc::new(Semaphore::new(
        positive_setting("MAX_CONCURRENT_UPLOADS", 4)
            .unwrap_or(4)
            .min(1024) as usize,
    ))
});
// Public mirrors and thumbnail generation cannot starve each other or uploads.
static THUMBNAIL_SLOTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    Arc::new(Semaphore::new(
        positive_setting("MAX_CONCURRENT_THUMBNAILS", 2)
            .unwrap_or(2)
            .min(1024) as usize,
    ))
});
static MIRROR_SLOTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    Arc::new(Semaphore::new(
        positive_setting("MAX_CONCURRENT_MIRRORS", 1)
            .unwrap_or(1)
            .min(1024) as usize,
    ))
});
fn acquire_from(slots: Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
    slots
        .try_acquire_owned()
        .map_err(|_| ResourceError::Busy.into())
}
pub fn acquire() -> Result<OwnedSemaphorePermit> {
    acquire_from(SLOTS.clone())
}
pub fn acquire_thumbnail() -> Result<OwnedSemaphorePermit> {
    acquire_from(THUMBNAIL_SLOTS.clone())
}
pub fn acquire_mirror() -> Result<OwnedSemaphorePermit> {
    acquire_from(MIRROR_SLOTS.clone())
}
pub fn mirror_fetch_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(
        positive_setting("MIRROR_FETCH_TIMEOUT_SECS", 120)
            .unwrap_or(120)
            .min(3600),
    )
}
