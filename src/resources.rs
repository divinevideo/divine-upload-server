#[cfg(test)]
mod tests {
    #[test]
    fn maintenance_and_upload_admission_use_distinct_pools() {
        assert!(!std::sync::Arc::ptr_eq(
            &super::SLOTS,
            &super::MAINTENANCE_SLOTS
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
    let slots = positive_setting("MAX_CONCURRENT_UPLOADS", 4)?;
    if slots > 1024 {
        return Err(anyhow!("MAX_CONCURRENT_UPLOADS must not exceed 1024"));
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
// Public maintenance requests must not consume authenticated upload admission.
static MAINTENANCE_SLOTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));
fn acquire_from(slots: Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
    slots
        .try_acquire_owned()
        .map_err(|_| ResourceError::Busy.into())
}
pub fn acquire() -> Result<OwnedSemaphorePermit> {
    acquire_from(SLOTS.clone())
}
pub fn acquire_maintenance() -> Result<OwnedSemaphorePermit> {
    acquire_from(MAINTENANCE_SLOTS.clone())
}
