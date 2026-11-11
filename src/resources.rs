#[cfg(test)]
mod tests {
    #[test]
    fn maintenance_and_upload_admission_use_distinct_pools() {
        assert!(!std::sync::Arc::ptr_eq(
            &super::UPLOAD_ADMISSION.active,
            &super::THUMBNAIL_SLOTS
        ));
        assert!(!std::sync::Arc::ptr_eq(
            &super::UPLOAD_ADMISSION.active,
            &super::MIRROR_SLOTS
        ));
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
    async fn short_burst_waits_for_capacity_and_excess_requests_shed() {
        let admission = super::UploadAdmission::new(1, std::time::Duration::from_secs(5));
        let active = admission.acquire().await.unwrap();
        let mut queued = std::pin::pin!(admission.acquire());
        assert!(futures::poll!(queued.as_mut()).is_pending());

        let mut excess = std::pin::pin!(admission.acquire());
        assert!(matches!(
            futures::poll!(excess.as_mut()),
            std::task::Poll::Ready(Err(error)) if error.is::<super::ResourceError>()
        ));

        drop(active);
        let _admitted = queued.await.unwrap();
        assert_eq!(admission.waiting.available_permits(), 1);
    }

    #[tokio::test]
    async fn cancelling_a_waiter_releases_its_queue_slot() {
        let admission = super::UploadAdmission::new(1, std::time::Duration::from_secs(5));
        let _active = admission.acquire().await.unwrap();
        {
            let mut queued = std::pin::pin!(admission.acquire());
            assert!(futures::poll!(queued.as_mut()).is_pending());
        }
        assert_eq!(admission.waiting.available_permits(), 1);
    }

    #[tokio::test]
    async fn expired_waiters_release_their_queue_slots() {
        let admission = super::UploadAdmission::new(1, std::time::Duration::ZERO);
        let active = admission.acquire().await.unwrap();
        assert!(admission.acquire().await.is_err());
        assert_eq!(admission.waiting.available_permits(), 1);

        drop(active);
        assert!(admission.acquire().await.is_ok());
    }

    #[tokio::test]
    async fn pending_session_verification_is_bounded_independently() {
        let admission = super::UploadAdmission::new(1, std::time::Duration::from_secs(5));
        let verification = admission.verify_session().unwrap();
        assert!(admission.verify_session().is_err());
        let _active = admission.acquire().await.unwrap();

        drop(verification);
        assert!(admission.verify_session().is_ok());
    }
}

use anyhow::{anyhow, Result};
use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};
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
struct UploadAdmission {
    active: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    session_checks: Arc<Semaphore>,
    wait: Duration,
}

impl UploadAdmission {
    fn new(concurrency: usize, wait: Duration) -> Self {
        Self {
            active: Arc::new(Semaphore::new(concurrency)),
            waiting: Arc::new(Semaphore::new(concurrency)),
            session_checks: Arc::new(Semaphore::new(concurrency)),
            wait,
        }
    }

    fn verify_session(&self) -> Result<OwnedSemaphorePermit> {
        acquire_from(self.session_checks.clone())
    }

    async fn acquire(&self) -> Result<OwnedSemaphorePermit> {
        if let Ok(permit) = self.active.clone().try_acquire_owned() {
            return Ok(permit);
        }

        // Admit only a bounded number of waiters and never read their bodies here.
        let _waiting = acquire_from(self.waiting.clone())?;
        let permit = tokio::time::timeout(self.wait, self.active.clone().acquire_owned())
            .await
            .map_err(|_| ResourceError::Busy)?;
        permit.map_err(|_| ResourceError::Busy.into())
    }
}

static UPLOAD_ADMISSION: LazyLock<UploadAdmission> = LazyLock::new(|| {
    UploadAdmission::new(
        positive_setting("MAX_CONCURRENT_UPLOADS", 4)
            .unwrap_or(4)
            .min(1024) as usize,
        Duration::from_secs(5),
    )
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
pub async fn acquire() -> Result<OwnedSemaphorePermit> {
    UPLOAD_ADMISSION.acquire().await
}
pub fn acquire_session_verification() -> Result<OwnedSemaphorePermit> {
    UPLOAD_ADMISSION.verify_session()
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
