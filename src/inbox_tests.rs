use super::*;
use crate::storage::StorageProvider;

pub(super) async fn wait_for_expiry_while_locked(
    holder: &tokio_postgres::Transaction<'_>,
    waiter_pid: i32,
    job_id: &str,
) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let waiting: bool = holder
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')",
                    &[&waiter_pid],
                )
                .await
                .unwrap()
                .get(0);
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("request must actually wait on the other connection's row lock");
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let expired: bool = holder
                .query_one(
                    "SELECT LEAST(lease_until,attempt_deadline)<=clock_timestamp() FROM media_processing_jobs WHERE processing_job_id=$1",
                    &[&job_id],
                )
                .await
                .unwrap()
                .get(0);
            if expired {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("database clock must cross the reservation boundary");
}

async fn renewal_after_lock_wait(expire_lease: bool) {
    let Some((store, second)) = stores().await else {
        return;
    };
    let event = event();
    store.accept(&event, &event.job_id).await.unwrap();
    let assignment = store.claim().await.unwrap().unwrap();
    let lease = Lease::from(&assignment);
    let waiter_pid: i32 = store
        .database
        .lock()
        .await
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    store.database.lock().await.execute(
        "UPDATE media_processing_jobs SET lease_until=clock_timestamp()+CASE WHEN $2 THEN interval '2 seconds' ELSE interval '60 seconds' END,attempt_deadline=clock_timestamp()+CASE WHEN $2 THEN interval '60 seconds' ELSE interval '2 seconds' END WHERE processing_job_id=$1",
        &[&lease.processing_job_id,&expire_lease],
    ).await.unwrap();
    let before: f64 = store.database.lock().await.query_one(
        "SELECT extract(epoch FROM lease_until)::double precision FROM media_processing_jobs WHERE processing_job_id=$1",
        &[&lease.processing_job_id],
    ).await.unwrap().get(0);
    let mut holder = second.database.lock().await;
    let tx = holder.transaction().await.unwrap();
    tx.query_one(
        "SELECT 1 FROM media_processing_jobs WHERE processing_job_id=$1 FOR UPDATE",
        &[&lease.processing_job_id],
    )
    .await
    .unwrap();
    let waiting_store = store.clone();
    let waiting_lease = lease.clone();
    let request = tokio::spawn(async move { waiting_store.renew(&waiting_lease).await });
    wait_for_expiry_while_locked(&tx, waiter_pid, &lease.processing_job_id).await;
    tx.commit().await.unwrap();
    drop(holder);
    assert!(
        !request.await.unwrap().unwrap(),
        "renewal must reject expiry crossed during a lock-only wait"
    );
    let after: f64 = store.database.lock().await.query_one(
        "SELECT extract(epoch FROM lease_until)::double precision FROM media_processing_jobs WHERE processing_job_id=$1",
        &[&lease.processing_job_id],
    ).await.unwrap().get(0);
    assert_eq!(
        before, after,
        "a rejected renewal must not extend the lease"
    );
}

#[tokio::test]
async fn renewal_lock_wait_cannot_revive_expired_lease() {
    renewal_after_lock_wait(true).await;
}

#[tokio::test]
async fn renewal_lock_wait_cannot_accept_expired_deadline() {
    renewal_after_lock_wait(false).await;
}

pub(super) fn event() -> MediaFinalizedV1 {
    MediaFinalizedV1::new(
        &"a".repeat(64),
        15,
        "video/mp4",
        &"b".repeat(64),
        ObjectRef {
            provider: StorageProvider::B2,
            bucket: "synthetic-private".into(),
            key: "a".repeat(64),
            version: Some("version-1".into()),
        },
    )
    .unwrap()
}

pub(super) async fn stores() -> Option<(InboxStore, InboxStore)> {
    let Ok(url) = std::env::var("TEST_COORDINATION_DATABASE_URL") else {
        eprintln!("SKIP inbox database proof: disposable TEST_COORDINATION_DATABASE_URL not set");
        return None;
    };
    let schema = format!("inbox_test_{:016x}", rand::random::<u64>());
    let one = crate::coordination::connect(&url).await.unwrap();
    one.batch_execute(&format!(
        "CREATE SCHEMA {schema}; SET search_path TO {schema}"
    ))
    .await
    .unwrap();
    let two = crate::coordination::connect(&url).await.unwrap();
    two.batch_execute(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    Some((
        InboxStore::from_client(one).await.unwrap(),
        InboxStore::from_client(two).await.unwrap(),
    ))
}

pub(super) fn marker(assignment: &Assignment) -> ObjectRef {
    ObjectRef {
        provider: StorageProvider::B2,
        bucket: "synthetic-private".into(),
        key: format!(
            "_private/artifact-families/v1/{}/complete.json",
            assignment.sha256
        ),
        version: Some("marker-v1".into()),
    }
}

#[tokio::test]
async fn durable_replay_and_conflict_preserve_one_job_and_owner_events() {
    let Some((store, second)) = stores().await else {
        return;
    };
    let first = event();
    store.accept(&first, &first.job_id).await.unwrap();
    second.accept(&first, &first.job_id).await.unwrap();
    let mut other = first.clone();
    other.owner = Some("c".repeat(64));
    other.content_type = "video/webm".into();
    other.refresh_job_id().unwrap();
    second.accept(&other, &other.job_id).await.unwrap();
    let mut conflict = first.clone();
    conflict.content_type = "video/webm".into();
    assert!(store.accept(&conflict, &conflict.job_id).await.is_err());
    let client = store.database.lock().await;
    let row=client.query_one("SELECT (SELECT count(*) FROM media_finalization_inbox),(SELECT count(*) FROM media_processing_jobs)",&[]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 2);
    assert_eq!(row.get::<_, i64>(1), 1);
    drop(client);
    let (a, b) = tokio::join!(store.claim(), second.claim());
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(usize::from(a.is_some()) + usize::from(b.is_some()), 1);
    assert_eq!(
        a.or(b).unwrap().content_type,
        "video/mp4",
        "owner MIME hints cannot retarget accepted processing input"
    );
}

#[tokio::test]
async fn changed_targets_fence_old_completion_and_reclaim_exact_all_destinations() {
    let Some((store, second)) = stores().await else {
        return;
    };
    let first = event();
    store.accept(&first, &first.job_id).await.unwrap();
    let old = store.claim().await.unwrap().unwrap();
    let mut changed = first.clone();
    changed.replicas.push(ObjectRef {
        provider: StorageProvider::Gcs,
        bucket: "synthetic-gcs".into(),
        key: first.sha256.clone(),
        version: Some("123".into()),
    });
    changed.refresh_job_id().unwrap();
    second.accept(&changed, &changed.job_id).await.unwrap();
    assert!(!store.renew(&Lease::from(&old)).await.unwrap());
    assert!(!store
        .complete(&Lease::from(&old), &marker(&old))
        .await
        .unwrap());
    assert!(
        second.claim().await.unwrap().is_none(),
        "don't overlap an unexpired attempt"
    );
    store
        .database
        .lock()
        .await
        .execute(
            "UPDATE media_processing_jobs SET lease_until=clock_timestamp()-interval '1 second'",
            &[],
        )
        .await
        .unwrap();
    let fresh = second.claim().await.unwrap().unwrap();
    assert!(fresh.lease_generation > old.lease_generation);
    assert!(fresh.input_revision > old.input_revision);
    assert_eq!(fresh.destinations.len(), 2);
    assert!(second.renew(&Lease::from(&fresh)).await.unwrap());
    assert!(second
        .complete(&Lease::from(&fresh), &marker(&fresh))
        .await
        .unwrap());
    assert!(
        second
            .complete(&Lease::from(&fresh), &marker(&fresh))
            .await
            .unwrap(),
        "lost completion ack is replayable"
    );
    assert!(!store
        .complete(&Lease::from(&old), &marker(&old))
        .await
        .unwrap());
    assert!(store.claim().await.unwrap().is_none());
}

#[tokio::test]
async fn expiry_hard_deadline_and_attempt_budget_never_accept_stale_or_loop_forever() {
    let Some((store, _)) = stores().await else {
        return;
    };
    let first = event();
    store.accept(&first, &first.job_id).await.unwrap();
    for generation in 1..=3 {
        let assignment = store.claim().await.unwrap().unwrap();
        assert_eq!(assignment.lease_generation, generation);
        store.database.lock().await.execute("UPDATE media_processing_jobs SET attempt_deadline=clock_timestamp()-interval '1 second'",&[]).await.unwrap();
        assert!(!store.renew(&Lease::from(&assignment)).await.unwrap());
        assert!(!store
            .complete(&Lease::from(&assignment), &marker(&assignment))
            .await
            .unwrap());
        store.database.lock().await.execute("UPDATE media_processing_jobs SET lease_until=clock_timestamp()-interval '1 second'",&[]).await.unwrap();
    }
    assert!(store.claim().await.unwrap().is_none());
    assert!(store.claim().await.unwrap().is_none());
    let client = store.database.lock().await;
    let row=client.query_one("SELECT state, failure_reason, (SELECT count(*) FROM media_finalization_inbox) FROM media_processing_jobs",&[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "blocked");
    assert_eq!(row.get::<_, String>(1), "attempt_budget_exhausted");
    assert_eq!(row.get::<_, i64>(2), 1);
}

#[tokio::test]
async fn bounded_budget_sweep_skips_locked_exhausted_jobs() {
    let Some((store, second)) = stores().await else {
        return;
    };
    let first = event();
    store.accept(&first, &first.job_id).await.unwrap();
    let id = validate_event(&first, &first.job_id).unwrap();
    store.database.lock().await.execute("UPDATE media_processing_jobs SET state='running',lease_generation=3,attempts_in_revision=3,lease_until=clock_timestamp()-interval '1 second'",&[]).await.unwrap();
    let mut client = second.database.lock().await;
    let tx = client.transaction().await.unwrap();
    tx.query_one(
        "SELECT processing_job_id FROM media_processing_jobs WHERE processing_job_id=$1 FOR UPDATE",
        &[&id],
    )
    .await
    .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_millis(300), store.claim()).await;
    assert!(
        result.is_ok(),
        "one locked exhausted job must not block intake/claims"
    );
    assert!(result.unwrap().unwrap().is_none());
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn completed_receipt_survives_expiry_and_supersession_has_a_new_bounded_budget() {
    let Some((store, _)) = stores().await else {
        return;
    };
    let first = event();
    store.accept(&first, &first.job_id).await.unwrap();
    let mut last = None;
    for _ in 0..3 {
        let assignment = store.claim().await.unwrap().unwrap();
        store.database.lock().await.execute("UPDATE media_processing_jobs SET lease_until=clock_timestamp()-interval '1 second'",&[]).await.unwrap();
        last = Some(assignment);
    }
    let last = last.unwrap();
    // Make the third lease live solely to exercise a completion at its budget.
    store
        .database
        .lock()
        .await
        .execute(
            "UPDATE media_processing_jobs SET lease_until=clock_timestamp()+interval '30 seconds'",
            &[],
        )
        .await
        .unwrap();
    let manifest = marker(&last);
    assert!(store
        .complete(&Lease::from(&last), &manifest)
        .await
        .unwrap());
    store.database.lock().await.execute("UPDATE media_processing_jobs SET attempt_deadline=clock_timestamp()-interval '1 second'",&[]).await.unwrap();
    let receipt = store
        .lookup(&last.processing_job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.state, "completed");
    assert_eq!(receipt.manifest, Some(manifest));
    let mut other = first.clone();
    other.owner = Some("c".repeat(64));
    other.refresh_job_id().unwrap();
    store.accept(&other, &other.job_id).await.unwrap();
    assert_eq!(
        store
            .lookup(&last.processing_job_id)
            .await
            .unwrap()
            .unwrap()
            .input_revision,
        last.input_revision
    );
    let mut changed = first.clone();
    changed.replicas.push(ObjectRef {
        provider: StorageProvider::Gcs,
        bucket: "synthetic-gcs".into(),
        key: first.sha256,
        version: Some("123".into()),
    });
    changed.refresh_job_id().unwrap();
    store.accept(&changed, &changed.job_id).await.unwrap();
    let newer = store
        .lookup(&last.processing_job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(newer.state, "pending");
    assert!(newer.manifest.is_none());
    assert!(newer.input_revision > receipt.input_revision);
    let fresh = store
        .claim()
        .await
        .unwrap()
        .expect("new destination revision gets its own attempt budget");
    assert_eq!(fresh.lease_generation, 4);
    assert!(!store
        .complete(&Lease::from(&last), &marker(&last))
        .await
        .unwrap());
}

#[tokio::test]
async fn destination_growth_is_bounded_and_rejection_is_atomic() {
    let Some((store, _)) = stores().await else {
        return;
    };
    let mut first = event();
    first.source.provider = StorageProvider::Gcs;
    first.source.version = Some("123".into());
    first.refresh_job_id().unwrap();
    store.accept(&first, &first.job_id).await.unwrap();
    for n in 0..15 {
        let mut next = first.clone();
        next.replicas.push(ObjectRef {
            provider: StorageProvider::B2,
            bucket: "synthetic-private".into(),
            key: first.sha256.clone(),
            version: Some(format!("copy-{n}")),
        });
        next.refresh_job_id().unwrap();
        store.accept(&next, &next.job_id).await.unwrap();
    }
    let mut overflow = first.clone();
    overflow.replicas.push(ObjectRef {
        provider: StorageProvider::B2,
        bucket: "synthetic-private".into(),
        key: first.sha256.clone(),
        version: Some("overflow".into()),
    });
    overflow.refresh_job_id().unwrap();
    assert!(
        store.accept(&overflow, &overflow.job_id).await.is_err(),
        "unbounded destination expansion must not be acknowledged"
    );
    let id = validate_event(&first, &first.job_id).unwrap();
    assert_eq!(
        store.lookup(&id).await.unwrap().unwrap().destinations.len(),
        16
    );
    assert_eq!(
        store
            .database
            .lock()
            .await
            .query_one("SELECT count(*) FROM media_finalization_inbox", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        16
    );
}
