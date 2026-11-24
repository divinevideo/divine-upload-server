//! Optional-backup completion, rollback reads, and recorded-contract recovery.
use super::*;

#[tokio::test]
async fn backup_recovery_interrupted_b2_only_write_retries_as_new_then_replays() {
    let f = Fixture::new().await;
    let storage = f.storage.clone().with_gcs_backup_enabled(false);
    f.objects.lock().unwrap().fail_verified_b2_head_once = true;
    let first = storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await;
    assert!(first.is_err());
    {
        let objects = f.objects.lock().unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&admission_record(&objects).1).unwrap();
        assert_eq!(value["b2"]["state"], "verified");
        assert_eq!(value["gcs"]["state"], "not_issued");
    }
    let recovered = storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .unwrap();
    assert!(
        recovered.newly_stored,
        "interrupted write must retain its best-effort hook opportunity"
    );
    assert!(
        !storage
            .put("media", Bytes::from_static(b"original"), "video/mp4", None)
            .await
            .unwrap()
            .newly_stored
    );
    assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
}

#[tokio::test]
async fn backup_recovery_completed_thumbnail_survives_backup_reenable_and_gcs_rollback() {
    for mode in [WriteMode::B2Primary, WriteMode::Gcs] {
        let f = Fixture::new().await;
        let hash = "a".repeat(64);
        let thumb = format!("{hash}.jpg");
        f.storage
            .clone()
            .with_gcs_backup_enabled(false)
            .put(
                &thumb,
                Bytes::from_static(b"synthetic-jpeg"),
                "image/jpeg",
                None,
            )
            .await
            .unwrap();
        // The original's accepted primary record determines derivative placement.
        {
            let mut objects = f.objects.lock().unwrap();
            let (generation, bytes) = admission_record(&objects).clone();
            let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            value["key"] = hash.clone().into();
            value["sha256"] = hash.clone().into();
            value["b2"]["object"]["key"] = hash.clone().into();
            objects.records.insert(
                format!(
                    "__resumable/b2-primary-writes/v1/{}.json",
                    hex::encode(Sha256::digest(hash.as_bytes()))
                ),
                (generation, Bytes::from(serde_json::to_vec(&value).unwrap())),
            );
        }
        let mut state = crate::tests::test_state(None, 1);
        Arc::get_mut(&mut state).unwrap().media_storage = f
            .storage
            .with_mode(mode)
            .unwrap()
            .with_gcs_backup_enabled(true);
        let response = crate::handle_thumbnail_generate(
            State(state),
            axum::extract::Path(hash),
            axum::http::HeaderMap::new(),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK, "{mode:?}");
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 100)
                .await
                .unwrap(),
            "synthetic-jpeg"
        );
        assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
    }
}

#[tokio::test]
async fn backup_recovery_cross_mode_sessions_repair_only_their_required_copies() {
    use crate::resumable::{ResumableBackend, ResumableRuntime, UploadSession};
    for mode in [WriteMode::Gcs, WriteMode::Dual, WriteMode::B2] {
        let Some(url) = crate::outbox::tests::isolated_database_url().await else {
            return;
        };
        let f = Fixture::new().await;
        let storage = f.storage.clone().with_gcs_backup_enabled(false);
        let hash = hex::encode(Sha256::digest(b"original"));
        let original = storage
            .put(
                &hash,
                Bytes::from_static(b"original"),
                "video/mp4",
                Some(&"a".repeat(64)),
            )
            .await
            .unwrap();
        let runtime = ResumableRuntime::new(
            storage,
            f.storage.gcs.clone(),
            "fixture-gcs".into(),
            Some(&url),
        )
        .await
        .unwrap();
        let temp_object = if mode == WriteMode::Gcs {
            "__resumable/uploads/up_old/blob".into()
        } else {
            format!(
                "__resumable/staged/{}/{}/{}/up_old",
                if mode == WriteMode::B2 { "b2" } else { "dual" },
                hex::encode("fixture-gcs"),
                hex::encode("fixture-b2")
            )
        };
        let session = UploadSession {
            processing_outbox_required: Some(false),
            revision: 0,
            chunk_size: 1024,
            upload_id: "up_old".into(),
            owner: "b".repeat(64),
            final_sha256: hash.clone(),
            declared_size: 8,
            content_type: "application/octet-stream".into(),
            file_name: None,
            expires_at_epoch_secs: u64::MAX,
            next_offset: 8,
            session_url: if mode == WriteMode::Gcs {
                "native-gcs"
            } else {
                "staged-v1"
            }
            .into(),
            session_token: "fixture".into(),
            temp_object: temp_object.clone(),
            storage_complete: true,
            finalized_object: None,
        };
        if mode == WriteMode::Gcs {
            f.objects
                .lock()
                .unwrap()
                .staging
                .insert(temp_object, Bytes::from_static(b"original"));
        } else {
            let db = crate::coordination::connect(&url).await.unwrap();
            db.execute(
                "INSERT INTO upload_chunks VALUES($1,0,7,$2,$3)",
                &[
                    &temp_object,
                    &hash,
                    &serde_json::to_value(&original.objects).unwrap(),
                ],
            )
            .await
            .unwrap();
        }
        assert!(
            !runtime.backend.finalize(&session).await.unwrap(),
            "repair is not a new original: {mode:?}"
        );
        let refs = runtime.backend.final_objects(&session).await.unwrap();
        let expected = match mode {
            WriteMode::Gcs => vec![StorageProvider::Gcs],
            WriteMode::Dual => vec![StorageProvider::Gcs, StorageProvider::B2],
            _ => vec![StorageProvider::B2],
        };
        assert_eq!(
            refs.iter().map(|r| r.provider).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            f.objects.lock().unwrap().writes,
            if mode == WriteMode::B2 {
                vec!["b2"]
            } else {
                vec!["b2", "gcs"]
            }
        );
        assert!(!runtime.backend.finalize(&session).await.unwrap());
    }
}

#[tokio::test]
async fn backup_recovery_recorded_outbox_primary_keeps_both_copies_with_switch_off() {
    for video in [false, true] {
        let Some(url) = crate::outbox::tests::isolated_database_url().await else {
            return;
        };
        let outbox = crate::outbox::ProcessingOutbox::connect(
            &url,
            "https://processor.invalid/finalized",
            "fixture-token",
        )
        .await
        .unwrap();
        let f = Fixture::new().await;
        let hash = hex::encode(Sha256::digest(b"original"));
        let owner = "a".repeat(64);
        let mime = if video {
            "video/mp4"
        } else {
            "application/octet-stream"
        };
        outbox
            .begin_upload(&hash, 8, mime, &owner, &f.storage)
            .await
            .unwrap();
        let intent = outbox
            .begin_direct_upload(
                &hash,
                8,
                mime,
                &owner,
                &f.storage.clone().with_gcs_backup_enabled(false),
            )
            .await
            .unwrap();
        let stored = intent
            .storage
            .put(&hash, Bytes::from_static(b"original"), mime, Some(&owner))
            .await
            .unwrap();
        assert_eq!(
            stored
                .objects
                .iter()
                .map(|r| r.provider)
                .collect::<Vec<_>>(),
            vec![StorageProvider::B2, StorageProvider::Gcs]
        );
        assert_eq!(intent.thumbnail.is_some(), video);
        if let Some(reservation) = intent.thumbnail {
            let objects = stored
                .objects
                .iter()
                .cloned()
                .map(|mut r| {
                    r.key = format!("{hash}.jpg");
                    r
                })
                .collect();
            outbox
                .record_thumbnail(
                    &hash,
                    &owner,
                    &reservation,
                    stored.objects.clone(),
                    crate::outbox::VerifiedThumbnail {
                        sha256: "b".repeat(64),
                        size: 10,
                        objects,
                    },
                )
                .await
                .unwrap();
        }
        outbox
            .enqueue_upload(&hash, 8, mime, &owner, stored.objects)
            .await
            .unwrap();
        let db = crate::coordination::connect(&url).await.unwrap();
        assert!(db
            .query_one(
                "SELECT completed FROM upload_processing_intents WHERE sha256=$1",
                &[&hash]
            )
            .await
            .unwrap()
            .get::<_, bool>(0));
    }
}

#[tokio::test]
async fn backup_recovery_outbox_reconciler_requires_recorded_copies_without_writing() {
    for mode in [
        WriteMode::B2Primary,
        WriteMode::Dual,
        WriteMode::Gcs,
        WriteMode::B2,
    ] {
        let Some(url) = crate::outbox::tests::isolated_database_url().await else {
            return;
        };
        let outbox = crate::outbox::ProcessingOutbox::connect(
            &url,
            "https://processor.invalid/finalized",
            "fixture-token",
        )
        .await
        .unwrap();
        let f = Fixture::new().await;
        let storage = f.storage.clone().with_gcs_backup_enabled(false);
        let hash = hex::encode(Sha256::digest(b"original"));
        let owner = "a".repeat(64);
        outbox
            .begin_upload(
                &hash,
                8,
                "application/octet-stream",
                &owner,
                &f.storage.with_mode(mode).unwrap(),
            )
            .await
            .unwrap();
        storage
            .put(
                &hash,
                Bytes::from_static(b"original"),
                "application/octet-stream",
                Some(&owner),
            )
            .await
            .unwrap();
        let recovered = outbox.recover_intents(&storage, 10, 1000).await.unwrap();
        assert_eq!(recovered, if mode == WriteMode::B2 { 1 } else { 0 });
        assert_eq!(
            f.objects.lock().unwrap().writes,
            ["b2"],
            "reconciler must not issue backup repairs"
        );
        if mode != WriteMode::B2 {
            f.storage
                .put(
                    &hash,
                    Bytes::from_static(b"original"),
                    "application/octet-stream",
                    Some(&owner),
                )
                .await
                .unwrap();
            assert_eq!(
                outbox.recover_intents(&storage, 10, 1000).await.unwrap(),
                1,
                "{mode:?}"
            );
            assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
        }
    }
}

#[tokio::test]
async fn backup_recovery_completion_control_ack_and_fence_outcomes() {
    for outcome in [
        "rejected",
        "lost-ack",
        "older-actor-lands",
        "concurrent-retry",
    ] {
        let f = Fixture::new().await;
        let storage = f.storage.clone().with_gcs_backup_enabled(false);
        {
            let mut objects = f.objects.lock().unwrap();
            objects.fail_completion_once = outcome != "lost-ack";
            objects.lose_completion_ack_once = outcome == "lost-ack";
        }
        let first = storage
            .put("media", Bytes::from_static(b"original"), "video/mp4", None)
            .await;
        if outcome == "lost-ack" {
            assert!(first.unwrap().newly_stored);
        } else {
            assert!(first.is_err());
            f.objects.lock().unwrap().land_stale_completion_before_fence =
                outcome == "older-actor-lands";
            if outcome == "concurrent-retry" {
                let (one, two) = tokio::join!(
                    storage.put("media", Bytes::from_static(b"original"), "video/mp4", None),
                    storage.put("media", Bytes::from_static(b"original"), "video/mp4", None)
                );
                assert_eq!(
                    [one, two]
                        .iter()
                        .filter(|r| r.as_ref().is_ok_and(|r| r.newly_stored))
                        .count(),
                    1
                );
            } else {
                let retry = storage
                    .put("media", Bytes::from_static(b"original"), "video/mp4", None)
                    .await;
                if outcome == "older-actor-lands" {
                    assert!(retry
                        .unwrap_err()
                        .to_string()
                        .contains("changed during update"));
                } else {
                    assert!(retry.unwrap().newly_stored);
                }
            }
        }
        assert!(
            !storage
                .put("media", Bytes::from_static(b"original"), "video/mp4", None)
                .await
                .unwrap()
                .newly_stored
        );
        let objects = f.objects.lock().unwrap();
        let (generation, committed) = admission_record(&objects);
        if outcome == "rejected" || outcome == "concurrent-retry" {
            let (stale_generation, stale_payload) = &objects.completion_requests[0];
            assert!(*generation > *stale_generation);
            assert_ne!(
                committed, stale_payload,
                "stale actor cannot claim the retry's completion by readback"
            );
        }
        assert_eq!(objects.writes, ["b2"]);
    }
}

#[tokio::test]
async fn backup_recovery_completed_cache_checks_recorded_hash_provenance_and_version() {
    for corruption in [
        "same-size-bytes",
        "owner",
        "mime",
        "wrong-version",
        "latest-changed",
    ] {
        let f = Fixture::new().await;
        let key = format!("{}.jpg", "a".repeat(64));
        f.storage
            .clone()
            .with_gcs_backup_enabled(false)
            .put(
                &key,
                Bytes::from_static(b"original"),
                "image/jpeg",
                Some("fixture-owner"),
            )
            .await
            .unwrap();
        {
            let mut objects = f.objects.lock().unwrap();
            match corruption {
                "same-size-bytes" => {
                    objects.b2.as_mut().unwrap().bytes = Bytes::from_static(b"tampered")
                }
                "owner" => objects.b2.as_mut().unwrap().owner = Some("different-owner".into()),
                "mime" => objects.b2.as_mut().unwrap().mime = "video/mp4".into(),
                "wrong-version" => objects.wrong_pinned_b2_head = true,
                _ => objects.latest_b2_version = Some("b2-version-2".into()),
            }
        }
        let result = f.storage.read_verified(&key).await;
        assert_eq!(
            result.is_ok(),
            corruption == "latest-changed",
            "{corruption}"
        );
        let objects = f.objects.lock().unwrap();
        assert_eq!(objects.writes, ["b2"]);
        assert!(objects
            .read_versions
            .iter()
            .all(|version| version == "b2-version-1"));
    }
}

// Frozen v1 wire shape from ff807d4, intentionally independent of the new
// Record implementation: strict old readers reject unknown fields/variants.
#[derive(Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum LegacyPrimarySlot {
    NotIssued,
    Issued,
    Acknowledged { object: ObjectRef },
    Verified { object: ObjectRef },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyPrimaryRecord {
    schema_version: u8,
    mode: WriteMode,
    key: String,
    gcs_bucket: String,
    b2_bucket: String,
    size: u64,
    sha256: String,
    content_type: String,
    owner: Option<String>,
    attempt: String,
    multipart_upload_id: Option<String>,
    b2: LegacyPrimarySlot,
    gcs: LegacyPrimarySlot,
}

#[tokio::test]
async fn backup_recovery_completion_marker_keeps_legacy_json_and_nonce_fences() {
    let f = Fixture::new().await;
    let storage = f.storage.clone().with_gcs_backup_enabled(false);
    storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .unwrap();
    let completed = {
        let objects = f.objects.lock().unwrap();
        serde_json::from_slice::<serde_json::Value>(&admission_record(&objects).1).unwrap()
    };
    let legacy: LegacyPrimaryRecord = serde_json::from_value(completed.clone()).unwrap();
    assert_eq!(serde_json::to_value(&legacy).unwrap(), completed);
    let nonce = legacy.attempt.strip_prefix("b2-only-complete-v1:").unwrap();
    assert_eq!(nonce.len(), 32);
    assert!(nonce
        .bytes()
        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)));
    // The old reader still sees an unissued GCS slot, so its existing takeover
    // rule can fence and repair the backup. It never mistakes the marker for a
    // provider receipt or treats this as a two-copy completion.
    assert!(matches!(legacy.b2, LegacyPrimarySlot::Verified { .. }));
    assert!(matches!(legacy.gcs, LegacyPrimarySlot::NotIssued));
    f.objects.lock().unwrap().fail_final_admission_once = Some(503);
    assert!(f
        .storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .is_err());
    let acknowledged = {
        let objects = f.objects.lock().unwrap();
        serde_json::from_slice::<serde_json::Value>(&admission_record(&objects).1).unwrap()
    };
    let fenced: LegacyPrimaryRecord = serde_json::from_value(acknowledged.clone()).unwrap();
    assert!(fenced.attempt.starts_with("b2-only-complete-v1:"));
    assert_ne!(fenced.attempt, legacy.attempt);
    assert!(matches!(fenced.gcs, LegacyPrimarySlot::Acknowledged { .. }));
    assert!(
        !f.storage
            .put("media", Bytes::from_static(b"original"), "video/mp4", None)
            .await
            .unwrap()
            .newly_stored
    );
    let final_record = {
        let objects = f.objects.lock().unwrap();
        serde_json::from_slice::<LegacyPrimaryRecord>(&admission_record(&objects).1).unwrap()
    };
    assert_ne!(final_record.attempt, fenced.attempt);
    assert!(final_record.attempt.starts_with("b2-only-complete-v1:"));
    assert!(matches!(
        final_record.gcs,
        LegacyPrimarySlot::Verified { .. }
    ));
}

#[tokio::test]
async fn backup_recovery_rejects_completed_marker_without_verified_primary() {
    for state in ["not_issued", "issued", "acknowledged"] {
        let f = Fixture::new().await;
        let storage = f.storage.clone().with_gcs_backup_enabled(false);
        storage
            .put("media", Bytes::from_static(b"original"), "video/mp4", None)
            .await
            .unwrap();
        {
            let mut objects = f.objects.lock().unwrap();
            let (_, bytes) = objects.records.values_mut().next().unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            if state == "acknowledged" {
                value["b2"]["state"] = state.into();
            } else {
                value["b2"] = serde_json::json!({"state": state});
            }
            *bytes = Bytes::from(serde_json::to_vec(&value).unwrap());
        }
        let result = storage
            .put("media", Bytes::from_static(b"original"), "video/mp4", None)
            .await;
        assert!(
            result.is_err(),
            "completed marker cannot legitimize {state} primary"
        );
        assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
    }
}

#[tokio::test]
async fn backup_recovery_session_cannot_bypass_uncertain_effect_or_changed_staging() {
    use crate::resumable::{ResumableBackend, ResumableRuntime, UploadSession};
    for failure in [
        "issued-b2",
        "issued-gcs",
        "changed-staging",
        "missing-staging",
    ] {
        let f = Fixture::new().await;
        let hash = hex::encode(Sha256::digest(b"original"));
        let storage = f.storage.clone().with_gcs_backup_enabled(false);
        {
            let mut objects = f.objects.lock().unwrap();
            objects.lose_b2_put_ack = failure == "issued-b2";
            objects.lose_gcs_put_ack = failure == "issued-gcs";
        }
        let initial = storage
            .clone()
            .with_gcs_backup_enabled(failure == "issued-gcs")
            .put(&hash, Bytes::from_static(b"original"), "video/mp4", None)
            .await;
        assert_eq!(initial.is_ok(), failure.ends_with("staging"));
        let runtime =
            ResumableRuntime::new(storage, f.storage.gcs.clone(), "fixture-gcs".into(), None)
                .await
                .unwrap();
        let session = UploadSession {
            processing_outbox_required: Some(false),
            revision: 0,
            chunk_size: 1024,
            upload_id: "up_old".into(),
            owner: "b".repeat(64),
            final_sha256: hash,
            declared_size: 8,
            content_type: "video/mp4".into(),
            file_name: None,
            expires_at_epoch_secs: u64::MAX,
            next_offset: 8,
            session_url: "native-gcs".into(),
            session_token: "fixture".into(),
            temp_object: "__resumable/uploads/up_old/blob".into(),
            storage_complete: true,
            finalized_object: None,
        };
        let (before, writes) = {
            let mut objects = f.objects.lock().unwrap();
            if failure != "missing-staging" {
                objects.staging.insert(
                    session.temp_object.clone(),
                    Bytes::from_static(if failure == "changed-staging" {
                        b"tampered"
                    } else {
                        b"original"
                    }),
                );
            }
            (admission_record(&objects).clone(), objects.writes.clone())
        };
        assert!(
            runtime.backend.finalize(&session).await.is_err(),
            "{failure}"
        );
        let objects = f.objects.lock().unwrap();
        assert_eq!(admission_record(&objects), &before, "{failure}");
        assert_eq!(objects.writes, writes);
    }
}

#[tokio::test]
async fn backup_recovery_native_session_recorded_outbox_keeps_two_copy_contract() {
    use crate::resumable::{
        ResumableBackend, ResumableManager, ResumableRuntime, ResumableUploadInitRequest,
        SessionStore,
    };
    let f = Fixture::new().await;
    let runtime = ResumableRuntime::new(
        f.storage.clone().with_gcs_backup_enabled(false),
        f.storage.gcs.clone(),
        "fixture-gcs".into(),
        None,
    )
    .await
    .unwrap();
    let manager = ResumableManager::new(
        runtime.backend.clone(),
        runtime.store.clone(),
        "https://upload.invalid",
        "https://media.invalid",
        256 * 1024,
        300,
    );
    let owner = "b".repeat(64);
    let hash = hex::encode(Sha256::digest(b"original"));
    let init = manager
        .init_session(
            &owner,
            ResumableUploadInitRequest {
                sha256: hash,
                size: 8,
                content_type: "video/mp4".into(),
                file_name: None,
            },
        )
        .await
        .unwrap();
    manager
        .upload_chunk(
            &init.upload_id,
            Some(&init.required_headers["Authorization"]),
            "bytes 0-7/8",
            Bytes::from_static(b"original"),
        )
        .await
        .unwrap();
    let mut session = runtime.store.load(&init.upload_id).await.unwrap().unwrap();
    session.processing_outbox_required = Some(true);
    assert!(runtime.backend.finalize(&session).await.unwrap());
    assert_eq!(
        runtime
            .backend
            .final_objects(&session)
            .await
            .unwrap()
            .iter()
            .map(|r| r.provider)
            .collect::<Vec<_>>(),
        vec![StorageProvider::B2, StorageProvider::Gcs]
    );
    assert_eq!(f.objects.lock().unwrap().writes, ["b2", "gcs"]);
}

#[tokio::test]
async fn backup_recovery_stale_completion_cas_cannot_land_after_new_attempt() {
    let f = Fixture::new().await;
    let storage = f.storage.clone().with_gcs_backup_enabled(false);
    f.objects.lock().unwrap().fail_completion_once = true;
    assert!(storage
        .put("media", Bytes::from_static(b"original"), "video/mp4", None)
        .await
        .is_err());
    assert!(
        storage
            .put("media", Bytes::from_static(b"original"), "video/mp4", None)
            .await
            .unwrap()
            .newly_stored
    );
    let (stale_generation, stale_payload, current) = {
        let objects = f.objects.lock().unwrap();
        (
            objects.completion_requests[0].0,
            objects.completion_requests[0].1.clone(),
            admission_record(&objects).clone(),
        )
    };
    let mut media = google_cloud_storage::http::objects::upload::Media::new(format!(
        "__resumable/b2-primary-writes/v1/{}.json",
        hex::encode(Sha256::digest(b"media"))
    ));
    media.content_type = "application/json".into();
    let error = storage
        .gcs()
        .unwrap()
        .upload_object(
            &UploadObjectRequest {
                bucket: "fixture-gcs".into(),
                if_generation_match: Some(stale_generation),
                ..Default::default()
            },
            stale_payload,
            &UploadType::Simple(media),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, google_cloud_storage::http::Error::Response(response) if response.code == 412)
    );
    assert_eq!(admission_record(&f.objects.lock().unwrap()), &current);
}

#[tokio::test]
async fn backup_recovery_only_exact_marker_grammar_counts_as_completed() {
    for token in [
        "b2-only-complete-v1:",
        "b2-only-complete-v1:0000000000000000000000000000000",
        "b2-only-complete-v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "b2-only-complete-v2:00000000000000000000000000000000",
        "00000000000000000000000000000000",
    ] {
        let f = Fixture::new().await;
        let storage = f.storage.clone().with_gcs_backup_enabled(false);
        let hash = hex::encode(Sha256::digest(b"original"));
        storage
            .put(&hash, Bytes::from_static(b"original"), "video/mp4", None)
            .await
            .unwrap();
        {
            let mut objects = f.objects.lock().unwrap();
            let (_, bytes) = objects.records.values_mut().next().unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            value["attempt"] = token.into();
            *bytes = Bytes::from(serde_json::to_vec(&value).unwrap());
        }
        assert!(storage.primary_receipts(&hash, 8).await.is_err());
        assert!(
            storage
                .put(&hash, Bytes::from_static(b"original"), "video/mp4", None)
                .await
                .unwrap()
                .newly_stored
        );
        assert!(
            !storage
                .put(&hash, Bytes::from_static(b"original"), "video/mp4", None)
                .await
                .unwrap()
                .newly_stored
        );
        assert_eq!(f.objects.lock().unwrap().writes, ["b2"]);
    }
}

async fn assert_recorded_completed_cache(marked: bool) {
    for backups_enabled in [false, true] {
        for change in [
            "same-size-bytes",
            "owner",
            "mime",
            "wrong-version",
            "latest-changed",
            "backup-missing",
            "backup-corrupt",
            "backup-owner",
            "backup-mime",
        ] {
            let f = Fixture::new().await;
            let key = format!("{}.jpg", "a".repeat(64));
            if marked {
                f.storage
                    .clone()
                    .with_gcs_backup_enabled(false)
                    .put(
                        &key,
                        Bytes::from_static(b"original"),
                        "image/jpeg",
                        Some("fixture-owner"),
                    )
                    .await
                    .unwrap();
            }
            f.storage
                .put(
                    &key,
                    Bytes::from_static(b"original"),
                    "image/jpeg",
                    Some("fixture-owner"),
                )
                .await
                .unwrap();
            let storage = f.storage.clone().with_gcs_backup_enabled(backups_enabled);
            assert_eq!(storage.read_verified(&key).await.unwrap(), "original");
            {
                let mut objects = f.objects.lock().unwrap();
                objects.read_versions.clear();
                match change {
                    "same-size-bytes" => {
                        objects.b2.as_mut().unwrap().bytes = Bytes::from_static(b"tampered")
                    }
                    "owner" => objects.b2.as_mut().unwrap().owner = Some("different-owner".into()),
                    "mime" => objects.b2.as_mut().unwrap().mime = "video/mp4".into(),
                    "wrong-version" => objects.wrong_pinned_b2_head = true,
                    "backup-missing" => objects.gcs = None,
                    "backup-corrupt" => objects.corrupt_gcs = true,
                    "backup-owner" => {
                        objects.gcs.as_mut().unwrap().owner = Some("different-owner".into())
                    }
                    "backup-mime" => objects.gcs.as_mut().unwrap().mime = "video/mp4".into(),
                    _ => objects.latest_b2_version = Some("b2-version-2".into()),
                }
            }
            let result = storage.read_verified(&key).await;
            let expected_ok =
                change == "latest-changed" || (change.starts_with("backup-") && !backups_enabled);
            assert_eq!(
                result.is_ok(),
                expected_ok,
                "marked={marked}, backups={backups_enabled}, change={change}"
            );
            let objects = f.objects.lock().unwrap();
            assert!(
                objects.read_versions.iter().all(|v| v == "b2-version-1"),
                "cache must read admitted version: {change}"
            );
            assert_eq!(objects.writes, ["b2", "gcs"]);
        }
    }
}

#[tokio::test]
async fn recorded_cache_two_copy_completion_verifies_pinned_primary_with_backups_off() {
    assert_recorded_completed_cache(false).await;
}

#[tokio::test]
async fn recorded_cache_repaired_completion_verifies_pinned_primary_with_backups_off() {
    assert_recorded_completed_cache(true).await;
}

#[tokio::test]
async fn recorded_cache_incomplete_admission_cannot_fall_through_to_latest_b2() {
    for incomplete in [
        "b2-verified-unmarked",
        "gcs-issued",
        "gcs-acknowledged",
        "marked-gcs-issued",
        "marked-gcs-acknowledged",
    ] {
        let f = Fixture::new().await;
        let key = format!("{}.jpg", "a".repeat(64));
        let no_backup = f.storage.clone().with_gcs_backup_enabled(false);
        if incomplete.starts_with("marked-") {
            no_backup
                .put(&key, Bytes::from_static(b"original"), "image/jpeg", None)
                .await
                .unwrap();
        }
        {
            let mut objects = f.objects.lock().unwrap();
            objects.fail_verified_b2_head_once = incomplete == "b2-verified-unmarked";
            objects.lose_gcs_put_ack = incomplete.ends_with("issued");
            objects.fail_final_admission_once = incomplete.ends_with("acknowledged").then_some(503);
        }
        let writer = if incomplete == "b2-verified-unmarked" {
            no_backup.clone()
        } else {
            f.storage.clone()
        };
        assert!(writer
            .put(&key, Bytes::from_static(b"original"), "image/jpeg", None)
            .await
            .is_err());
        let before = {
            let objects = f.objects.lock().unwrap();
            (admission_record(&objects).clone(), objects.writes.clone())
        };
        for reader in [no_backup, f.storage.clone()] {
            assert!(reader.read_verified(&key).await.is_err(), "{incomplete}");
        }
        let objects = f.objects.lock().unwrap();
        assert_eq!(admission_record(&objects), &before.0);
        assert_eq!(objects.writes, before.1);
    }
}

#[tokio::test]
async fn recorded_cache_without_admission_retains_legacy_read_behavior() {
    let f = Fixture::new().await;
    f.objects.lock().unwrap().b2 = Some(Copy {
        bytes: Bytes::from_static(b"legacy"),
        owner: None,
        mime: "image/jpeg".into(),
    });
    assert_eq!(
        f.storage
            .clone()
            .with_gcs_backup_enabled(false)
            .read_verified("legacy.jpg")
            .await
            .unwrap(),
        "legacy"
    );
    assert!(f.objects.lock().unwrap().records.is_empty());
    assert!(f.objects.lock().unwrap().writes.is_empty());
}
