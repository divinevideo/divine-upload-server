# Divine Upload Server Resumable Contract Alignment Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align `divine-upload-server` with the approved Divine resumable upload contract so `divine-blossom` can proxy the public control plane without reshaping data-plane responses.

**Architecture:** Keep this repo as the resumable data plane on `upload.divine.video` and leave client-facing `HEAD /upload`, `POST /upload/init`, and `POST /upload/{uploadId}/complete` ownership with `divine-blossom`. In this repo, align JSON field names, expiry formatting, session headers, and completion payload shape to the approved contract while preserving legacy single-shot upload behavior.

**Tech Stack:** Rust, Axum, Serde, Tokio, Google Cloud Storage, Python unittest

---

**Scope split:** This plan covers `divine-upload-server` only. `divine-blossom` still needs a separate implementation plan for the public control-plane routes on `media.divine.video`.

**File Structure**

- `src/resumable.rs`
  - owns resumable request and response models, expiry formatting, session state, and completion metadata
- `src/main.rs`
  - owns HTTP headers and handler responses for session `HEAD` and `PUT`
- `src/landing.html`
  - owns the operator-facing endpoint description for the upload host
- `README.md`
  - owns the repo boundary and deployment contract documentation
- `tests/test_export_video_upload_hashes.py`
  - unchanged behavior safety net for Python tooling

## Chunk 1: Contract Models And Expiry Format

### Task 1: Make resumable init request and response JSON match the approved contract

**Files:**
- Modify: `src/resumable.rs`
- Test: `src/resumable.rs`

- [ ] **Step 1: Write the failing serialization tests**

```rust
#[test]
fn init_contract_request_accepts_camel_case_fields() {
    let payload = serde_json::json!({
        "sha256": "abc",
        "size": 12,
        "contentType": "video/mp4",
        "fileName": "clip.mp4"
    });

    let request: ResumableUploadInitRequest = serde_json::from_value(payload)
        .expect("camelCase init request should deserialize");

    assert_eq!(request.content_type, "video/mp4");
    assert_eq!(request.file_name.as_deref(), Some("clip.mp4"));
}

#[test]
fn init_contract_response_serializes_camel_case_fields() {
    let response = ResumableUploadInitResponse {
        upload_id: "up_123".to_string(),
        upload_url: "https://upload.divine.video/sessions/up_123".to_string(),
        expires_at: "2026-03-28T04:00:00Z".to_string(),
        chunk_size: 8 * 1024 * 1024,
        next_offset: 0,
        required_headers: std::collections::HashMap::new(),
        capabilities: ResumableCapabilities {
            resume: true,
            query_offset: true,
        },
    };

    let json = serde_json::to_value(response).expect("serialize response");

    assert!(json.get("uploadId").is_some());
    assert!(json.get("uploadUrl").is_some());
    assert!(json.get("expiresAt").is_some());
    assert!(json.get("chunkSize").is_some());
    assert!(json.get("nextOffset").is_some());
    assert!(json.get("capabilities").is_some());
    assert!(json.get("upload_id").is_none());
}
```

- [ ] **Step 2: Run the targeted Rust tests to verify they fail**

Run: `cargo test init_contract_ -- --nocapture`
Expected: FAIL because the current models only expose `snake_case` fields and have no `capabilities` object.

- [ ] **Step 3: Implement the minimal model changes**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumableUploadInitRequest {
    pub sha256: String,
    pub size: u64,
    #[serde(alias = "content_type")]
    pub content_type: String,
    #[serde(alias = "file_name")]
    pub file_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumableCapabilities {
    pub resume: bool,
    pub query_offset: bool,
}
```

- [ ] **Step 4: Run the targeted Rust tests to verify they pass**

Run: `cargo test init_contract_ -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit the contract model changes**

```bash
git add src/resumable.rs
git commit -m "feat: align resumable init payloads with divine contract"
```

### Task 2: Emit RFC 3339 expiries for init and session status

**Files:**
- Modify: `src/resumable.rs`
- Test: `src/resumable.rs`

- [ ] **Step 1: Write the failing expiry-format tests**

```rust
#[test]
fn rfc3339_expiry_helper_formats_contract_timestamp() {
    let formatted = format_epoch_secs_as_rfc3339(1_774_660_000);
    assert_eq!(formatted, "2026-03-28T00:40:00Z");
}

#[tokio::test]
async fn rfc3339_expiry_is_used_in_session_status() {
    let manager = manager();
    let response = manager
        .init_session(
            "owner_pubkey",
            ResumableUploadInitRequest {
                sha256: "5b48aa1fcf30af61243ac9307eb98b7fa22df1c58573c3ca5d1b14fc30099929"
                    .to_string(),
                size: 1024 * 1024,
                content_type: "video/mp4".to_string(),
                file_name: None,
            },
        )
        .await
        .expect("init response");
    let auth = response.required_headers.get("Authorization").unwrap().to_string();
    let head = manager
        .head_session(&response.upload_id, Some(&auth))
        .await
        .expect("head session");

    assert!(response.expires_at.ends_with('Z'));
    assert!(head.expires_at.ends_with('Z'));
}
```

- [ ] **Step 2: Run the targeted Rust tests to verify they fail**

Run: `cargo test rfc3339_expiry_ -- --nocapture`
Expected: FAIL because the current code emits epoch-second strings.

- [ ] **Step 3: Implement the minimal formatting helper and response changes**

```rust
fn format_epoch_secs_as_rfc3339(epoch_secs: u64) -> String {
    time::OffsetDateTime::from_unix_timestamp(epoch_secs as i64)
        .expect("valid epoch seconds")
        .format(&time::format_description::well_known::Rfc3339)
        .expect("rfc3339 timestamp")
}
```

- [ ] **Step 4: Run the targeted Rust tests to verify they pass**

Run: `cargo test rfc3339_expiry_ -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit the expiry-format changes**

```bash
git add src/resumable.rs Cargo.toml Cargo.lock
git commit -m "feat: emit rfc3339 resumable expiry timestamps"
```

## Chunk 2: Session Headers And Completion Response

### Task 3: Add the approved session expiry header to session `HEAD` and chunk `PUT` responses

**Files:**
- Modify: `src/resumable.rs`
- Modify: `src/main.rs`
- Test: `src/main.rs`

- [ ] **Step 1: Write the failing header test**

```rust
#[test]
fn session_responses_include_upload_expires_at_header() {
    let response = build_session_status_response(UploadSessionStatus {
        next_offset: 0,
        declared_size: 1024,
        expires_at: "2026-03-28T00:40:00Z".to_string(),
        chunk_size: 8 * 1024 * 1024,
    });

    assert_eq!(
        response.headers().get("Upload-Expires-At").unwrap(),
        "2026-03-28T00:40:00Z"
    );
}
```

- [ ] **Step 2: Run the targeted Rust test to verify it fails**

Run: `cargo test session_responses_include_upload_expires_at_header -- --nocapture`
Expected: FAIL because the current responses only emit `Upload-Expires`.

- [ ] **Step 3: Implement the minimal header helper and wire both handlers through it**

```rust
const SESSION_EXPIRES_AT_HEADER: &str = "Upload-Expires-At";
```

- [ ] **Step 4: Run the targeted Rust test to verify it passes**

Run: `cargo test session_responses_include_upload_expires_at_header -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit the session-header changes**

```bash
git add src/main.rs src/resumable.rs
git commit -m "feat: advertise upload session expiry with contract header"
```

### Task 4: Return a mobile-compatible completion descriptor from resumable `complete`

**Files:**
- Modify: `src/resumable.rs`
- Modify: `src/main.rs`
- Test: `src/resumable.rs`

- [ ] **Step 1: Write the failing completion-shape test**

```rust
#[test]
fn complete_response_serializes_public_descriptor_fields() {
    let response = CompleteUploadResponse {
        url: "https://media.divine.video/abc".to_string(),
        fallback_url: Some("https://media.divine.video/abc".to_string()),
        thumbnail: Some("https://media.divine.video/abc.jpg".to_string()),
        streaming: Some(StreamingInfo {
            hls_url: None,
            mp4_url: None,
            thumbnail_url: Some("https://media.divine.video/abc.jpg".to_string()),
            status: Some("processing".to_string()),
        }),
    };

    let json = serde_json::to_value(response).expect("serialize complete response");

    assert_eq!(json.get("url").unwrap(), "https://media.divine.video/abc");
    assert_eq!(json.get("fallbackUrl").unwrap(), "https://media.divine.video/abc");
    assert!(json.get("streaming").is_some());
}
```

- [ ] **Step 2: Run the targeted Rust test to verify it fails**

Run: `cargo test complete_response_serializes_public_descriptor_fields -- --nocapture`
Expected: FAIL because the current completion response still exposes internal blob metadata fields.

- [ ] **Step 3: Implement the minimal completion response rewrite**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamingInfo {
    pub hls_url: Option<String>,
    pub mp4_url: Option<String>,
    pub thumbnail_url: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompleteUploadResponse {
    pub url: String,
    pub fallback_url: Option<String>,
    pub thumbnail: Option<String>,
    pub streaming: Option<StreamingInfo>,
}
```

- [ ] **Step 4: Run the targeted Rust test to verify it passes**

Run: `cargo test complete_response_serializes_public_descriptor_fields -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit the completion response changes**

```bash
git add src/resumable.rs src/main.rs
git commit -m "feat: return public completion descriptor for resumable uploads"
```

## Chunk 3: Docs, Landing Page, And Full Verification

### Task 5: Update docs so the upload host no longer claims the public control-plane role

**Files:**
- Modify: `README.md`
- Modify: `src/landing.html`

- [ ] **Step 1: Write the wording changes**

```text
Control plane: media.divine.video
Data plane: upload.divine.video
Opaque session URLs are server-issued and resumable-session specific.
```

- [ ] **Step 2: Review the rendered copy in the edited files**

Run: `sed -n '1,220p' README.md && sed -n '90,150p' src/landing.html`
Expected: Both files clearly describe `divine-blossom` as the public control plane and `divine-upload-server` as the opaque session data plane.

- [ ] **Step 3: Commit the doc updates**

```bash
git add README.md src/landing.html
git commit -m "docs: clarify divine upload control-plane boundary"
```

### Task 6: Run the full verification suite

**Files:**
- Modify: none

- [ ] **Step 1: Check formatting**

Run: `cargo fmt --all -- --check`
Expected: PASS

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: PASS

- [ ] **Step 3: Run Rust tests**

Run: `cargo test --all`
Expected: PASS

- [ ] **Step 4: Run Python tests**

Run: `python3 -m unittest discover -s tests -p 'test_*.py'`
Expected: PASS

- [ ] **Step 5: Commit any final fixups if verification required them**

```bash
git add src/resumable.rs src/main.rs README.md src/landing.html Cargo.toml Cargo.lock
git commit -m "chore: finalize resumable contract alignment"
```
