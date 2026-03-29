Status: Approved

# Divine Upload Server Resumable Contract Alignment Design

**Problem**

`divine-upload-server` is live on `upload.divine.video`, but the public resumable upload contract approved for the mobile client is defined around a control plane on `media.divine.video` and an opaque data plane on `upload.divine.video`. Today the live services do not line up with that contract: `divine-blossom` is not advertising or serving the resumable control-plane routes, and this repo still exposes several draft-era data-plane mismatches in JSON shape, expiry format, and completion response shape.

**Goals**

- Keep `media.divine.video` as the canonical client-facing Blossom control plane.
- Keep `upload.divine.video` as the opaque resumable session data plane.
- Make this repo's resumable payloads and headers line up with the approved Divine resumable session draft closely enough that `divine-blossom` can proxy them without lossy translation.
- Preserve legacy single-shot `PUT /upload` behavior.
- Keep session uploads non-public and only publish the final verified blob.

**Non-Goals**

- Move `HEAD /upload`, `POST /upload/init`, or `POST /upload/{uploadId}/complete` ownership from `divine-blossom` to this repo's public responsibility.
- Teach the mobile client a tus-style or upload-host-specific protocol.
- Re-architect the existing single-shot upload path in this pass.
- Solve the entire cross-repo rollout inside this repository alone.

**Approved Direction**

We will keep the draft Divine resumable contract as the source of truth.

- `divine-blossom` remains responsible for:
  - `HEAD /upload` capability discovery on `media.divine.video`
  - client-facing `POST /upload/init`
  - client-facing `POST /upload/{uploadId}/complete`
  - client-facing `DELETE /upload/{uploadId}`
- `divine-upload-server` remains responsible for:
  - creating and tracking resumable upload sessions
  - accepting `PUT` and `HEAD` on opaque session URLs under `upload.divine.video`
  - finalizing verified uploads into canonical storage
  - returning completion metadata that `divine-blossom` can proxy with minimal or no shape translation

**Why This Direction**

Option 1 is the only direction that matches the approved mobile spec in `divine-mobile` and preserves the intended Blossom boundary. Treating the currently live upload host as the protocol source of truth would force a mobile protocol fork, invalidate the March 26 design and plan, and couple the client to an unfinished backend shape rather than to the intended Divine contract.

**Current State**

- `README.md` already documents this repo as the data plane behind `upload.divine.video`, with `divine-blossom` owning `HEAD /upload` and the control-plane proxy role.
- `src/main.rs` already implements:
  - `POST /upload/init`
  - `POST /upload/:upload_id/complete`
  - `DELETE /upload/:upload_id`
  - `PUT /sessions/:upload_id`
  - `HEAD /sessions/:upload_id`
- Live probes on March 28, 2026 show:
  - `HEAD https://media.divine.video/upload` returns `200` without Divine capability headers
  - `POST https://media.divine.video/upload/init` returns `404`
  - `HEAD https://upload.divine.video/upload` returns `405` with `Allow: PUT,OPTIONS`

**Observed Gaps In This Repo**

1. Request and response JSON are still Rust-style `snake_case`, while the approved contract is `camelCase`.
2. Expiry values are emitted as raw epoch-second strings, while the approved contract uses RFC 3339 timestamps.
3. Session progress headers currently use `Upload-Expires`; the approved contract calls for `Upload-Expires-At`.
4. `POST /upload/{uploadId}/complete` returns internal blob metadata, not the public-facing Blossom descriptor shape expected by the mobile upload service.
5. The landing page overstates public endpoint ownership and does not explain the control-plane/data-plane split clearly enough.

**Design**

## 1. Keep The Repo Boundary Intact

This repo will not become the public control plane. The public client contract still lives on `media.divine.video`, served by `divine-blossom`.

Inside that split, this repo should make its resumable handler contract proxy-friendly:

- accept the same field names the public control plane expects
- return the same field names the public control plane wants to forward
- expose the same session progress headers the mobile client is built against

That reduces duplicated translation logic and lowers the risk of drift between repos.

## 2. Align Init Payloads With The Draft Contract

`ResumableUploadInitRequest` and `ResumableUploadInitResponse` in `src/resumable.rs` should become `camelCase` at the JSON boundary:

- request:
  - `sha256`
  - `size`
  - `contentType`
  - `fileName`
- response:
  - `uploadId`
  - `uploadUrl`
  - `expiresAt`
  - `chunkSize`
  - `nextOffset`
  - `requiredHeaders`
  - `capabilities`

Compatibility detail:

- accept legacy `content_type` and `file_name` as aliases during the transition so existing internal callers do not break accidentally
- emit only the approved `camelCase` names in responses

## 3. Emit RFC 3339 Expiry Values

The approved contract examples use timestamps such as `2026-03-26T15:00:00Z`, not epoch strings.

This repo should therefore:

- return `expiresAt` as RFC 3339 in init responses
- return `Upload-Expires-At` as RFC 3339 in session `HEAD` and chunk `PUT` responses

For rollout safety, the data plane may also keep emitting `Upload-Expires` temporarily as a compatibility header if doing so is cheap, but the approved contract header must be present and tested.

## 4. Return A Public Descriptor Shape From Complete

The completion handler should return a response shaped for the existing mobile parser:

- top-level `url`
- top-level `fallbackUrl`
- optional top-level `thumbnail`
- optional `streaming` object with:
  - `hlsUrl`
  - `mp4Url`
  - `thumbnailUrl` or `thumbnail`
  - `status`

For this repo's first alignment pass:

- `fallbackUrl` should point at the canonical media URL on `media.divine.video`
- `url` should also be set so existing clients always have a primary URL
- `streaming.status` may be `"processing"` when transcoding has been triggered but stream assets are not yet known
- `thumbnail` should reuse the existing thumbnail generation logic when available
- `dim` and other internal metadata can remain available internally, but the proxied completion contract should prioritize the public descriptor shape

This keeps the response consumable by the current mobile upload service without forcing `divine-blossom` to manufacture fields it does not own.

## 5. Clarify Docs And Operator Surfaces

`README.md` and `src/landing.html` should be updated so they stop implying that clients discover resumable capability on the upload host.

They should state clearly:

- `media.divine.video` is the control plane
- `upload.divine.video` is the opaque session data plane
- session URLs are for server-issued resumable uploads only

That makes the live service behavior easier to reason about during rollout and reduces confusion from probe-based debugging.

**File Boundaries**

- `src/resumable.rs`
  - request/response schema alignment
  - expiry formatting helpers
  - completion response contract shape
  - unit tests for serialization and completion metadata
- `src/main.rs`
  - resumable response headers
  - handler wiring if response structs change
  - route-level tests where needed
- `README.md`
  - public ownership clarification
- `src/landing.html`
  - endpoint and control-plane/data-plane wording cleanup

**Verification**

- Rust unit tests in `src/resumable.rs` for:
  - camelCase request parsing
  - camelCase init response serialization
  - RFC 3339 expiry formatting
  - completion response shape
- Rust tests in `src/main.rs` or extracted helpers for:
  - `Upload-Expires-At` presence on session responses
  - compatibility header behavior if retained
- Full repo verification:
  - `cargo fmt --all -- --check`
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test --all`
  - `python3 -m unittest discover -s tests -p 'test_*.py'`

**Cross-Repo Dependency**

This repository alone cannot satisfy the mobile resumable contract.

Separate `divine-blossom` work is still required to:

- advertise capability headers on `HEAD /upload`
- expose public `init`, `complete`, and abort routes on `media.divine.video`
- proxy those routes to this service without distorting the approved contract

That work should be tracked in a separate spec and implementation plan in the `divine-blossom` repository.

**Recommendation**

Align this repo to the approved Divine resumable draft now, then land the matching `divine-blossom` control-plane proxy work. That preserves the March 26 mobile contract and turns the current production gap into a deployment-alignment problem instead of a client-protocol rewrite.
