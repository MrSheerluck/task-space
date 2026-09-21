use task_core::BoardData;
use task_core::Note;
use task_core::crdt::SpaceDoc;
use task_core::sync::{
    EncodedUpdate, SYNC_DOCUMENT_SCHEMA_VERSION, SYNC_PROTOCOL_VERSION,
    SYNC_RECONCILE_PROTOCOL_VERSION, SpaceMetadataOperation, SyncMetadataRequest, SyncPullRequest,
    SyncReconcileRequest,
};
use task_server::postgres::{PostgresStoreError, PostgresSyncStore};
use uuid::Uuid;

/// Run with:
///
/// TASK_SPACE_TEST_DATABASE_URL=postgres://... \
///   cargo test -p task-server --test postgres_sync -- --ignored --nocapture
///
/// The account is random and is removed at the end, so this can run against a
/// disposable staging database after migrations have been applied.
#[tokio::test]
#[ignore = "requires TASK_SPACE_TEST_DATABASE_URL"]
async fn postgres_sync_round_trip_replay_and_metadata_are_durable() {
    let database_url = std::env::var("TASK_SPACE_TEST_DATABASE_URL")
        .expect("set TASK_SPACE_TEST_DATABASE_URL for the PostgreSQL smoke test");
    let store = PostgresSyncStore::connect(&database_url)
        .await
        .expect("PostgreSQL should be reachable");
    store.migrate().await.expect("migrations should apply");

    let account_id = format!("sync-smoke-{}", Uuid::new_v4());
    let space_id = 9_000_000_000_000_u64;
    let stable_space_id = Uuid::new_v4().to_string();
    let cleanup = || async {
        sqlx::query("DELETE FROM spaces WHERE account_id = $1")
            .bind(&account_id)
            .execute(store.pool())
            .await
            .expect("smoke account should be removable");
    };

    store
        .register_space(
            &account_id,
            space_id,
            Some(&stable_space_id),
            "sync smoke",
            100,
        )
        .await
        .expect("space registration should work against the migrated schema");

    // A metadata event may precede a document reconcile. The reconcile
    // response's advisory sequence must include it for diagnostics, but the
    // browser must not treat that sequence as an applied SSE cursor because
    // the response does not contain the metadata payload.
    let pre_reconcile_metadata = SyncMetadataRequest {
        protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
        space_id,
        stable_space_id: Some(stable_space_id.clone()),
        operation_id: format!("pre-reconcile-{}", Uuid::new_v4()),
        operation: SpaceMetadataOperation::Rename,
        name: Some("metadata before document".into()),
        expected_version: Some(0),
    };
    store
        .apply_metadata(&account_id, &pre_reconcile_metadata)
        .await
        .expect("metadata before reconcile should commit");
    let pre_reconcile_event_id: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(event_id), 0) FROM sync_events WHERE account_id = $1",
    )
    .bind(&account_id)
    .fetch_one(store.pool())
    .await
    .expect("metadata event should be durable");

    let source = SpaceDoc::new();
    source.import_board(&BoardData {
        notes: vec![Note {
            id: 1,
            text: "durable postgres sync".into(),
            ..Default::default()
        }],
        ..Default::default()
    });
    let empty = SpaceDoc::new();
    let update = source
        .encode_update(&empty.state_vector())
        .expect("source update should encode");
    let request = SyncReconcileRequest {
        protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
        document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
        space_id,
        stable_space_id: Some(stable_space_id.clone()),
        mutation_id: format!("mutation-{}", Uuid::new_v4()),
        device_id: format!("device-{}", Uuid::new_v4()),
        local_generation: 1,
        last_server_sequence: 0,
        state_vector: EncodedUpdate::from_bytes(&empty.state_vector()),
        update: EncodedUpdate::from_bytes(&update),
    };

    let first = store
        .reconcile(&account_id, &request)
        .await
        .expect("first reconcile should commit");
    assert!(first.accepted);
    assert!(first.event_id.is_some());
    assert!(first.event_cursor >= pre_reconcile_event_id as u64);

    let retry = store
        .reconcile(&account_id, &request)
        .await
        .expect("identical reconcile retry should be idempotent");
    assert!(retry.accepted);
    assert_eq!(retry.event_id, None);
    let mut reused_with_different_payload = request.clone();
    reused_with_different_payload.update = EncodedUpdate::from_bytes(
        &source
            .encode_update(&source.state_vector())
            .expect("empty Yrs update should encode"),
    );
    assert!(matches!(
        store
            .reconcile(&account_id, &reused_with_different_payload)
            .await,
        Err(PostgresStoreError::MutationIdReused)
    ));

    // A new mutation id may repeat an update that the server already knows.
    // It must still be claimed for idempotency/conflict protection, but must
    // not create another replay row or SSE event.
    let mut repeated_noop = request.clone();
    repeated_noop.mutation_id = format!("repeated-noop-{}", Uuid::new_v4());
    repeated_noop.local_generation = 2;
    let repeated_noop_result = store
        .reconcile(&account_id, &repeated_noop)
        .await
        .expect("repeated known update should reconcile as a no-op");
    assert!(repeated_noop_result.accepted);
    assert_eq!(repeated_noop_result.event_id, None);

    let claims_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sync_mutation_claims
         WHERE account_id = $1 AND space_id = $2 AND mutation_kind = 'reconcile'",
    )
    .bind(&account_id)
    .bind(space_id as i64)
    .fetch_one(store.pool())
    .await
    .expect("mutation claims should be queryable");
    let updates_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM crdt_updates
         WHERE account_id = $1 AND space_id = $2 AND event_kind = 'document'",
    )
    .bind(&account_id)
    .bind(space_id as i64)
    .fetch_one(store.pool())
    .await
    .expect("CRDT updates should be queryable");
    assert_eq!(claims_before, 2);
    assert_eq!(updates_before, 1);
    let mut invalid_request = request.clone();
    invalid_request.mutation_id = format!("invalid-{}", Uuid::new_v4());
    invalid_request.update = EncodedUpdate::from_bytes(&[0xff, 0x00, 0x7f]);
    assert!(matches!(
        store.reconcile(&account_id, &invalid_request).await,
        Err(PostgresStoreError::InvalidUpdate(_))
    ));
    let claims_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sync_mutation_claims
         WHERE account_id = $1 AND space_id = $2 AND mutation_kind = 'reconcile'",
    )
    .bind(&account_id)
    .bind(space_id as i64)
    .fetch_one(store.pool())
    .await
    .expect("mutation claims should remain queryable after rollback");
    let updates_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM crdt_updates
         WHERE account_id = $1 AND space_id = $2 AND event_kind = 'document'",
    )
    .bind(&account_id)
    .bind(space_id as i64)
    .fetch_one(store.pool())
    .await
    .expect("CRDT updates should remain queryable after rollback");
    assert_eq!(claims_after, claims_before);
    assert_eq!(updates_after, updates_before);

    let pull = store
        .pull(
            &account_id,
            &SyncPullRequest {
                protocol_version: SYNC_PROTOCOL_VERSION,
                document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
                space_id,
                stable_space_id: Some(stable_space_id.clone()),
                last_server_sequence: 0,
                state_vector: EncodedUpdate::from_bytes(&empty.state_vector()),
                local_generation: 0,
            },
        )
        .await
        .expect("pull should return the durable snapshot delta");
    let pulled = SpaceDoc::from_update(&pull.update.to_bytes().unwrap())
        .expect("pulled delta should decode");
    assert_eq!(pulled.board().notes[0].text, "durable postgres sync");

    // A fresh edit must reset the compaction-idle window even when an older
    // compaction marker is present. This guards the retention query against
    // compacting an actively edited document.
    sqlx::query(
        "UPDATE crdt_documents
         SET compacted_at = NOW() - INTERVAL '2 hours'
         WHERE account_id = $1 AND space_id = $2",
    )
    .bind(&account_id)
    .bind(space_id as i64)
    .execute(store.pool())
    .await
    .expect("smoke document should be mutable");
    let previous_board = source.board();
    let mut changed_board = previous_board.clone();
    changed_board.notes[0].text = "fresh edit after compaction".into();
    let changed = SpaceDoc::from_update(&source.snapshot()).expect("source snapshot should load");
    changed.apply_board_diff(&previous_board, &changed_board, 123);
    let second_request = SyncReconcileRequest {
        mutation_id: format!("mutation-{}", Uuid::new_v4()),
        local_generation: 2,
        state_vector: EncodedUpdate::from_bytes(&source.state_vector()),
        update: EncodedUpdate::from_bytes(
            &changed
                .encode_update(&source.state_vector())
                .expect("second update should encode"),
        ),
        ..request.clone()
    };
    store
        .reconcile(&account_id, &second_request)
        .await
        .expect("fresh edit should reconcile");
    let _compacted = store
        .compact_documents(60 * 60, 0, 100)
        .await
        .expect("compaction should run");
    let still_old: bool = sqlx::query_scalar(
        "SELECT compacted_at < NOW() - INTERVAL '1 hour'
         FROM crdt_documents
         WHERE account_id = $1 AND space_id = $2",
    )
    .bind(&account_id)
    .bind(space_id as i64)
    .fetch_one(store.pool())
    .await
    .expect("smoke document should remain queryable");
    assert!(
        still_old,
        "recently edited document must not be compacted from an old marker"
    );

    let other_device = SpaceDoc::new();
    other_device.import_board(&BoardData {
        notes: vec![Note {
            id: 2,
            text: "offline device merge".into(),
            ..Default::default()
        }],
        ..Default::default()
    });
    let other_request = SyncReconcileRequest {
        mutation_id: format!("mutation-{}", Uuid::new_v4()),
        device_id: format!("device-{}", Uuid::new_v4()),
        local_generation: 1,
        state_vector: EncodedUpdate::from_bytes(&empty.state_vector()),
        update: EncodedUpdate::from_bytes(
            &other_device
                .encode_update(&empty.state_vector())
                .expect("second device update should encode"),
        ),
        ..request.clone()
    };
    store
        .reconcile(&account_id, &other_request)
        .await
        .expect("second offline device should reconcile");
    let merged_pull = store
        .pull(
            &account_id,
            &SyncPullRequest {
                protocol_version: SYNC_PROTOCOL_VERSION,
                document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
                space_id,
                stable_space_id: Some(stable_space_id.clone()),
                last_server_sequence: 0,
                state_vector: EncodedUpdate::from_bytes(&empty.state_vector()),
                local_generation: 0,
            },
        )
        .await
        .expect("merged pull should return both device changes");
    let merged = SpaceDoc::from_update(&merged_pull.update.to_bytes().unwrap())
        .expect("merged pull should decode");
    assert!(
        merged
            .board()
            .notes
            .iter()
            .any(|note| note.text == "fresh edit after compaction")
    );
    assert!(
        merged
            .board()
            .notes
            .iter()
            .any(|note| note.text == "offline device merge")
    );

    let events = store
        .events_since(&account_id, 0)
        .await
        .expect("durable event replay should return the reconcile event");
    assert_eq!(events.len(), 4);
    assert_eq!(
        events[0].stable_space_id.as_deref(),
        Some(stable_space_id.as_str())
    );

    let metadata_request = SyncMetadataRequest {
        protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
        space_id,
        stable_space_id: Some(stable_space_id.clone()),
        operation_id: format!("rename-{}", Uuid::new_v4()),
        operation: SpaceMetadataOperation::Rename,
        name: Some("renamed smoke".into()),
        expected_version: Some(1),
    };
    let metadata = store
        .apply_metadata(&account_id, &metadata_request)
        .await
        .expect("metadata mutation should commit");
    assert_eq!(metadata.name, "renamed smoke");
    assert_eq!(metadata.metadata_version, 2);

    // Same-version metadata operations must use the raw operation id as the
    // deterministic tie-breaker. A lower id loses without changing the row;
    // a greater id wins even though both requests were based on version 1.
    let losing_metadata = SyncMetadataRequest {
        operation_id: "a-loser".into(),
        name: Some("should not win".into()),
        expected_version: Some(1),
        ..metadata_request.clone()
    };
    assert!(matches!(
        store.apply_metadata(&account_id, &losing_metadata).await,
        Err(PostgresStoreError::MetadataConflictSuperseded)
    ));
    let winning_metadata = SyncMetadataRequest {
        operation_id: "z-winner".into(),
        name: Some("deterministic winner".into()),
        expected_version: Some(1),
        ..metadata_request.clone()
    };
    let winner = store
        .apply_metadata(&account_id, &winning_metadata)
        .await
        .expect("greater metadata operation id should win");
    assert_eq!(winner.name, "deterministic winner");
    assert_eq!(winner.metadata_version, 3);

    let mismatch = store
        .pull(
            &account_id,
            &SyncPullRequest {
                protocol_version: SYNC_PROTOCOL_VERSION,
                document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
                space_id,
                stable_space_id: Some(Uuid::new_v4().to_string()),
                last_server_sequence: 0,
                state_vector: EncodedUpdate::from_bytes(&empty.state_vector()),
                local_generation: 0,
            },
        )
        .await;
    assert!(matches!(
        mismatch,
        Err(PostgresStoreError::SpaceAccessDenied)
    ));

    store
        .prune_history(0)
        .await
        .expect("history retention should prune the replay log");
    assert!(matches!(
        store
            .events_since(&account_id, first.event_id.unwrap())
            .await,
        Err(PostgresStoreError::EventCursorRequiresReset)
    ));
    assert!(matches!(
        store
            .reconcile(&account_id, &reused_with_different_payload)
            .await,
        Err(PostgresStoreError::MutationIdReused)
    ));
    let metadata_retry = store
        .apply_metadata(&account_id, &metadata_request)
        .await
        .expect("metadata retry should survive replay-history pruning");
    assert_eq!(metadata_retry.name, "renamed smoke");
    assert_eq!(metadata_retry.metadata_version, 2);

    cleanup().await;
}
