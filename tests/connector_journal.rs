use im_bridge::modules::bridge::connector_journal::{
    ConnectorJournalRecord, ConnectorJournalStatus, JournalError, MemoryConnectorJournal,
    ReplayOutcome,
};

fn prepared(
    operation_id: &str,
    locator_hash: &str,
    mutation_digest: &str,
) -> ConnectorJournalRecord {
    ConnectorJournalRecord {
        operation_id: operation_id.to_string(),
        locator_hash: locator_hash.to_string(),
        mutation_digest: mutation_digest.to_string(),
        status: ConnectorJournalStatus::Prepared,
        before_sha256: None,
        after_sha256: None,
        before_integrity: None,
        after_integrity: None,
    }
}

#[test]
fn prepare_applied_replay_same_triple_is_already_applied() {
    let mut journal = MemoryConnectorJournal::default();
    journal
        .prepare(prepared("op-1", "loc-a", "mut-a"))
        .expect("prepare");
    let status = journal
        .mark_applied("op-1", "after-sha".to_string(), "after-int".to_string())
        .expect("apply");
    assert_eq!(status, ConnectorJournalStatus::Applied);
    let outcome = journal
        .replay("op-1", "loc-a", "mut-a")
        .expect("replay matching triple");
    assert_eq!(outcome, ReplayOutcome::AlreadyApplied);
}

#[test]
fn replay_with_different_mutation_digest_is_operation_id_reused() {
    let mut journal = MemoryConnectorJournal::default();
    journal
        .prepare(prepared("op-2", "loc-a", "mut-a"))
        .expect("prepare");
    let error = journal
        .replay("op-2", "loc-a", "mut-other")
        .expect_err("digest mismatch must reuse");
    assert_eq!(error, JournalError::OperationIdReused);
}

#[test]
fn prepared_can_become_unknown_but_applied_cannot() {
    let mut journal = MemoryConnectorJournal::default();
    journal
        .prepare(prepared("op-3", "loc-a", "mut-a"))
        .expect("prepare");
    journal.mark_unknown("op-3").expect("prepared -> unknown");
    assert_eq!(
        journal.get("op-3").map(|record| record.status),
        Some(ConnectorJournalStatus::Unknown)
    );

    journal
        .prepare(prepared("op-4", "loc-a", "mut-a"))
        .expect("prepare applied path");
    journal
        .mark_applied("op-4", "after-sha".to_string(), "after-int".to_string())
        .expect("apply");
    let error = journal
        .mark_unknown("op-4")
        .expect_err("applied cannot become unknown");
    assert_eq!(error, JournalError::InvalidTransition);
}

#[test]
fn applied_cannot_become_failed() {
    let mut journal = MemoryConnectorJournal::default();
    journal
        .prepare(prepared("op-5", "loc-a", "mut-a"))
        .expect("prepare");
    journal
        .mark_applied("op-5", "after-sha".to_string(), "after-int".to_string())
        .expect("apply");
    let error = journal
        .mark_failed("op-5")
        .expect_err("applied cannot fail");
    assert_eq!(error, JournalError::InvalidTransition);
}

#[test]
fn replay_of_missing_operation_is_not_found() {
    let mut journal = MemoryConnectorJournal::default();
    let outcome = journal
        .replay("missing", "loc-a", "mut-a")
        .expect("missing is an outcome");
    assert_eq!(outcome, ReplayOutcome::NotFound);
}
