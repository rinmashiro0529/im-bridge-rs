use im_bridge::modules::bridge::operations::BridgeOperationStatus;

#[test]
fn bridge_operation_follows_the_committed_state_path() {
    let path = [
        BridgeOperationStatus::Received,
        BridgeOperationStatus::SnapshotReady,
        BridgeOperationStatus::Generating,
        BridgeOperationStatus::Generated,
        BridgeOperationStatus::Committing,
        BridgeOperationStatus::Committed,
        BridgeOperationStatus::Delivered,
    ];
    let mut current = path[0];
    for next in path.into_iter().skip(1) {
        assert!(current.can_transition_to(next));
        current = current.transition(next).unwrap();
    }
    assert_eq!(current, BridgeOperationStatus::Delivered);
    assert!(current.is_terminal());
}

#[test]
fn invalid_operation_jump_and_terminal_reuse_are_rejected() {
    let error = BridgeOperationStatus::Received
        .transition(BridgeOperationStatus::Committed)
        .expect_err("operation cannot skip snapshot and generation");
    assert_eq!(error.code, "BRIDGE_OPERATION_INVALID_TRANSITION");

    assert!(!BridgeOperationStatus::Delivered.can_transition_to(BridgeOperationStatus::Received));
    assert!(!BridgeOperationStatus::Conflict.can_transition_to(BridgeOperationStatus::Failed));
    assert!(BridgeOperationStatus::Conflict.is_terminal());
    assert!(BridgeOperationStatus::Failed.is_terminal());
    assert!(BridgeOperationStatus::Interrupted.is_terminal());
}

#[test]
fn snapshot_ready_can_skip_generation_for_non_generation_mutations() {
    assert!(
        BridgeOperationStatus::SnapshotReady.can_transition_to(BridgeOperationStatus::Generated)
    );
    let next = BridgeOperationStatus::SnapshotReady
        .transition(BridgeOperationStatus::Generated)
        .expect("undo/new may skip generating");
    assert_eq!(next, BridgeOperationStatus::Generated);
}
