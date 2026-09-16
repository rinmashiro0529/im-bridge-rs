use im_bridge::modules::telegram::panel::{plan_effect, render_home, PanelKind, PanelSlot};

#[test]
fn panel_slot_replacement_keeps_message_id_across_50_page_changes() {
    let mut slot = PanelSlot {
        panel_id: "panel-1".into(),
        account_id: "account-1".into(),
        numeric_bot_id: 42,
        chat_id: 7,
        message_id: 9001,
        kind: PanelKind::Home,
        revision: 0,
        active: true,
        expires_at_unix: 1,
    };
    for page in 0..50 {
        let panel = render_home();
        let (effect, next) = plan_effect(
            Some(&slot),
            panel,
            slot.panel_id.clone(),
            slot.account_id.clone(),
            slot.numeric_bot_id,
            slot.chat_id,
            60,
        );
        assert!(matches!(
            effect,
            im_bridge::modules::telegram::panel::PanelEffect::Replace { .. }
        ));
        assert_eq!(
            next.message_id, 9001,
            "page {page} replaced the panel message"
        );
        assert_eq!(next.revision, slot.revision + 1);
        slot = next;
    }
}

#[test]
fn stale_panel_revision_is_rejected_by_the_planner_contract() {
    let slot = PanelSlot {
        panel_id: "panel-1".into(),
        account_id: "account-1".into(),
        numeric_bot_id: 42,
        chat_id: 7,
        message_id: 9001,
        kind: PanelKind::Home,
        revision: 4,
        active: true,
        expires_at_unix: 1,
    };
    let (_, next) = plan_effect(
        Some(&slot),
        render_home(),
        slot.panel_id.clone(),
        slot.account_id.clone(),
        slot.numeric_bot_id,
        slot.chat_id,
        60,
    );
    assert_eq!(next.revision, 5);
    assert_eq!(next.message_id, slot.message_id);
}
