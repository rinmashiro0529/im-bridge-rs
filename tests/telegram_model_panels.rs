use im_bridge::domain::st::StModelSummary;
use im_bridge::modules::telegram::callback::{parse_callback_data, CallbackAction};
use im_bridge::modules::telegram::panel::{render_provider_models, render_providers, ModelPurpose};

#[test]
fn providers_are_rendered_in_two_levels_and_callback_data_stays_within_telegram_limit() {
    let models = (0..31)
        .map(|index| StModelSummary {
            id: format!(
                "model-{index}-{}",
                "x".repeat(if index == 0 { 80 } else { 2 })
            ),
            owned_by: if index == 1 {
                None
            } else if index % 2 == 0 {
                Some("openai-compatible".into())
            } else {
                Some("local".into())
            },
        })
        .collect::<Vec<_>>();
    let providers = vec!["local".into(), "openai-compatible".into(), "未分类".into()];
    let provider_panel = render_providers(ModelPurpose::Chat, &providers, 0, 1);
    let provider_model_panel = render_provider_models(
        ModelPurpose::Chat,
        &"very-long-provider-name-should-be-compacted".repeat(3),
        &models,
        Some(&models[0].id),
        0,
        4,
    );
    for row in provider_panel
        .keyboard
        .iter()
        .chain(provider_model_panel.keyboard.iter())
    {
        for button in row {
            assert!(
                parse_callback_data(&button.callback_data).is_ok(),
                "{}",
                button.callback_data
            );
            assert!(
                button.callback_data.len() < 4096,
                "{}",
                button.callback_data
            );
        }
    }
    assert!(provider_model_panel.text.contains("[当前]"));
    assert!(provider_model_panel
        .keyboard
        .iter()
        .flatten()
        .any(|button| button.callback_data.contains(&models[0].id)));
    assert!(provider_model_panel
        .keyboard
        .iter()
        .flatten()
        .any(|button| button
            .callback_data
            .contains("very-long-provider-name-should-be-compacted")));
}

#[test]
fn chat_and_compression_callback_purposes_are_independent() {
    let chat = CallbackAction::Providers {
        purpose: ModelPurpose::Chat,
        page: 0,
    };
    let comp = CallbackAction::Providers {
        purpose: ModelPurpose::Compression,
        page: 0,
    };
    assert_ne!(chat.to_callback_data(), comp.to_callback_data());
    assert!(matches!(
        parse_callback_data(&chat.to_callback_data()).unwrap(),
        CallbackAction::Providers {
            purpose: ModelPurpose::Chat,
            ..
        }
    ));
    assert!(matches!(
        parse_callback_data(&comp.to_callback_data()).unwrap(),
        CallbackAction::Providers {
            purpose: ModelPurpose::Compression,
            ..
        }
    ));
}
