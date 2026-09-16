use crate::domain::channel::TelegramBot;
use crate::error::AppResult;
use crate::modules::telegram::TelegramModule;
use crate::seams::secret_vault::SecretVault;

pub struct TeloxideRuntime;

impl TeloxideRuntime {
    pub async fn start(
        module: &TelegramModule,
        bot: &TelegramBot,
        vault: &dyn SecretVault,
    ) -> AppResult<()> {
        // Production polling is started through TelegramModule::start_bot after token decrypt.
        // The actual getUpdates loop is intentionally behind a durable inbox so offset is
        // advanced only after updates are persisted.
        module.start_bot(bot, vault).await
    }
}
