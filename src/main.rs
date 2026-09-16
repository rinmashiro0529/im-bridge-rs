use std::sync::Arc;

use clap::Parser;
use im_bridge::adapters::http::router::router;
use im_bridge::adapters::sqlite::{backup_database, connect_pool, migrate};
use im_bridge::bootstrap::AppState;
use im_bridge::config::{AppConfig, Cli, Commands};
use im_bridge::error::AppResult;
use im_bridge::modules::identity::IdentityModule;
use im_bridge::modules::migration::FilesystemLegacySource;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> AppResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    match cli.command {
        Commands::Serve { config } => serve(config.as_deref()).await,
        Commands::Doctor { config } => doctor(config.as_deref()).await,
        Commands::BootstrapAdmin {
            username,
            password_file,
            config,
        } => {
            let mut password = read_password(password_file.as_deref())?;
            let result = bootstrap_admin(config.as_deref(), &username, &password).await;
            zeroize::Zeroize::zeroize(&mut password);
            result
        }
        Commands::ImportSt {
            data_root,
            plugin_db,
            dry_run,
            config,
        } => import_st(config.as_deref(), &data_root, plugin_db.as_deref(), dry_run).await,
        Commands::ExportSt {
            workspace,
            output,
            config,
        } => export_st(config.as_deref(), &workspace, &output).await,
        Commands::Backup { output, config } => backup(config.as_deref(), &output).await,
        Commands::RotateMasterKey {
            dry_run,
            new_key,
            config,
        } => rotate_key(config.as_deref(), &new_key, dry_run).await,
    }
}

async fn load_state(config_path: Option<&std::path::Path>) -> AppResult<AppState> {
    let config = AppConfig::from_env_or_defaults(config_path)?;
    AppState::bootstrap(
        config,
        std::env::var("IMBRIDGE_FAKE_LLM").ok().as_deref() == Some("1"),
    )
    .await
}

async fn serve(config_path: Option<&std::path::Path>) -> AppResult<()> {
    let state = Arc::new(load_state(config_path).await?);
    if let Err(err) = state.telegram.autostart_enabled().await {
        tracing::error!(error = %err, "telegram autostart failed");
    }
    let listen = state.config.listen;
    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|err| im_bridge::AppError::internal(format!("bind failed: {err}")))?;
    tracing::info!(%listen, "im-bridge listening");
    let server_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|err| im_bridge::AppError::internal(format!("server error: {err}")));
    let shutdown_result = state.telegram.shutdown_all().await;
    server_result?;
    shutdown_result
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(error) => {
                    tracing::error!(%error, "failed to register SIGTERM handler");
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(%error, "failed to receive interrupt signal");
                }
            }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to receive interrupt signal");
        }
    }
}

async fn doctor(config_path: Option<&std::path::Path>) -> AppResult<()> {
    let state = load_state(config_path).await?;
    sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(&state.pool)
        .await?;
    println!("database=ok");
    println!("data_dir={}", state.config.data_dir.display());
    Ok(())
}

fn read_password(path: Option<&std::path::Path>) -> AppResult<String> {
    use std::io::{BufRead, Read};

    let mut password = String::new();
    match path {
        Some(path) if path == std::path::Path::new("-") => {
            std::io::stdin()
                .lock()
                .take(4097)
                .read_line(&mut password)
                .map_err(|_| im_bridge::AppError::internal("failed to read password from stdin"))?;
        }
        Some(path) => {
            let metadata = std::fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(im_bridge::AppError::bad_request(
                    "PASSWORD_FILE_INVALID",
                    "password file must be a regular file and must not be a symbolic link",
                ));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o077 != 0 {
                    return Err(im_bridge::AppError::bad_request(
                        "PASSWORD_FILE_PERMISSIONS",
                        "password file must not grant group or other access",
                    ));
                }
            }
            if metadata.len() > 4096 {
                return Err(im_bridge::AppError::bad_request(
                    "PASSWORD_FILE_TOO_LARGE",
                    "password file must not exceed 4096 bytes",
                ));
            }
            password = std::fs::read_to_string(path)?;
        }
        None => {
            return Err(im_bridge::AppError::bad_request(
                "PASSWORD_INPUT_REQUIRED",
                "use --password-file <path> or --password-file - to read one line from stdin",
            ));
        }
    }
    while password.ends_with(['\r', '\n']) {
        password.pop();
    }
    if !(12..=1024).contains(&password.len()) {
        zeroize::Zeroize::zeroize(&mut password);
        return Err(im_bridge::AppError::bad_request(
            "PASSWORD_INVALID",
            "administrator password must contain 12-1024 bytes",
        ));
    }
    Ok(password)
}

async fn bootstrap_admin(
    config_path: Option<&std::path::Path>,
    username: &str,
    password: &str,
) -> AppResult<()> {
    let config = AppConfig::from_env_or_defaults(config_path)?;
    config.ensure_dirs()?;
    let pool = connect_pool(&config.database_path).await?;
    migrate(&pool).await?;
    let identity = IdentityModule::new(pool);
    let account = identity
        .bootstrap_admin(username, password, username)
        .await?;
    println!("account_id={}", account.id);
    Ok(())
}

async fn import_st(
    config_path: Option<&std::path::Path>,
    data_root: &std::path::Path,
    plugin_db: Option<&std::path::Path>,
    dry_run: bool,
) -> AppResult<()> {
    let dry_run_dir = dry_run
        .then(|| std::env::temp_dir().join(format!("im-bridge-dry-run-{}", uuid::Uuid::new_v4())));
    let state = if let Some(temp_dir) = dry_run_dir.as_deref() {
        let mut config = AppConfig::from_env_or_defaults(config_path)?;
        config.data_dir = temp_dir.to_path_buf();
        config.database_path = temp_dir.join("app.db");
        config.master_key_path = temp_dir.join("master.key");
        AppState::bootstrap(config, true).await?
    } else {
        load_state(config_path).await?
    };
    let source = FilesystemLegacySource::new(data_root.to_path_buf());
    let mut report = state.importer.import_source(&source, dry_run).await?;
    if let Some(plugin_db) = plugin_db {
        let plugin_report = state
            .importer
            .import_plugin_db(plugin_db, &state.telegram, state.vault.as_ref(), dry_run)
            .await?;
        report.bots += plugin_report.bots;
        report.bindings += plugin_report.bindings;
        report.warnings.extend(plugin_report.warnings);
    }
    println!(
        "characters={} conversations={} messages={} bots={} bindings={} warnings={}",
        report.characters,
        report.conversations,
        report.messages,
        report.bots,
        report.bindings,
        report.warnings.len()
    );
    for warning in report.warnings {
        println!("warning={warning}");
    }
    drop(state);
    if let Some(temp_dir) = dry_run_dir {
        let _ = std::fs::remove_dir_all(temp_dir);
    }
    Ok(())
}

async fn export_st(
    config_path: Option<&std::path::Path>,
    workspace: &str,
    output: &std::path::Path,
) -> AppResult<()> {
    let state = load_state(config_path).await?;
    let count = state.importer.export_workspace(workspace, output).await?;
    println!("exported={count}");
    Ok(())
}

async fn backup(config_path: Option<&std::path::Path>, output: &std::path::Path) -> AppResult<()> {
    let state = load_state(config_path).await?;
    backup_database(&state.pool, output).await?;
    println!("backup={}", output.display());
    Ok(())
}

async fn rotate_key(
    config_path: Option<&std::path::Path>,
    new_key: &std::path::Path,
    dry_run: bool,
) -> AppResult<()> {
    if !dry_run {
        return Err(im_bridge::AppError::service_unavailable(
            "MASTER_KEY_ROTATION_DISABLED",
            "online master-key rotation is disabled until the recoverable rotation protocol is implemented",
        ));
    }
    let state = load_state(config_path).await?;
    let mut bytes = std::fs::read(new_key)?;
    let result = state.vault.rotate_master_key(&bytes, true).await;
    zeroize::Zeroize::zeroize(&mut bytes);
    let count = result?;
    println!("validated={count} dry_run=true");
    Ok(())
}
