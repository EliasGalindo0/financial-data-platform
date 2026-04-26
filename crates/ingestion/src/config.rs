use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub database_url: String,
    pub redis_url: String,
    pub listen_addr: String,
    pub log_level: String,
    pub db_max_connections: u32,
    pub rate_limit_per_minute: u64,
    pub otlp_endpoint: Option<String>,
    pub environment: String,
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok(); // optional — doesn't fail if .env missing

        let cfg = config::Config::builder()
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(
                config::File::with_name(&format!(
                    "config/{}",
                    std::env::var("APP_ENV").unwrap_or_else(|_| "development".into())
                ))
                .required(false),
            )
            .add_source(config::Environment::with_prefix("APP").separator("__"))
            .set_default("listen_addr", "0.0.0.0:8080")?
            .set_default("log_level", "info")?
            .set_default("db_max_connections", 20u32)?
            .set_default("rate_limit_per_minute", 1000u64)?
            .set_default("environment", "development")?
            .build()?;

        Ok(cfg.try_deserialize()?)
    }
}
