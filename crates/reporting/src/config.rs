use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ReportingConfig {
    pub database_url: String,
    pub listen_addr: String,
    pub db_max_connections: u32,
    pub log_level: String,
}

impl ReportingConfig {
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();
        let cfg = config::Config::builder()
            .add_source(config::Environment::with_prefix("REPORT").separator("__"))
            .set_default("listen_addr", "0.0.0.0:8082")?
            .set_default("db_max_connections", 5u32)?
            .set_default("log_level", "info")?
            .build()?;
        Ok(cfg.try_deserialize()?)
    }
}
