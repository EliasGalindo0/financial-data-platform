use std::{
    collections::HashMap,
    sync::{OnceLock, Mutex},
};

use uuid::Uuid;

/// Deterministic fault injection for local/CI chaos testing.
///
/// Enable by setting `FDP_FAULTS`:
/// - `FDP_FAULTS=dlq.write=always`
/// - `FDP_FAULTS=processor.transient=every:3,fraud.hang=once`
/// - `FDP_FAULTS=dlq.write=rate:0.25` (stable per correlation_id when provided)
///
/// Supported modes:
/// - `always`
/// - `never`
/// - `once` (first call triggers, then disables)
/// - `every:N` (1-indexed; triggers on Nth, 2N-th, ...)
/// - `rate:P` (0.0–1.0; uses correlation_id hashing when provided, otherwise a counter)
pub fn should_fail(point: &str, correlation_id: Option<Uuid>) -> bool {
    let cfg = config();
    let Some(rule) = cfg.rules.get(point) else {
        return false;
    };

    match rule {
        Rule::Always => true,
        Rule::Never => false,
        Rule::Once => {
            let n = next_call_count(point);
            n == 1
        }
        Rule::Every(n) => {
            let call = next_call_count(point);
            call % (*n as u64) == 0
        }
        Rule::Rate(p) => {
            let p = p.clamp(0.0, 1.0);
            if p <= 0.0 {
                return false;
            }
            if p >= 1.0 {
                return true;
            }

            let u = match correlation_id {
                Some(id) => stable_u64_from_uuid(id),
                None => next_call_count(point),
            };

            // Map to [0, 1). Use upper 53 bits for f64 mantissa friendliness.
            let x = (u >> 11) as f64 / ((1u64 << 53) as f64);
            x < (p as f64)
        }
    }
}

fn stable_u64_from_uuid(id: Uuid) -> u64 {
    let b = id.as_bytes();
    let mut x = 0u64;
    for (i, byte) in b.iter().enumerate() {
        x ^= (*byte as u64) << ((i % 8) * 8);
        x = x.rotate_left(7) ^ 0x9E3779B97F4A7C15u64;
    }
    x
}

#[derive(Debug, Clone)]
enum Rule {
    Always,
    Never,
    Once,
    Every(u32),
    Rate(f32),
}

#[derive(Debug, Default)]
struct FaultConfig {
    rules: HashMap<String, Rule>,
}

static CONFIG: OnceLock<FaultConfig> = OnceLock::new();
static COUNTERS: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

fn config() -> &'static FaultConfig {
    CONFIG.get_or_init(|| {
        let mut cfg = FaultConfig::default();
        let raw = std::env::var("FDP_FAULTS").unwrap_or_default();
        for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let Some((k, v)) = part.split_once('=') else { continue };
            let key = k.trim().to_string();
            let val = v.trim();

            let rule = match val {
                "always" => Some(Rule::Always),
                "never" => Some(Rule::Never),
                "once" => Some(Rule::Once),
                _ if val.starts_with("every:") => val["every:".len()..]
                    .parse::<u32>()
                    .ok()
                    .filter(|n| *n > 0)
                    .map(Rule::Every),
                _ if val.starts_with("rate:") => val["rate:".len()..]
                    .parse::<f32>()
                    .ok()
                    .map(Rule::Rate),
                _ => None,
            };

            if let Some(rule) = rule {
                cfg.rules.insert(key, rule);
            }
        }

        cfg
    })
}

fn next_call_count(point: &str) -> u64 {
    let map = COUNTERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().expect("fault injector mutex poisoned");
    let n = guard.entry(point.to_string()).or_insert(0);
    *n += 1;
    *n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_uuid_hash_is_stable() {
        let id = Uuid::nil();
        let a = stable_u64_from_uuid(id);
        let b = stable_u64_from_uuid(id);
        assert_eq!(a, b);
    }
}

