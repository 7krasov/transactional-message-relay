use log::kv::{Error, Key, ToValue, Value, Visitor};
use log::{error, Metadata, Record};
use serde_json::json;
use std::collections::HashMap;
use std::str::FromStr;

struct JsonLogger;

impl log::Log for JsonLogger {
    #[allow(unused_variables)]
    fn enabled(&self, metadata: &Metadata) -> bool {
        if metadata.target().starts_with("sqlx::query") {
            return false;
        }
        true
        // metadata.level() <= log::LevelFilter::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            // Custom visitor to collect key-value pairs
            struct KvCollector<'a> {
                map: &'a mut HashMapLogData,
            }

            impl<'kvs> Visitor<'kvs> for KvCollector<'_> {
                fn visit_pair(&mut self, key: Key<'kvs>, value: Value<'kvs>) -> Result<(), Error> {
                    if key.as_str() == "ctxt" {
                        let json_string = value.to_string();
                        let hm: HashMap<String, String> =
                            serde_json::from_str(&json_string).unwrap();
                        for (k, v) in hm {
                            self.map.insert(k, v);
                        }
                    } else {
                        self.map.insert(key.to_string(), value.to_string());
                    }
                    Ok(())
                }
            }

            #[allow(unused_mut)]
            let mut log_entry = crate::log_hashmap! {};
            let mut kv_collector = KvCollector {
                map: &mut log_entry,
            };
            let r = record.key_values().visit(&mut kv_collector);

            if let Err(e) = r {
                error!("logger: record.key_values() Error: {:?}", e);
                return;
            }

            log_entry.insert("message", record.args().to_string().as_str());
            log_entry.insert("level", record.level().to_string().as_str());

            // Add custom fields if any
            if let Some(module_path) = record.module_path() {
                log_entry.insert("module_path", module_path.to_string().as_str());
            }
            if let Some(file) = record.file() {
                log_entry.insert("file", file.to_string().as_str());
            }
            if let Some(line) = record.line() {
                log_entry.insert("line", line.to_string().as_str());
            }

            log_entry.insert("target", record.target().to_string());

            let json_log = json!(log_entry.orig_hash_map());
            println!("{}", json_log);
        }
    }

    fn flush(&self) {}
}

pub struct HashMapLogData(pub HashMap<String, String>);

impl HashMapLogData {
    pub fn insert<T: ToString, U: ToString>(&mut self, key: T, value: U) {
        self.0.insert(key.to_string(), value.to_string());
    }
    pub fn orig_hash_map(&self) -> HashMap<String, String> {
        self.0.clone()
    }
}

impl ToValue for HashMapLogData {
    fn to_value(&self) -> Value {
        Value::from_serde(&self.0)
    }
}

#[macro_export]
macro_rules! log_hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         // let mut map = HashMap::new();
         let mut map = $crate::logger::HashMapLogData(HashMap::new());
         $( map.insert($key, $val); )*
         // HashMapLogData(map)
         map
    }}
}

static LOGGER: JsonLogger = JsonLogger;

pub fn init_logger() {
    log::set_logger(&LOGGER).unwrap();
    //log::LOG_LEVEL_NAMES
    let log_level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "trace".to_string());
    log::set_max_level(log::LevelFilter::from_str(log_level.as_str()).unwrap());
}
